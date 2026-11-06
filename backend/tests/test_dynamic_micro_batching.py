"""Unit and Integration Tests for Worker Dynamic Micro-Batching Logic.

Tests:
1. Environment Variable Fallback & Alias Resolution:
   - Validates that default values are applied when .env is unconfigured.
   - Validates that custom .env variables and aliases are correctly parsed.
   - Validates that StreamWorker parameters fall back to settings or constructor overrides.
2. Low Queue Depth (Immediate Flush):
   - When queue depth is low (<= queue_depth_threshold), messages are flushed immediately
     with 0ms accumulation delay and minimal batch size.
3. High Queue Depth (Batching Active):
   - When queue depth rises (> queue_depth_threshold), worker ingests in batches
     up to max_batch_size within batch_timeout_ms and commits in a single transaction.
4. Dynamic Mode Transition:
   - Seamless transition between low-queue immediate flush and high-queue micro-batching.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
import uuid

# Ensure backend/src is on sys.path
_BACKEND_DIR = Path(__file__).resolve().parent.parent
_SRC_DIR = _BACKEND_DIR / "src"
for _path in [str(_BACKEND_DIR), str(_SRC_DIR)]:
    if _path not in sys.path:
        sys.path.insert(0, _path)

import pytest
from sqlalchemy import select

from app.core.config import Settings, settings
from app.core.database import async_session_factory
from app.core.redis import close_redis_pool, get_redis_client
from app.models import Customer
from app.schemas.write_event import WriteAction, WriteEvent, WriteResult
from app.services.stream_worker import StreamWorker
from app.services.write_stream import write_stream_service


@pytest.fixture(autouse=True)
async def cleanup_redis():
    yield
    await close_redis_pool()


def test_dynamic_micro_batching_settings_defaults():
    """Verify that default fallback values exist and apply when .env is empty or unconfigured."""
    # Instantiating Settings with empty dictionary simulates missing .env configuration
    empty_settings = Settings(_env_file=None)
    assert empty_settings.STREAM_WORKER_MIN_BATCH_SIZE == 1
    assert empty_settings.STREAM_WORKER_MAX_BATCH_SIZE == 50
    assert empty_settings.STREAM_WORKER_QUEUE_DEPTH_THRESHOLD == 5
    assert empty_settings.STREAM_WORKER_BATCH_TIMEOUT_MS == 10
    assert empty_settings.STREAM_WORKER_BLOCK_MS == 1000
    assert empty_settings.STREAM_WORKER_FLUSH_STRATEGY == "dynamic"


def test_dynamic_micro_batching_settings_aliases():
    """Verify that common aliases from .env are resolved with high precedence."""
    aliased = Settings(
        _env_file=None,
        BATCH_SIZE=80,
        QUEUE_DEPTH_THRESHOLD=15,
        FLUSH_TIMEOUT_MS=25,
        BLOCK_MS=500,
    )
    assert aliased.STREAM_WORKER_MAX_BATCH_SIZE == 80
    assert aliased.STREAM_WORKER_QUEUE_DEPTH_THRESHOLD == 15
    assert aliased.STREAM_WORKER_BATCH_TIMEOUT_MS == 25
    assert aliased.STREAM_WORKER_BLOCK_MS == 500


def test_stream_worker_initialization_defaults_and_overrides():
    """Verify that StreamWorker falls back to global settings or constructor overrides."""
    # Default initialization
    worker = StreamWorker(consumer_name="test-worker-defaults")
    assert worker.min_batch_size == settings.STREAM_WORKER_MIN_BATCH_SIZE
    assert worker.max_batch_size == settings.STREAM_WORKER_MAX_BATCH_SIZE
    assert worker.queue_depth_threshold == settings.STREAM_WORKER_QUEUE_DEPTH_THRESHOLD
    assert worker.batch_timeout_ms == settings.STREAM_WORKER_BATCH_TIMEOUT_MS
    assert worker.block_ms == settings.STREAM_WORKER_BLOCK_MS

    # Custom override initialization
    custom_worker = StreamWorker(
        consumer_name="test-worker-custom",
        min_batch_size=2,
        max_batch_size=25,
        queue_depth_threshold=8,
        batch_timeout_ms=15,
        block_ms=300,
        flush_strategy="size",
    )
    assert custom_worker.min_batch_size == 2
    assert custom_worker.max_batch_size == 25
    assert custom_worker.queue_depth_threshold == 8
    assert custom_worker.batch_timeout_ms == 15
    assert custom_worker.block_ms == 300
    assert custom_worker.flush_strategy == "size"


@pytest.mark.asyncio
async def test_low_queue_depth_flushes_immediately():
    """When queue depth is low (<= queue_depth_threshold), messages are flushed immediately

    without waiting for linger timeout.
    """
    stream_name = f"test:stream:low_queue:{uuid.uuid4().hex[:6]}"
    group_name = f"group_{uuid.uuid4().hex[:6]}"

    worker = StreamWorker(
        stream_name=stream_name,
        group_name=group_name,
        consumer_name="worker-low-queue",
        min_batch_size=1,
        max_batch_size=50,
        queue_depth_threshold=5,
        batch_timeout_ms=200,  # Deliberately high linger to prove it is NOT waited on
        block_ms=500,
    )
    await worker.init_consumer_group()

    # Dispatch 1 message
    test_email = f"low_queue_{uuid.uuid4().hex[:6]}@example.com"
    event = WriteEvent(
        event_id=str(uuid.uuid4()),
        action=WriteAction.CREATE_CUSTOMER,
        entity="customer",
        payload={
            "first_name": "LowQueue",
            "last_name": "ImmediateFlush",
            "email": test_email,
            "password": "securepassword123",
            "date_of_birth": "1995-01-01",
        },
        created_at=datetime.now(timezone.utc).isoformat(),
    )

    redis = get_redis_client()
    await redis.xadd(
        stream_name,
        {"event_id": event.event_id, "data": event.model_dump_json()},
    )

    # Queue depth is 1 <= 5, so worker MUST flush immediately without waiting 200ms
    t_start = time.perf_counter()
    processed = await worker.process_one_batch()
    elapsed_ms = (time.perf_counter() - t_start) * 1000.0

    assert processed == 1
    # Elapsed time must be significantly less than the 200ms batch_timeout_ms
    assert elapsed_ms < 150.0, f"Expected immediate flush (<150ms), got {elapsed_ms:.2f}ms"

    # Verify customer was persisted in PostgreSQL via PgBouncer
    async with async_session_factory() as session:
        res = await session.execute(
            select(Customer).where(Customer.email == test_email)
        )
        saved = res.scalar_one_or_none()
        assert saved is not None
        assert saved.first_name == "LowQueue"

    # Cleanup test stream
    await redis.delete(stream_name)


@pytest.mark.asyncio
async def test_high_queue_depth_batches_messages_into_single_transaction():
    """When queue depth rises (> queue_depth_threshold), messages are batched together

    and committed in a single PostgreSQL transaction.
    """
    stream_name = f"test:stream:high_queue:{uuid.uuid4().hex[:6]}"
    group_name = f"group_{uuid.uuid4().hex[:6]}"

    tx_start_count = 0
    tx_complete_count = 0

    class TrackingBatchWorker(StreamWorker):
        def on_tx_start(self, batch, t_perf, t_wall):
            nonlocal tx_start_count
            tx_start_count += 1

        def on_tx_complete(self, batch, t_perf, t_wall):
            nonlocal tx_complete_count
            tx_complete_count += 1

    worker = TrackingBatchWorker(
        stream_name=stream_name,
        group_name=group_name,
        consumer_name="worker-high-queue",
        min_batch_size=1,
        max_batch_size=10,
        queue_depth_threshold=3,  # Queue depth > 3 triggers batching
        batch_timeout_ms=50,
        block_ms=500,
    )
    await worker.init_consumer_group()

    redis = get_redis_client()
    num_events = 7
    created_emails: list[str] = []

    # Enqueue 7 events simultaneously (queue depth will be 7 > threshold 3)
    for i in range(num_events):
        email = f"high_queue_{i}_{uuid.uuid4().hex[:6]}@example.com"
        created_emails.append(email)
        event = WriteEvent(
            event_id=str(uuid.uuid4()),
            action=WriteAction.CREATE_CUSTOMER,
            entity="customer",
            payload={
                "first_name": f"BatchUser{i}",
                "last_name": "HighQueue",
                "email": email,
                "password": "securepassword123",
                "date_of_birth": "1994-06-15",
            },
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        await redis.xadd(
            stream_name,
            {"event_id": event.event_id, "data": event.model_dump_json()},
        )

    # Verify queue depth is > threshold
    depth = await worker.get_queue_depth()
    assert depth == num_events
    assert depth > worker.queue_depth_threshold

    # Process batch - worker should batch all 7 into a SINGLE transaction commit
    processed = await worker.process_one_batch()
    assert processed == num_events

    # Verify all 7 events executed in exactly 1 transaction
    assert tx_start_count == 1
    assert tx_complete_count == 1

    # Verify all 7 customers exist in database
    async with async_session_factory() as session:
        res = await session.execute(
            select(Customer).where(Customer.email.in_(created_emails))
        )
        saved_customers = res.scalars().all()
        assert len(saved_customers) == num_events

    # Cleanup
    await redis.delete(stream_name)


@pytest.mark.asyncio
async def test_dynamic_transition_low_to_high_and_back_to_low():
    """Verify that worker seamlessly transitions between immediate flush and batching

    as queue depth dynamically fluctuates.
    """
    stream_name = f"test:stream:fluctuate:{uuid.uuid4().hex[:6]}"
    group_name = f"group_{uuid.uuid4().hex[:6]}"

    batch_sizes_processed: list[int] = []

    class FluctuatingWorker(StreamWorker):
        async def execute_and_commit_batch(self, batch):
            batch_sizes_processed.append(len(batch))
            return await super().execute_and_commit_batch(batch)

    worker = FluctuatingWorker(
        stream_name=stream_name,
        group_name=group_name,
        consumer_name="fluctuate-worker",
        min_batch_size=1,
        max_batch_size=20,
        queue_depth_threshold=3,
        batch_timeout_ms=30,
        block_ms=200,
    )
    await worker.init_consumer_group()
    redis = get_redis_client()

    # Step 1: Queue depth is low (1 message) -> Should process as size 1
    ev1 = WriteEvent(
        event_id=str(uuid.uuid4()),
        action=WriteAction.CREATE_CUSTOMER,
        entity="customer",
        payload={
            "first_name": "Step1",
            "last_name": "Low",
            "email": f"step1_{uuid.uuid4().hex[:6]}@example.com",
            "password": "pwd",
            "date_of_birth": "1990-01-01",
        },
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    await redis.xadd(stream_name, {"event_id": ev1.event_id, "data": ev1.model_dump_json()})
    p1 = await worker.process_one_batch()
    assert p1 == 1
    assert batch_sizes_processed[-1] == 1

    # Step 2: Traffic surge! Enqueue 6 messages (> threshold 3) -> Should batch all 6
    for i in range(6):
        ev = WriteEvent(
            event_id=str(uuid.uuid4()),
            action=WriteAction.CREATE_CUSTOMER,
            entity="customer",
            payload={
                "first_name": f"Surge{i}",
                "last_name": "High",
                "email": f"surge_{i}_{uuid.uuid4().hex[:6]}@example.com",
                "password": "pwd",
                "date_of_birth": "1990-01-01",
            },
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        await redis.xadd(stream_name, {"event_id": ev.event_id, "data": ev.model_dump_json()})

    p2 = await worker.process_one_batch()
    assert p2 == 6
    assert batch_sizes_processed[-1] == 6

    # Step 3: Back to low traffic (1 message) -> Should process as size 1 immediately
    ev3 = WriteEvent(
        event_id=str(uuid.uuid4()),
        action=WriteAction.CREATE_CUSTOMER,
        entity="customer",
        payload={
            "first_name": "Step3",
            "last_name": "LowAgain",
            "email": f"step3_{uuid.uuid4().hex[:6]}@example.com",
            "password": "pwd",
            "date_of_birth": "1990-01-01",
        },
        created_at=datetime.now(timezone.utc).isoformat(),
    )
    await redis.xadd(stream_name, {"event_id": ev3.event_id, "data": ev3.model_dump_json()})
    p3 = await worker.process_one_batch()
    assert p3 == 1
    assert batch_sizes_processed[-1] == 1

    assert batch_sizes_processed == [1, 6, 1]

    # Cleanup
    await redis.delete(stream_name)
