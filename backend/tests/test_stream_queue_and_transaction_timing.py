"""Test for Redis Stream Ingestion Queue Wait Time & PostgreSQL Worker Transaction Duration.

Measures:
1. Queue Wait Time: difference between message enqueue timestamp and the time the worker starts processing.
2. Transaction Duration: time the worker takes to execute the database operations and commit the transaction via PgBouncer.
3. Total Latency: complete roundtrip from client dispatch to result receipt.

Zero modifications to production logic.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from typing import Any
import uuid

# Ensure backend and src are in sys.path
_BACKEND_DIR = Path(__file__).resolve().parent.parent
_SRC_DIR = _BACKEND_DIR / "src"
for _path in [str(_BACKEND_DIR), str(_SRC_DIR)]:
    if _path not in sys.path:
        sys.path.insert(0, _path)

import pytest
from sqlalchemy import select

from app.core.config import settings
from app.core.database import async_session_factory
from app.core.redis import close_redis_pool, get_redis_client
from app.models import FlightSeat, FlightSeatStatus
from app.schemas.write_event import (
    WriteAction,
    WriteEvent,
    WriteResult,
    WriteResultStatus,
)
from app.services.stream_worker import StreamWorker
from app.services.write_stream import WriteStreamService, write_stream_service


@dataclass
class TimingRecord:
    event_id: str
    action: str
    entity: str
    redis_msg_id: str
    # Wall clock timestamps (ISO UTC strings)
    enqueue_timestamp_iso: str
    worker_start_timestamp_iso: str
    tx_complete_timestamp_iso: str
    client_done_timestamp_iso: str
    # High-precision monotonic durations (milliseconds)
    queue_wait_ms: float
    tx_duration_ms: float
    pubsub_notification_ms: float
    total_latency_ms: float
    success: bool
    status_code: int


class TimingWorker(StreamWorker):
    """Subclass of StreamWorker for timing measurement only.

    Maintains 100% of the original execution logic while capturing high-precision
    timestamps for message pickup, transaction start, and transaction commit.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.recorded_timings: dict[str, dict[str, Any]] = {}

    def on_batch_received(
        self,
        batch: list[tuple[str, str, str | None]],
        t_received_perf: float,
        t_received_wall: datetime,
    ) -> None:
        for msg_id, event_id, data_str in batch:
            if event_id not in self.recorded_timings:
                self.recorded_timings[event_id] = {
                    "redis_msg_id": msg_id,
                    "worker_received_perf": t_received_perf,
                }

    def on_tx_start(
        self,
        batch: list[tuple[str, str, str | None]],
        t_tx_start_perf: float,
        t_tx_start_wall: datetime,
    ) -> None:
        for msg_id, event_id, data_str in batch:
            if event_id in self.recorded_timings:
                self.recorded_timings[event_id]["worker_start_perf"] = (
                    t_tx_start_perf
                )
                self.recorded_timings[event_id]["worker_start_wall"] = (
                    t_tx_start_wall
                )

    def on_item_processed(
        self,
        event_id: str,
        result: WriteResult,
        t_item_start: float,
        t_item_end: float,
        msg_id: str,
    ) -> None:
        if event_id in self.recorded_timings:
            self.recorded_timings[event_id].update(
                {
                    "item_start_perf": t_item_start,
                    "item_end_perf": t_item_end,
                    "status_code": result.status_code,
                    "success": result.status == WriteResultStatus.SUCCESS,
                }
            )

    def on_tx_complete(
        self,
        batch: list[tuple[str, str, str | None]],
        t_tx_complete_perf: float,
        t_tx_complete_wall: datetime,
    ) -> None:
        for msg_id, event_id, data_str in batch:
            if event_id in self.recorded_timings:
                self.recorded_timings[event_id]["tx_complete_perf"] = (
                    t_tx_complete_perf
                )
                self.recorded_timings[event_id]["tx_complete_wall"] = (
                    t_tx_complete_wall
                )

    async def process_one_batch(
        self, count: int | None = None, block: int | None = None
    ) -> int:
        return await super().process_one_batch(count=count, block=block)


def print_timing_report(
    title: str, records: list[TimingRecord]
) -> None:
    """Print formatted ASCII table showing queue wait time and transaction duration."""
    print("\n" + "=" * 115)
    print(f"TIMING BREAKDOWN: {title}")
    print("=" * 115)

    header = (
        f"{'Action':<22} | {'Enqueue Time':<15} | {'Worker Start':<15} | "
        f"{'Queue Wait':<12} | {'Tx Complete':<15} | {'Tx Duration':<13} | {'Total Latency':<13}"
    )
    print(header)
    print("-" * len(header))

    for r in records:
        enq_str = r.enqueue_timestamp_iso.split("T")[1][:12]
        start_str = r.worker_start_timestamp_iso.split("T")[1][:12]
        tx_str = r.tx_complete_timestamp_iso.split("T")[1][:12]
        print(
            f"{r.action:<22} | {enq_str:<15} | {start_str:<15} | "
            f"{r.queue_wait_ms:>8.2f} ms  | {tx_str:<15} | {r.tx_duration_ms:>9.2f} ms   | {r.total_latency_ms:>9.2f} ms"
        )

    print("-" * len(header))
    avg_queue = sum(r.queue_wait_ms for r in records) / len(records)
    avg_tx = sum(r.tx_duration_ms for r in records) / len(records)
    avg_total = sum(r.total_latency_ms for r in records) / len(records)

    print(
        f"{'AVERAGE':<22} | {'':<15} | {'':<15} | "
        f"{avg_queue:>8.2f} ms  | {'':<15} | {avg_tx:>9.2f} ms   | {avg_total:>9.2f} ms"
    )
    print(
        f"{'MIN / MAX Queue Wait:':<35} {min(r.queue_wait_ms for r in records):.2f} ms / {max(r.queue_wait_ms for r in records):.2f} ms"
    )
    print(
        f"{'MIN / MAX Tx Duration:':<35} {min(r.tx_duration_ms for r in records):.2f} ms / {max(r.tx_duration_ms for r in records):.2f} ms"
    )
    print("=" * 115 + "\n")


@pytest.fixture(autouse=True)
async def cleanup():
    yield
    await close_redis_pool()


@pytest.mark.asyncio
async def test_single_message_queue_wait_and_transaction_duration():
    """Measures queue wait time and transaction duration for sequential individual operations."""
    test_stream = f"test:stream:timing:{uuid.uuid4().hex[:6]}"
    test_group = f"test_group_timing:{uuid.uuid4().hex[:6]}"
    worker = TimingWorker(
        stream_name=test_stream,
        group_name=test_group,
        consumer_name=f"timing-test-worker-{uuid.uuid4().hex[:6]}",
    )
    await worker.init_consumer_group()
    worker_task = asyncio.create_task(worker.run_loop())
    test_write_service = WriteStreamService(stream_name=test_stream)

    # Allow worker to enter continuous xreadgroup blocking loop
    await asyncio.sleep(0.05)

    records: list[TimingRecord] = []

    # 1. Available seat query for realistic booking test
    async with async_session_factory() as session:
        seat_res = await session.execute(
            select(FlightSeat.flight_seat_id, FlightSeat.flight_id)
            .where(FlightSeat.status == FlightSeatStatus.AVAILABLE)
            .limit(1)
        )
        avail_seat = seat_res.first()
        seat_id = avail_seat.flight_seat_id if avail_seat else 1

    operations_to_test = [
        (
            WriteAction.UPDATE_FLIGHT_STATUS,
            "flight",
            {"flight_id": 1, "status": "SCHEDULED"},
        ),
        (
            WriteAction.CREATE_CUSTOMER,
            "customer",
            {
                "first_name": "Timing",
                "last_name": "Benchmark",
                "email": f"timing_{uuid.uuid4().hex[:8]}@test.com",
                "password": "password_hash_test_123",
                "date_of_birth": "1992-05-20",
            },
        ),
        (
            WriteAction.CREATE_BOOKING,
            "booking",
            {
                "customer_id": 1,
                "tickets": [
                    {
                        "flight_seat_id": seat_id,
                        "passenger_name": "Latency Test Passenger",
                        "price": "210.00",
                    }
                ],
            },
        ),
    ]

    created_booking_id: int | None = None

    try:
        for action, entity, payload in operations_to_test:
            event_id = str(uuid.uuid4())
            # Capture exact enqueue time on client side
            t_enqueue_wall = datetime.now(timezone.utc)
            t_enqueue_perf = time.perf_counter()

            res = await test_write_service.dispatch_and_wait(
                action=action,
                entity=entity,
                payload=payload,
                timeout=8.0,
                event_id=event_id,
            )

            # Capture exact client completion time
            t_client_done_perf = time.perf_counter()
            t_client_done_wall = datetime.now(timezone.utc)

            if action == WriteAction.CREATE_BOOKING and res.get("booking_id"):
                created_booking_id = res["booking_id"]

            # Match timing recorded by worker
            w_timing = worker.recorded_timings.get(event_id)
            if w_timing and "tx_complete_perf" in w_timing:
                queue_wait_ms = max(
                    0.0,
                    (w_timing["worker_start_perf"] - t_enqueue_perf) * 1000.0,
                )
                tx_duration_ms = max(
                    0.0,
                    (w_timing["tx_complete_perf"] - w_timing["worker_start_perf"])
                    * 1000.0,
                )
                pubsub_ms = max(
                    0.0,
                    (t_client_done_perf - w_timing["tx_complete_perf"]) * 1000.0,
                )
                total_latency_ms = max(
                    0.0,
                    (t_client_done_perf - t_enqueue_perf) * 1000.0,
                )

                records.append(
                    TimingRecord(
                        event_id=event_id,
                        action=action.value,
                        entity=entity,
                        redis_msg_id=w_timing["redis_msg_id"],
                        enqueue_timestamp_iso=t_enqueue_wall.isoformat(),
                        worker_start_timestamp_iso=w_timing[
                            "worker_start_wall"
                        ].isoformat(),
                        tx_complete_timestamp_iso=w_timing[
                            "tx_complete_wall"
                        ].isoformat(),
                        client_done_timestamp_iso=t_client_done_wall.isoformat(),
                        queue_wait_ms=round(queue_wait_ms, 2),
                        tx_duration_ms=round(tx_duration_ms, 2),
                        pubsub_notification_ms=round(pubsub_ms, 2),
                        total_latency_ms=round(total_latency_ms, 2),
                        success=w_timing["success"],
                        status_code=w_timing["status_code"],
                    )
                )

        # Cancel the created booking to test CANCEL_BOOKING and restore state
        if created_booking_id:
            cancel_event_id = str(uuid.uuid4())
            t_enqueue_wall = datetime.now(timezone.utc)
            t_enqueue_perf = time.perf_counter()

            res = await test_write_service.dispatch_and_wait(
                action=WriteAction.CANCEL_BOOKING,
                entity="booking",
                payload={"booking_id": created_booking_id},
                timeout=8.0,
                event_id=cancel_event_id,
            )

            t_client_done_perf = time.perf_counter()
            t_client_done_wall = datetime.now(timezone.utc)

            w_timing = worker.recorded_timings.get(cancel_event_id)
            if w_timing and "tx_complete_perf" in w_timing:
                queue_wait_ms = max(
                    0.0,
                    (w_timing["worker_start_perf"] - t_enqueue_perf) * 1000.0,
                )
                tx_duration_ms = max(
                    0.0,
                    (w_timing["tx_complete_perf"] - w_timing["worker_start_perf"])
                    * 1000.0,
                )
                pubsub_ms = max(
                    0.0,
                    (t_client_done_perf - w_timing["tx_complete_perf"]) * 1000.0,
                )
                total_latency_ms = max(
                    0.0,
                    (t_client_done_perf - t_enqueue_perf) * 1000.0,
                )

                records.append(
                    TimingRecord(
                        event_id=cancel_event_id,
                        action=WriteAction.CANCEL_BOOKING.value,
                        entity="booking",
                        redis_msg_id=w_timing["redis_msg_id"],
                        enqueue_timestamp_iso=t_enqueue_wall.isoformat(),
                        worker_start_timestamp_iso=w_timing[
                            "worker_start_wall"
                        ].isoformat(),
                        tx_complete_timestamp_iso=w_timing[
                            "tx_complete_wall"
                        ].isoformat(),
                        client_done_timestamp_iso=t_client_done_wall.isoformat(),
                        queue_wait_ms=round(queue_wait_ms, 2),
                        tx_duration_ms=round(tx_duration_ms, 2),
                        pubsub_notification_ms=round(pubsub_ms, 2),
                        total_latency_ms=round(total_latency_ms, 2),
                        success=w_timing["success"],
                        status_code=w_timing["status_code"],
                    )
                )

    finally:
        worker.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        redis = get_redis_client()
        await redis.delete(test_stream)

    # Print formatted output
    print_timing_report("Sequential Single-Message Ingestion & Processing", records)

    # Validations & assertions
    assert len(records) >= 3
    for r in records:
        assert r.success is True
        assert r.queue_wait_ms > 0, "Queue wait time must be > 0 ms"
        assert r.tx_duration_ms > 0, "Transaction duration must be > 0 ms"
        assert r.total_latency_ms >= r.tx_duration_ms


@pytest.mark.asyncio
async def test_burst_messages_queue_wait_and_batched_transaction_duration():
    """Measures queue wait time and batched transaction execution time when multiple

    messages arrive concurrently in the Redis Stream.
    """
    test_stream = f"test:stream:burst:{uuid.uuid4().hex[:6]}"
    test_group = f"test_group_burst:{uuid.uuid4().hex[:6]}"
    worker = TimingWorker(
        stream_name=test_stream,
        group_name=test_group,
        consumer_name=f"burst-timing-worker-{uuid.uuid4().hex[:6]}",
    )
    await worker.init_consumer_group()
    worker_task = asyncio.create_task(worker.run_loop())
    await asyncio.sleep(0.05)
    test_write_service = WriteStreamService(stream_name=test_stream)

    num_messages = 5
    records: list[TimingRecord] = []

    async def send_burst_event(idx: int):
        event_id = str(uuid.uuid4())
        status_val = "SCHEDULED" if idx % 2 == 0 else "DELAYED"
        payload = {"flight_id": (idx % 100) + 1, "status": status_val}
        t_enq_wall = datetime.now(timezone.utc)
        t_enq_perf = time.perf_counter()

        res = await test_write_service.dispatch_and_wait(
            action=WriteAction.UPDATE_FLIGHT_STATUS,
            entity="flight",
            payload=payload,
            timeout=8.0,
            event_id=event_id,
        )
        t_done_perf = time.perf_counter()
        t_done_wall = datetime.now(timezone.utc)
        return idx, event_id, t_enq_wall, t_enq_perf, t_done_wall, t_done_perf

    try:
        tasks = [send_burst_event(i) for i in range(num_messages)]
        burst_results = await asyncio.gather(*tasks)

        for idx, event_id, t_enq_wall, t_enq_perf, t_done_wall, t_done_perf in burst_results:
            w_timing = worker.recorded_timings.get(event_id)
            if w_timing and "tx_complete_perf" in w_timing:
                q_wait = max(
                    0.0,
                    (w_timing["worker_start_perf"] - t_enq_perf) * 1000.0,
                )
                tx_dur = max(
                    0.0,
                    (w_timing["tx_complete_perf"] - w_timing["worker_start_perf"])
                    * 1000.0,
                )
                pub_ms = max(
                    0.0,
                    (t_done_perf - w_timing["tx_complete_perf"]) * 1000.0,
                )
                tot_ms = max(
                    0.0,
                    (t_done_perf - t_enq_perf) * 1000.0,
                )

                records.append(
                    TimingRecord(
                        event_id=event_id,
                        action=f"BURST_FLIGHT_STATUS_{idx}",
                        entity="flight",
                        redis_msg_id=w_timing["redis_msg_id"],
                        enqueue_timestamp_iso=t_enq_wall.isoformat(),
                        worker_start_timestamp_iso=w_timing[
                            "worker_start_wall"
                        ].isoformat(),
                        tx_complete_timestamp_iso=w_timing[
                            "tx_complete_wall"
                        ].isoformat(),
                        client_done_timestamp_iso=t_done_wall.isoformat(),
                        queue_wait_ms=round(q_wait, 2),
                        tx_duration_ms=round(tx_dur, 2),
                        pubsub_notification_ms=round(pub_ms, 2),
                        total_latency_ms=round(tot_ms, 2),
                        success=w_timing["success"],
                        status_code=w_timing["status_code"],
                    )
                )

    finally:
        worker.stop()
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        redis = get_redis_client()
        await redis.delete(test_stream)

    print_timing_report(
        f"Concurrent Burst Ingestion ({num_messages} Concurrent Events)", records
    )

    assert len(records) == num_messages
    for r in records:
        assert r.success is True
        assert r.tx_duration_ms > 0


if __name__ == "__main__":
    # Allows running directly via `python tests/test_stream_queue_and_transaction_timing.py`
    async def main():
        print("\n>>> Running Test 1: Single-Message Ingestion & Processing...")
        await test_single_message_queue_wait_and_transaction_duration()

        print("\n>>> Running Test 2: Burst Concurrent Ingestion & Processing...")
        await test_burst_messages_queue_wait_and_batched_transaction_duration()

        await close_redis_pool()

    asyncio.run(main())
