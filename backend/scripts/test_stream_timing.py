#!/usr/bin/env python3
"""Redis Stream Ingestion Queue Wait Time & Transaction Duration Benchmark.

Measures the exact waiting times across all write APIs:
1. Queue Wait Time: difference between message enqueue timestamp and the time the worker starts processing.
2. Transaction Duration: time the worker takes to complete the PostgreSQL transaction via PgBouncer.
3. Total Latency: complete roundtrip from client dispatch to PubSub receipt.

Supports uniform operations across all write APIs, customizable concurrency, and stream workers.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import math
import os
from pathlib import Path
import random
import statistics
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

from sqlalchemy import select

from app.core.config import settings
from app.core.database import async_session_factory
from app.core.redis import (
    close_redis_pool,
    get_redis_client,
    tune_redis_connection_pool,
)
from app.models import Booking, FlightSeat, FlightSeatStatus
from app.schemas.write_event import (
    WriteAction,
    WriteEvent,
    WriteResult,
    WriteResultStatus,
)
from app.services.stream_worker import StreamWorker
from app.services.write_stream import WriteStreamService, write_stream_service

AIRPORTS = ["JFK", "LAX", "ORD", "DFW", "DEN", "SFO", "SEA", "ATL", "MIA", "BOS"]

# All write API actions processed by Redis Stream & StreamWorker
WRITE_APIS = [
    WriteAction.UPDATE_FLIGHT_STATUS,
    WriteAction.CREATE_CUSTOMER,
    WriteAction.CREATE_FLIGHT,
    WriteAction.CREATE_BOOKING,
    WriteAction.CANCEL_BOOKING,
]


@dataclass
class TimingRecord:
    event_id: str
    action: str
    entity: str
    redis_msg_id: str
    worker_name: str
    # Wall clock ISO timestamps
    enqueue_timestamp_iso: str
    worker_start_timestamp_iso: str
    tx_complete_timestamp_iso: str
    client_done_timestamp_iso: str
    # High-precision durations (milliseconds)
    queue_wait_ms: float
    tx_duration_ms: float
    pubsub_notification_ms: float
    total_latency_ms: float
    success: bool
    status_code: int
    error: str | None = None


class TimingWorker(StreamWorker):
    """Subclass of StreamWorker for high-precision timing measurement.

    Preserves 100% of the production processing and dynamic micro-batching transaction logic
    while capturing exact timestamps for message pickup, transaction start, and transaction commit.
    """

    def __init__(
        self,
        *args: Any,
        shared_timings: dict[str, dict[str, Any]] | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.shared_timings = shared_timings if shared_timings is not None else {}

    def on_batch_received(
        self,
        batch: list[tuple[str, str, str | None]],
        t_received_perf: float,
        t_received_wall: datetime,
    ) -> None:
        for msg_id, event_id, data_str in batch:
            if event_id not in self.shared_timings:
                self.shared_timings[event_id] = {
                    "redis_msg_id": msg_id,
                    "worker_name": self.consumer_name,
                    "worker_received_perf": t_received_perf,
                }

    def on_tx_start(
        self,
        batch: list[tuple[str, str, str | None]],
        t_tx_start_perf: float,
        t_tx_start_wall: datetime,
    ) -> None:
        for msg_id, event_id, data_str in batch:
            if event_id in self.shared_timings:
                self.shared_timings[event_id]["worker_start_perf"] = t_tx_start_perf
                self.shared_timings[event_id]["worker_start_wall"] = t_tx_start_wall

    def on_item_processed(
        self,
        event_id: str,
        result: WriteResult,
        t_item_start: float,
        t_item_end: float,
        msg_id: str,
    ) -> None:
        if event_id in self.shared_timings:
            self.shared_timings[event_id].update(
                {
                    "item_start_perf": t_item_start,
                    "item_end_perf": t_item_end,
                    "status_code": result.status_code,
                    "success": result.status == WriteResultStatus.SUCCESS,
                    "error": result.error,
                }
            )

    def on_tx_complete(
        self,
        batch: list[tuple[str, str, str | None]],
        t_tx_complete_perf: float,
        t_tx_complete_wall: datetime,
    ) -> None:
        for msg_id, event_id, data_str in batch:
            if event_id in self.shared_timings:
                self.shared_timings[event_id]["tx_complete_perf"] = t_tx_complete_perf
                self.shared_timings[event_id]["tx_complete_wall"] = t_tx_complete_wall

    async def process_one_batch(
        self, count: int | None = None, block: int | None = None
    ) -> int:
        return await super().process_one_batch(count=count, block=block)


def calc_percentile(sorted_list: list[float], p: float) -> float:
    """Calculates percentile with linear interpolation."""
    if not sorted_list:
        return 0.0
    k = (len(sorted_list) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_list[int(k)]
    return sorted_list[f] * (c - k) + sorted_list[c] * (k - f)


def stats_dict(values: list[float]) -> dict[str, float]:
    """Generates standard latency distribution metrics in milliseconds."""
    if not values:
        return {
            "count": 0,
            "mean": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "min": 0.0,
            "max": 0.0,
        }
    s = sorted(values)
    return {
        "count": len(values),
        "mean": round(statistics.mean(values), 2),
        "p50": round(calc_percentile(s, 50), 2),
        "p95": round(calc_percentile(s, 95), 2),
        "p99": round(calc_percentile(s, 99), 2),
        "min": round(min(values), 2),
        "max": round(max(values), 2),
    }


async def prefetch_seat_and_booking_pools(
    seat_limit: int = 500, booking_limit: int = 100
) -> tuple[list[int], list[int]]:
    """Prefetch pools of available seats and existing bookings for realistic payloads."""
    seats: list[int] = []
    bookings: list[int] = []

    try:
        async with async_session_factory() as session:
            # Query more available seats to cover both booking creation and seed bookings
            seat_stmt = (
                select(FlightSeat.flight_seat_id)
                .where(FlightSeat.status == FlightSeatStatus.AVAILABLE)
                .order_by(FlightSeat.flight_seat_id.desc())
                .limit(seat_limit + booking_limit)
            )
            seat_res = await session.execute(seat_stmt)
            seats = list(seat_res.scalars().all())

            b_stmt = (
                select(Booking.booking_id)
                .where(Booking.status == "PAID")
                .order_by(Booking.booking_id.desc())
                .limit(booking_limit)
            )
            b_res = await session.execute(b_stmt)
            bookings = list(b_res.scalars().all())

            # If booking pool is empty or insufficient (e.g. right after a reset_test_state.py run),
            # seed enough initial active bookings so CANCEL_BOOKING operations have valid IDs to cancel
            # rather than guessing random non-existent IDs.
            needed = booking_limit - len(bookings)
            if needed > 0 and len(seats) >= needed:
                seed_seats = [seats.pop() for _ in range(needed)]
                conn = await session.connection()
                raw = await conn.get_raw_connection()
                apg_conn = raw.driver_connection

                now_dt = datetime.now(timezone.utc)
                b_seq = await apg_conn.fetch(
                    "SELECT nextval('booking_booking_id_seq') AS id FROM generate_series(1, $1);",
                    needed,
                )
                b_ids = [r["id"] for r in b_seq]
                prefix = uuid.uuid4().hex[:4].upper()
                b_records = [
                    (
                        b_ids[i],
                        f"B{prefix}{i:07d}",
                        1,
                        Decimal("150.00"),
                        "PAID",
                        None,
                        now_dt,
                    )
                    for i in range(needed)
                ]
                await apg_conn.copy_records_to_table(
                    "booking",
                    records=b_records,
                    columns=[
                        "booking_id",
                        "booking_code",
                        "customer_id",
                        "total_amount",
                        "status",
                        "receipt_url",
                        "created_at",
                    ],
                )

                t_seq = await apg_conn.fetch(
                    "SELECT nextval('ticket_ticket_id_seq') AS id FROM generate_series(1, $1);",
                    needed,
                )
                t_records = [
                    (
                        t_seq[i]["id"],
                        b_ids[i],
                        seed_seats[i],
                        f"Init Passenger {i}",
                        Decimal("150.00"),
                        "ACTIVE",
                        None,
                        now_dt,
                    )
                    for i in range(needed)
                ]
                await apg_conn.copy_records_to_table(
                    "ticket",
                    records=t_records,
                    columns=[
                        "ticket_id",
                        "booking_id",
                        "flight_seat_id",
                        "passenger_name",
                        "price",
                        "status",
                        "ticket_url",
                        "issued_at",
                    ],
                )

                await apg_conn.execute(
                    """
                    UPDATE flight_seat AS fs
                    SET status = 'BOOKED'
                    FROM (SELECT * FROM UNNEST($1::bigint[]) AS t(flight_seat_id)) AS v
                    WHERE fs.flight_seat_id = v.flight_seat_id;
                    """,
                    seed_seats,
                )
                await session.commit()
                bookings.extend(b_ids)
    except Exception as e:
        print(f"Warning during prefetch: {e}")

    return seats, bookings


def build_operation_payload(
    action: WriteAction,
    op_idx: int,
    seat_pool: list[int],
    booking_pool: list[int],
) -> tuple[str, dict[str, Any]]:
    """Builds realistic payloads for each of the write APIs."""
    if action == WriteAction.UPDATE_FLIGHT_STATUS:
        flight_id = (op_idx % 25000) + 1
        status_val = random.choice(["SCHEDULED", "DELAYED", "DEPARTED"])
        return "flight", {"flight_id": flight_id, "status": status_val}

    elif action == WriteAction.CREATE_CUSTOMER:
        email = f"timing_user_{uuid.uuid4().hex[:10]}@bench.com"
        payload = {
            "first_name": "Timing",
            "last_name": f"User_{op_idx}",
            "email": email,
            "password": "timing_pass_123",
            "date_of_birth": "1993-08-14",
            "city": "Dhaka",
            "postal_code": "1200",
            "division": "Dhaka",
            "country": "Bangladesh",
        }
        return "customer", payload

    elif action == WriteAction.CREATE_FLIGHT:
        plane_id = random.randint(1, 100)
        dep = random.choice(AIRPORTS)
        arr = random.choice([a for a in AIRPORTS if a != dep])
        now = datetime.now(timezone.utc)
        dep_time = now + timedelta(days=random.randint(5, 60))
        arr_time = dep_time + timedelta(hours=random.randint(2, 6))
        payload = {
            "flight_number": f"TM{random.randint(1000, 9999)}",
            "plane_id": plane_id,
            "departure_airport": dep,
            "arrival_airport": arr,
            "departure_time": dep_time.isoformat(),
            "arrival_time": arr_time.isoformat(),
            "status": "SCHEDULED",
        }
        return "flight", payload

    elif action == WriteAction.CREATE_BOOKING:
        seat_id = seat_pool.pop() if seat_pool else random.randint(1, 100000)
        cust_id = random.randint(1, 1000)
        payload = {
            "customer_id": cust_id,
            "tickets": [
                {
                    "flight_seat_id": seat_id,
                    "passenger_name": f"Timing Pax {op_idx}",
                    "price": "195.50",
                }
            ],
        }
        return "booking", payload

    else:  # CANCEL_BOOKING
        b_id = (
            booking_pool.pop()
            if booking_pool
            else random.randint(1, 100)
        )
        return "booking", {"booking_id": b_id}


async def run_stream_timing_benchmark(
    total_operations: int = 50,
    concurrency: int = 10,
    stream_workers: int = 4,
    verbose: bool = True,
    json_report_path: str | None = None,
    md_report_path: str | None = None,
) -> dict[str, Any]:
    """Executes uniform operations across all write APIs and measures:

    1. Queue Wait Time (enqueue -> worker begins transaction)
    2. Transaction Duration (worker begins transaction -> commit)
    3. Total Latency (client dispatch -> result receipt)
    """
    # 1. Dynamically scale Redis connection pool
    effective_concurrency = concurrency + stream_workers
    dynamic_cap = tune_redis_connection_pool(effective_concurrency, multiplier=3)

    num_apis = len(WRITE_APIS)
    ops_per_api = total_operations // num_apis
    remainder = total_operations % num_apis

    # Build uniform list of operations across all write APIs
    uniform_task_list: list[WriteAction] = []
    for i, api_action in enumerate(WRITE_APIS):
        count = ops_per_api + (1 if i < remainder else 0)
        uniform_task_list.extend([api_action] * count)

    # Interleave / shuffle for realistic concurrent distribution
    random.shuffle(uniform_task_list)
    actual_total_operations = len(uniform_task_list)

    if verbose:
        print("\n" + "=" * 95)
        print("REDIS STREAM QUEUE WAIT & TRANSACTION DURATION TIMING BENCHMARK")
        print(
            f"Target: PgBouncer ({settings.PGBOUNCER_HOST}:{settings.PGBOUNCER_PORT}) & Redis ({settings.REDIS_HOST}:{settings.REDIS_PORT})"
        )
        print(
            f"Redis Connection Pool: Dynamic auto-scaling active (floor: {dynamic_cap:,}, formula: max(1000, ({concurrency} + {stream_workers}) * 3))"
        )
        print(
            f"Total Operations: {actual_total_operations:,} uniformly distributed across {num_apis} Write APIs ({ops_per_api} each)"
        )
        print(
            f"Concurrency: {concurrency} async client connections | Stream Workers: {stream_workers}"
        )
        print("=" * 95 + "\n")

    # 2. Prefetch available seats and bookings
    seat_pool, booking_pool = await prefetch_seat_and_booking_pools(
        seat_limit=max(100, actual_total_operations),
        booking_limit=max(100, actual_total_operations),
    )

    # 3. Initialize consumer group and background workers on an isolated benchmark stream
    bench_stream = f"stream:timing_bench:{uuid.uuid4().hex[:8]}"
    bench_group = f"group_timing_bench:{uuid.uuid4().hex[:8]}"
    shared_timings: dict[str, dict[str, Any]] = {}
    primary_worker = StreamWorker(
        stream_name=bench_stream,
        group_name=bench_group,
    )
    await primary_worker.init_consumer_group()

    worker_instances = [
        TimingWorker(
            stream_name=bench_stream,
            group_name=bench_group,
            consumer_name=f"timing-worker-{i}",
            shared_timings=shared_timings,
        )
        for i in range(stream_workers)
    ]
    worker_tasks = [
        asyncio.create_task(w.run_loop()) for w in worker_instances
    ]

    # Allow workers to enter continuous xreadgroup blocking loop
    await asyncio.sleep(0.05)

    bench_write_service = WriteStreamService(stream_name=bench_stream)

    # 4. Fill operation queue
    queue: asyncio.Queue = asyncio.Queue()
    for idx, action in enumerate(uniform_task_list):
        queue.put_nowait((idx, action))

    records: list[TimingRecord] = []
    created_booking_ids: list[int] = []
    bench_start_time = time.perf_counter()

    # 5. Client Worker Coroutine
    async def client_worker(worker_id: int):
        while not queue.empty():
            try:
                op_idx, action = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            # If action is CANCEL_BOOKING and we have freshly created bookings, prioritize them
            if action == WriteAction.CANCEL_BOOKING and created_booking_ids:
                b_id = created_booking_ids.pop()
                entity = "booking"
                payload = {"booking_id": b_id}
            else:
                entity, payload = build_operation_payload(
                    action, op_idx, seat_pool, booking_pool
                )

            t_enq_wall = datetime.now(timezone.utc)
            t_enq_perf = time.perf_counter()

            err_msg: str | None = None
            res_data: dict[str, Any] | None = None
            success = False
            event_id = str(uuid.uuid4())

            try:
                res_data = await bench_write_service.dispatch_and_wait(
                    action=action,
                    entity=entity,
                    payload=payload,
                    timeout=15.0,
                    event_id=event_id,
                )
                success = True
                if action == WriteAction.CREATE_BOOKING and res_data.get("booking_id"):
                    created_booking_ids.append(res_data["booking_id"])
            except Exception as ex:
                success = False
                err_msg = str(ex)
            finally:
                t_client_done_perf = time.perf_counter()
                t_client_done_wall = datetime.now(timezone.utc)

                # Look up worker timing from shared dictionary by exact event_id
                w_info = shared_timings.get(event_id)

                if w_info and "tx_complete_perf" in w_info and "worker_start_perf" in w_info:
                    q_wait_ms = max(
                        0.0,
                        (w_info["worker_start_perf"] - t_enq_perf) * 1000.0,
                    )
                    tx_dur_ms = max(
                        0.0,
                        (w_info["tx_complete_perf"] - w_info["worker_start_perf"]) * 1000.0,
                    )
                    pub_ms = max(
                        0.0,
                        (t_client_done_perf - w_info["tx_complete_perf"]) * 1000.0,
                    )
                    tot_lat_ms = max(
                        0.0,
                        (t_client_done_perf - t_enq_perf) * 1000.0,
                    )

                    records.append(
                        TimingRecord(
                            event_id=event_id,
                            action=action.value,
                            entity=entity,
                            redis_msg_id=w_info.get("redis_msg_id", ""),
                            worker_name=w_info.get("worker_name", ""),
                            enqueue_timestamp_iso=t_enq_wall.isoformat(),
                            worker_start_timestamp_iso=w_info[
                                "worker_start_wall"
                            ].isoformat(),
                            tx_complete_timestamp_iso=w_info[
                                "tx_complete_wall"
                            ].isoformat(),
                            client_done_timestamp_iso=t_client_done_wall.isoformat(),
                            queue_wait_ms=round(q_wait_ms, 2),
                            tx_duration_ms=round(tx_dur_ms, 2),
                            pubsub_notification_ms=round(pub_ms, 2),
                            total_latency_ms=round(tot_lat_ms, 2),
                            success=success and w_info.get("success", False),
                            status_code=w_info.get("status_code", 200),
                            error=err_msg or w_info.get("error"),
                        )
                    )
                else:
                    tot_lat_ms = max(
                        0.0,
                        (t_client_done_perf - t_enq_perf) * 1000.0,
                    )
                    op_success = success and (err_msg is None)
                    records.append(
                        TimingRecord(
                            event_id=event_id,
                            action=action.value,
                            entity=entity,
                            redis_msg_id=w_info.get("redis_msg_id", "") if w_info else "",
                            worker_name=w_info.get("worker_name", "timing-worker") if w_info else "timing-worker",
                            enqueue_timestamp_iso=t_enq_wall.isoformat(),
                            worker_start_timestamp_iso=t_client_done_wall.isoformat(),
                            tx_complete_timestamp_iso=t_client_done_wall.isoformat(),
                            client_done_timestamp_iso=t_client_done_wall.isoformat(),
                            queue_wait_ms=0.0,
                            tx_duration_ms=0.0,
                            pubsub_notification_ms=0.0,
                            total_latency_ms=round(tot_lat_ms, 2),
                            success=op_success,
                            status_code=200 if op_success else (504 if "timed out" in (err_msg or "") else 500),
                            error=err_msg,
                        )
                    )
                queue.task_done()

    # 6. Execute concurrent clients
    client_tasks = [
        asyncio.create_task(client_worker(i)) for i in range(concurrency)
    ]
    await asyncio.gather(*client_tasks)
    bench_elapsed = time.perf_counter() - bench_start_time

    # 7. Stop background stream workers
    for w in worker_instances:
        w.stop()
    for t in worker_tasks:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass

    redis = get_redis_client()
    await redis.delete(bench_stream)

    # 8. Compute detailed metrics
    total_completed = len(records)
    successes = sum(1 for r in records if r.success)
    failures = total_completed - successes
    throughput = (
        total_completed / bench_elapsed if bench_elapsed > 0 else 0.0
    )

    all_queue_waits = [r.queue_wait_ms for r in records]
    all_tx_durations = [r.tx_duration_ms for r in records]
    all_total_latencies = [r.total_latency_ms for r in records]

    # Group metrics per write API action
    api_groups: dict[str, list[TimingRecord]] = defaultdict(list)
    for r in records:
        api_groups[r.action].append(r)

    per_api_summary: dict[str, dict[str, Any]] = {}
    for action in WRITE_APIS:
        act_name = action.value
        group = api_groups.get(act_name, [])
        if not group:
            continue
        g_succ = sum(1 for r in group if r.success)
        g_q = [r.queue_wait_ms for r in group]
        g_tx = [r.tx_duration_ms for r in group]
        g_tot = [r.total_latency_ms for r in group]

        per_api_summary[act_name] = {
            "count": len(group),
            "success_rate_percent": round((g_succ / len(group)) * 100.0, 2),
            "queue_wait_ms": stats_dict(g_q),
            "tx_duration_ms": stats_dict(g_tx),
            "total_latency_ms": stats_dict(g_tot),
        }

    results = {
        "meta": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "target_pgbouncer": f"{settings.PGBOUNCER_HOST}:{settings.PGBOUNCER_PORT}",
            "target_redis": f"{settings.REDIS_HOST}:{settings.REDIS_PORT}",
            "total_operations": total_completed,
            "concurrency_clients": concurrency,
            "stream_workers": stream_workers,
            "elapsed_seconds": round(bench_elapsed, 2),
            "throughput_ops_per_sec": round(throughput, 2),
            "successes": successes,
            "failures": failures,
        },
        "overall_queue_wait_ms": stats_dict(all_queue_waits),
        "overall_tx_duration_ms": stats_dict(all_tx_durations),
        "overall_total_latency_ms": stats_dict(all_total_latencies),
        "per_api_breakdown": per_api_summary,
        "sample_records": [asdict(r) for r in records[:10]],
    }

    # 9. Formatted terminal output
    if verbose:
        print("\n" + "=" * 105)
        print("TIMING BENCHMARK EXECUTION SUMMARY")
        print("=" * 105)
        print(
            f" - Total Operations Completed: {total_completed:,} ({successes} ok, {failures} err)"
        )
        print(f" - Total Wall Clock Time:      {bench_elapsed:.2f} seconds")
        print(f" - Throughput:                 {throughput:.2f} ops/sec")
        print(
            f" - Queue Wait (p50 / p95):     {results['overall_queue_wait_ms']['p50']:.2f} ms / {results['overall_queue_wait_ms']['p95']:.2f} ms (Mean: {results['overall_queue_wait_ms']['mean']:.2f} ms)"
        )
        print(
            f" - Tx Duration (p50 / p95):    {results['overall_tx_duration_ms']['p50']:.2f} ms / {results['overall_tx_duration_ms']['p95']:.2f} ms (Mean: {results['overall_tx_duration_ms']['mean']:.2f} ms)"
        )
        print(
            f" - Total Latency (p50 / p95):  {results['overall_total_latency_ms']['p50']:.2f} ms / {results['overall_total_latency_ms']['p95']:.2f} ms (Mean: {results['overall_total_latency_ms']['mean']:.2f} ms)\n"
        )

        print("--- UNIFORM PER-API TIMING MATRIX ---")
        tbl_hdr = (
            f"{'API Action':<24} | {'Count':<5} | "
            f"{'Q-Wait p50':<10} | {'Q-Wait p95':<10} | "
            f"{'Tx p50':<9} | {'Tx p95':<9} | "
            f"{'Total p50':<9} | {'Total p95':<9}"
        )
        print(tbl_hdr)
        print("-" * len(tbl_hdr))
        for act_name, data in per_api_summary.items():
            print(
                f"{act_name:<24} | {data['count']:<5} | "
                f"{data['queue_wait_ms']['p50']:>8.2f}ms | {data['queue_wait_ms']['p95']:>8.2f}ms | "
                f"{data['tx_duration_ms']['p50']:>7.2f}ms | {data['tx_duration_ms']['p95']:>7.2f}ms | "
                f"{data['total_latency_ms']['p50']:>7.2f}ms | {data['total_latency_ms']['p95']:>7.2f}ms"
            )

        print("\n--- SAMPLE CHRONOLOGICAL TIMELINE (FIRST 5 OPS) ---")
        timeline_hdr = (
            f"{'Action':<22} | {'Enqueue Time':<13} | {'Worker Start':<13} | "
            f"{'Queue Wait':<11} | {'Tx Complete':<13} | {'Tx Duration':<11} | {'Total':<10} | {'Worker':<15}"
        )
        print(timeline_hdr)
        print("-" * len(timeline_hdr))
        for r in records[:5]:
            enq_str = r.enqueue_timestamp_iso.split("T")[1][:11]
            w_start_str = r.worker_start_timestamp_iso.split("T")[1][:11]
            tx_done_str = r.tx_complete_timestamp_iso.split("T")[1][:11]
            print(
                f"{r.action:<22} | {enq_str:<13} | {w_start_str:<13} | "
                f"{r.queue_wait_ms:>8.2f} ms | {tx_done_str:<13} | "
                f"{r.tx_duration_ms:>8.2f} ms | {r.total_latency_ms:>7.2f} ms | {r.worker_name:<15}"
            )
        print("=" * 105 + "\n")

    # 10. Optional report persistence
    if json_report_path:
        os.makedirs(os.path.dirname(json_report_path) or ".", exist_ok=True)
        with open(json_report_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        if verbose:
            print(f"Timing results saved to JSON: {json_report_path}")

    if md_report_path:
        os.makedirs(os.path.dirname(md_report_path) or ".", exist_ok=True)
        md_text = generate_timing_markdown_report(results)
        with open(md_report_path, "w", encoding="utf-8") as f:
            f.write(md_text)
        if verbose:
            print(f"Timing audit report saved to MD: {md_report_path}")

    return results


def generate_timing_markdown_report(results: dict[str, Any]) -> str:
    """Generates Github-flavored markdown report."""
    meta = results["meta"]
    q = results["overall_queue_wait_ms"]
    tx = results["overall_tx_duration_ms"]
    tot = results["overall_total_latency_ms"]

    lines = [
        "# Redis Stream Queue Wait & Transaction Duration Timing Report\n",
        f"**Date:** {meta['timestamp']}  \n",
        f"**Target:** {meta['target_pgbouncer']} (PgBouncer) & {meta['target_redis']} (Redis)  \n",
        f"**Concurrency:** {meta['concurrency_clients']} Client Connections | {meta['stream_workers']} Stream Workers  \n",
        f"**Throughput:** {meta['throughput_ops_per_sec']} ops/sec | **Total Operations:** {meta['total_operations']:,}\n\n",
        "## 1. Overall Timing Distribution\n\n",
        "| Phase | Mean (ms) | p50 Median (ms) | p95 (ms) | p99 (ms) | Min (ms) | Max (ms) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        f"| **1. Queue Wait Time** (`worker_start - enqueue`) | {q['mean']} | {q['p50']} | {q['p95']} | {q['p99']} | {q['min']} | {q['max']} |",
        f"| **2. Transaction Duration** (`commit - worker_start`) | {tx['mean']} | {tx['p50']} | {tx['p95']} | {tx['p99']} | {tx['min']} | {tx['max']} |",
        f"| **3. Total Latency** (`client_done - enqueue`) | {tot['mean']} | {tot['p50']} | {tot['p95']} | {tot['p99']} | {tot['min']} | {tot['max']} |",
        "\n## 2. Uniform Per-API Breakdown\n\n",
        "| API Action | Count | Queue Wait p50 (ms) | Queue Wait p95 (ms) | Tx Duration p50 (ms) | Tx Duration p95 (ms) | Total Latency p50 (ms) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]

    for act_name, data in results["per_api_breakdown"].items():
        lines.append(
            f"| `{act_name}` | {data['count']} | {data['queue_wait_ms']['p50']} | {data['queue_wait_ms']['p95']} | "
            f"{data['tx_duration_ms']['p50']} | {data['tx_duration_ms']['p95']} | {data['total_latency_ms']['p50']} |"
        )

    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Redis Stream Queue Wait Time & Transaction Duration Benchmark"
    )
    parser.add_argument(
        "--operations",
        type=int,
        default=50,
        help="Total number of operations distributed uniformly across all write APIs (default: 50)",
    )
    parser.add_argument(
        "--operations-per-api",
        type=int,
        default=None,
        help="Number of uniform operations per write API (overrides --operations if set)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Number of concurrent client connections dispatching events (default: 10)",
    )
    parser.add_argument(
        "--stream-workers",
        type=int,
        default=4,
        help="Number of background stream consumer workers (default: 4)",
    )
    parser.add_argument(
        "--json-report",
        type=str,
        default=None,
        help="Optional path to save JSON results output",
    )
    parser.add_argument(
        "--md-report",
        type=str,
        default=None,
        help="Optional path to save Markdown report output",
    )

    args = parser.parse_args()

    total_ops = (
        args.operations_per_api * len(WRITE_APIS)
        if args.operations_per_api is not None
        else args.operations
    )

    async def main():
        try:
            await run_stream_timing_benchmark(
                total_operations=total_ops,
                concurrency=args.concurrency,
                stream_workers=args.stream_workers,
                verbose=True,
                json_report_path=args.json_report,
                md_report_path=args.md_report,
            )
        finally:
            await close_redis_pool()

    asyncio.run(main())
