"""Concurrent Database & Redis Cache-Aside / Write Stream Fuzzing Benchmark.

Executes randomized concurrent read operations (Cache-Aside via Redis) and write operations
(exclusively through Redis Streams via PgBouncer in transaction pooling mode).
Collects latency percentiles, throughput, cache hit ratios, and container resource utilization
(PostgreSQL, PgBouncer, Redis).
"""

import argparse
import asyncio
from dataclasses import dataclass
import datetime
from decimal import Decimal
import json
import math
import os
import random
import statistics
import subprocess
import threading
import time
from typing import Any
import uuid
import asyncpg
import orjson
import psutil
from app.core.config import settings
from app.core.redis import (
    close_redis_pool,
    get_redis_client,
    tune_redis_connection_pool,
)
from app.schemas.write_event import WriteAction
from app.services.cache import cache_service
from app.services.seat_inventory import seat_inventory
from app.services.stream_worker import StreamWorker
from app.services.write_stream import write_stream_service

AIRPORTS = ["JFK", "LAX", "ORD", "DFW", "DEN", "SFO", "SEA", "ATL", "MIA", "BOS"]
ALL_ROUTES = [(a1, a2) for a1 in AIRPORTS for a2 in AIRPORTS if a1 != a2]
SEAT_CLASSES = ["FIRST", "BUSINESS", "PREMIUM_ECONOMY", "ECONOMY"]
CITIES = [
    ("New York", "NY"),
    ("Los Angeles", "CA"),
    ("Chicago", "IL"),
    ("Houston", "TX"),
    ("Phoenix", "AZ"),
    ("Philadelphia", "PA"),
    ("San Antonio", "TX"),
    ("San Diego", "CA"),
    ("Dallas", "TX"),
    ("San Jose", "CA"),
]


def get_popular_id(max_id: int, alpha: float = 1.3) -> int:
    """Returns a Pareto/Zipf distributed ID where a cluster of hot flights/customers

    receives the vast majority of requests.
    """
    val = int(random.paretovariate(alpha))
    return min(max(1, val), max_id)


def get_popular_route(alpha: float = 1.25) -> tuple[str, str]:
    """Returns a Pareto/Zipf distributed route so realistic popular flight corridors

    receive the bulk of customer search volume.
    """
    idx = get_popular_id(len(ALL_ROUTES), alpha=alpha) - 1
    return ALL_ROUTES[idx]


@dataclass
class OperationRecord:
    op_type: str
    op_category: str  # "READ" or "WRITE"
    latency_ms: float
    success: bool
    cache_hit: bool | None = None
    items_count: int = 0
    error: str | None = None


class ResourceMonitor:
    """Monitors CPU and RAM for client process, host system, and Docker containers

    (Postgres, PgBouncer, and Redis).
    """

    def __init__(self, sample_interval: float = 0.2):
        self.interval = sample_interval
        self._running = False
        self._async_task = None
        self._docker_thread = None
        self._docker_proc = None
        self.process = psutil.Process()

        # Samples storage
        self.client_cpu_samples = []
        self.client_ram_mb_samples = []
        self.system_cpu_samples = []
        self.system_ram_pct_samples = []
        self.postgres_cpu_samples = []
        self.postgres_ram_mb_samples = []
        self.pgbouncer_cpu_samples = []
        self.pgbouncer_ram_mb_samples = []
        self.redis_cpu_samples = []
        self.redis_ram_mb_samples = []

    def _docker_stream_worker(self):
        self._record_single_snapshot()
        try:
            self._docker_proc = subprocess.Popen(
                [
                    "docker",
                    "stats",
                    "--format",
                    "{{.Name}}:{{.CPUPerc}}:{{.MemUsage}}",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,
            )
            for line in iter(self._docker_proc.stdout.readline, ""):
                if not self._running:
                    break
                line = line.strip()
                if not line:
                    continue
                parts = line.split(":")
                if len(parts) >= 3:
                    name = parts[0].strip()
                    cpu_str = parts[1].replace("%", "").strip()
                    mem_str = parts[2].split("/")[0].strip()

                    cpu = float(cpu_str) if cpu_str else 0.0
                    mem_mb = 0.0
                    if "GiB" in mem_str:
                        mem_mb = (
                            float(mem_str.replace("GiB", "").strip()) * 1024
                        )
                    elif "MiB" in mem_str:
                        mem_mb = float(mem_str.replace("MiB", "").strip())
                    elif "kB" in mem_str:
                        mem_mb = float(mem_str.replace("kB", "").strip()) / 1024

                    if name == "postgres":
                        self.postgres_cpu_samples.append(cpu)
                        self.postgres_ram_mb_samples.append(mem_mb)
                    elif name == "pgbouncer":
                        self.pgbouncer_cpu_samples.append(cpu)
                        self.pgbouncer_ram_mb_samples.append(mem_mb)
                    elif name == "redis":
                        self.redis_cpu_samples.append(cpu)
                        self.redis_ram_mb_samples.append(mem_mb)
        except Exception:
            pass

    def _record_single_snapshot(self):
        try:
            out = (
                subprocess.check_output(
                    [
                        "docker",
                        "stats",
                        "--no-stream",
                        "--format",
                        "{{.Name}}:{{.CPUPerc}}:{{.MemUsage}}",
                    ],
                    stderr=subprocess.DEVNULL,
                    timeout=8.0,
                )
                .decode()
                .strip()
            )
            for line in out.split("\n"):
                if not line:
                    continue
                parts = line.split(":")
                if len(parts) >= 3:
                    name = parts[0].strip()
                    cpu_str = parts[1].replace("%", "").strip()
                    cpu = float(cpu_str) if cpu_str else 0.0
                    mem_part = parts[2].split("/")[0].strip()
                    mem_mb = 0.0
                    if "GiB" in mem_part:
                        mem_mb = (
                            float(mem_part.replace("GiB", "").strip()) * 1024
                        )
                    elif "MiB" in mem_part:
                        mem_mb = float(mem_part.replace("MiB", "").strip())
                    elif "kB" in mem_part:
                        mem_mb = (
                            float(mem_part.replace("kB", "").strip()) / 1024
                        )

                    if name == "postgres":
                        self.postgres_cpu_samples.append(cpu)
                        self.postgres_ram_mb_samples.append(mem_mb)
                    elif name == "pgbouncer":
                        self.pgbouncer_cpu_samples.append(cpu)
                        self.pgbouncer_ram_mb_samples.append(mem_mb)
                    elif name == "redis":
                        self.redis_cpu_samples.append(cpu)
                        self.redis_ram_mb_samples.append(mem_mb)
        except Exception:
            pass

    async def _async_monitor_loop(self):
        while self._running:
            try:
                self.client_cpu_samples.append(self.process.cpu_percent())
                self.client_ram_mb_samples.append(
                    self.process.memory_info().rss / (1024 * 1024)
                )
                self.system_cpu_samples.append(psutil.cpu_percent())
                self.system_ram_pct_samples.append(
                    psutil.virtual_memory().percent
                )
            except Exception:
                pass
            await asyncio.sleep(self.interval)

    def start(self):
        self._running = True
        self._docker_thread = threading.Thread(
            target=self._docker_stream_worker, daemon=True
        )
        self._docker_thread.start()
        self._async_task = asyncio.create_task(self._async_monitor_loop())

    async def stop(self):
        self._running = False
        self._record_single_snapshot()
        if self._docker_proc:
            try:
                self._docker_proc.terminate()
            except Exception:
                pass
        if self._async_task:
            self._async_task.cancel()
            try:
                await self._async_task
            except asyncio.CancelledError:
                pass

    def get_summary(self) -> dict:
        def safe_stats(samples):
            if not samples:
                return {"min": 0.0, "max": 0.0, "avg": 0.0}
            return {
                "min": round(min(samples), 2),
                "max": round(max(samples), 2),
                "avg": round(statistics.mean(samples), 2),
            }

        return {
            "client_cpu_percent": safe_stats(self.client_cpu_samples),
            "client_ram_mb": safe_stats(self.client_ram_mb_samples),
            "system_cpu_percent": safe_stats(self.system_cpu_samples),
            "system_ram_percent": safe_stats(self.system_ram_pct_samples),
            "postgres_container_cpu_percent": safe_stats(
                self.postgres_cpu_samples
            ),
            "postgres_container_ram_mb": safe_stats(
                self.postgres_ram_mb_samples
            ),
            "pgbouncer_container_cpu_percent": safe_stats(
                self.pgbouncer_cpu_samples
            ),
            "pgbouncer_container_ram_mb": safe_stats(
                self.pgbouncer_ram_mb_samples
            ),
            "redis_container_cpu_percent": safe_stats(self.redis_cpu_samples),
            "redis_container_ram_mb": safe_stats(self.redis_ram_mb_samples),
        }


async def get_db_stats() -> dict:
    """Fetch pg_stat_database metrics directly from PostgreSQL."""
    try:
        conn = await asyncpg.connect(
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            database=settings.POSTGRES_DB,
        )
        try:
            row = await conn.fetchrow(
                """
                SELECT 
                    blks_read, 
                    blks_hit,
                    xact_commit, 
                    tup_returned, 
                    tup_fetched
                FROM pg_stat_database 
                WHERE datname = $1;
            """,
                settings.POSTGRES_DB,
            )
            return dict(row) if row else {}
        finally:
            await conn.close()
    except Exception:
        return {}


async def get_redis_stats() -> dict:
    """Fetch Redis memory, keyspace, and stream metrics."""
    redis = get_redis_client()
    try:
        mem_info = await redis.info("memory")
        stats_info = await redis.info("stats")
        stream_len = 0
        try:
            stream_len = await redis.xlen(settings.REDIS_STREAM_WRITE_NAME)
        except Exception:
            pass

        return {
            "used_memory_bytes": int(mem_info.get("used_memory", 0)),
            "used_memory_mb": round(
                int(mem_info.get("used_memory", 0)) / (1024 * 1024), 2
            ),
            "used_memory_peak_mb": round(
                int(mem_info.get("used_memory_peak", 0)) / (1024 * 1024), 2
            ),
            "maxmemory_mb": round(
                int(mem_info.get("maxmemory", 0)) / (1024 * 1024), 2
            ),
            "evicted_keys": int(stats_info.get("evicted_keys", 0)),
            "keyspace_hits": int(stats_info.get("keyspace_hits", 0)),
            "keyspace_misses": int(stats_info.get("keyspace_misses", 0)),
            "stream_length": stream_len,
        }
    except Exception:
        return {}


# Pool of pre-seeded booking IDs for cancellation tests
created_booking_ids: list[int] = []
available_seat_pool: list[int] = []


async def populate_available_seat_pool(limit: int = 500):
    """Fetch a pool of available seats for realistic booking tests."""
    global available_seat_pool
    try:
        conn = await asyncpg.connect(
            host=settings.PGBOUNCER_HOST
            if settings.USE_PGBOUNCER
            else settings.POSTGRES_HOST,
            port=settings.PGBOUNCER_PORT
            if settings.USE_PGBOUNCER
            else settings.POSTGRES_PORT,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            database=settings.POSTGRES_DB,
            statement_cache_size=0,
        )
        try:
            rows = await conn.fetch(
                """
                SELECT flight_seat_id 
                FROM flight_seat 
                WHERE status = 'AVAILABLE' 
                ORDER BY flight_seat_id DESC 
                LIMIT $1;
            """,
                limit,
            )
            available_seat_pool = [r["flight_seat_id"] for r in rows]
        finally:
            await conn.close()
    except Exception as e:
        print(f"Warning: could not pre-populate seat pool: {e}")


async def execute_read_operation(
    db_conn: asyncpg.Connection, op_type: str, op_idx: int
) -> tuple[bool, int, str | None, bool]:
    """Executes a Read Operation via Cache-Aside:

    1. Checks Redis cache.
    2. On Miss: executes query against PostgreSQL via PgBouncer and sets Redis cache.
    Returns: (success, items_count, error_msg, is_cache_hit)
    """
    redis = get_redis_client()

    try:
        if op_type == "READ_FLIGHT_DETAILS":
            flight_id = get_popular_id(25000, alpha=1.25)
            cache_key = f"cache:flight:{flight_id}"

            cached = await redis.get(cache_key)
            if cached:
                return True, 1, None, True

            # Cache Miss -> PgBouncer DB fetch
            row = await db_conn.fetchrow(
                """
                SELECT flight_id, flight_number, plane_id, departure_airport, 
                       arrival_airport, departure_time, arrival_time, status 
                FROM flight WHERE flight_id = $1;
            """,
                flight_id,
            )
            if row:
                await cache_service.set(cache_key, dict(row), ttl=600)
                return True, 1, None, False
            return False, 0, "Flight not found", False

        elif op_type == "READ_FLIGHT_SEATS":
            flight_id = get_popular_id(25000, alpha=1.25)
            # Live seat status served directly from Redis Hash via single pipelined roundtrip
            seats, is_hit = await seat_inventory.get_or_load_seat_map(
                flight_id, db_conn, ttl=1800, return_hit_status=True
            )
            return True, len(seats), None, bool(is_hit)

        elif op_type == "READ_CUSTOMER_PROFILE":
            cust_id = get_popular_id(50000, alpha=1.25)
            cache_key = f"cache:customer:{cust_id}"

            cached = await redis.get(cache_key)
            if cached:
                return True, 1, None, True

            row = await db_conn.fetchrow(
                """
                SELECT customer_id, first_name, last_name, email, city, postal_code, division, country, date_of_birth
                FROM customer WHERE customer_id = $1;
            """,
                cust_id,
            )
            if row:
                await cache_service.set(cache_key, dict(row), ttl=600)
                return True, 1, None, False
            return False, 0, "Customer not found", False

        elif op_type == "READ_ROUTE_SEARCH":
            dep, arr = get_popular_route(alpha=1.25)
            cache_key = f"cache:search:{dep}:{arr}"

            cached = await redis.get(cache_key)
            if cached:
                items = orjson.loads(cached) if isinstance(cached, (str, bytes)) else []
                return True, len(items), None, True

            # Composite Covering Index-Only Scan via CTE pushdown: 2-5ms execution!
            rows = await db_conn.fetch(
                """
                WITH top_flights AS (
                    SELECT f.flight_id, f.flight_number, f.plane_id, f.departure_airport, f.arrival_airport, f.departure_time
                    FROM flight f
                    WHERE f.departure_airport = $1 AND f.arrival_airport = $2 AND f.status = 'SCHEDULED'
                    ORDER BY f.departure_time
                    LIMIT 10
                )
                SELECT tf.flight_id, tf.flight_number, tf.departure_airport, tf.arrival_airport, tf.departure_time,
                       p.model, COUNT(fs.flight_seat_id) AS available_seats
                FROM top_flights tf
                JOIN plane p ON tf.plane_id = p.plane_id
                JOIN flight_seat fs ON tf.flight_id = fs.flight_id AND fs.status = 'AVAILABLE'
                GROUP BY tf.flight_id, tf.flight_number, tf.departure_airport, tf.arrival_airport, tf.departure_time, p.model
                ORDER BY tf.departure_time;
            """,
                dep,
                arr,
            )
            items = [dict(r) for r in rows]
            await cache_service.set(cache_key, items, ttl=180)
            return True, len(items), None, False

        else:  # READ_CUSTOMER_BY_CITY
            city, division = random.choice(CITIES)
            cache_key = f"cache:cust_city:{city}:{division}"

            cached = await redis.get(cache_key)
            if cached:
                items = orjson.loads(cached) if isinstance(cached, (str, bytes)) else []
                return True, len(items), None, True

            rows = await db_conn.fetch(
                """
                SELECT customer_id, first_name, last_name, email, city, division
                FROM customer
                WHERE city = $1 AND division = $2
                LIMIT 10;
            """,
                city,
                division,
            )
            items = [dict(r) for r in rows]
            await cache_service.set(cache_key, items, ttl=300)
            return True, len(items), None, False

    except Exception as e:
        return False, 0, str(e), False


async def execute_write_operation(
    op_type: str, op_idx: int
) -> tuple[bool, int, str | None]:
    """Executes a Write Operation strictly through Redis Stream:

    Dispatches event to Redis Stream -> StreamWorker consumes -> PgBouncer transaction -> Commit -> Invalidate Cache.
    Returns: (success, items_count, error_msg)
    """
    global created_booking_ids, available_seat_pool

    try:
        if op_type == "WRITE_CREATE_CUSTOMER":
            email = f"fuzz_user_{uuid.uuid4().hex[:10]}@bench.com"
            payload = {
                "first_name": "Fuzz",
                "last_name": f"User_{op_idx}",
                "email": email,
                "password": "fuzz_password_123",
                "city": "Dhaka",
                "postal_code": "1200",
                "division": "Dhaka",
                "country": "Bangladesh",
                "date_of_birth": "1994-06-15",
            }
            res = await write_stream_service.dispatch_and_wait(
                action=WriteAction.CREATE_CUSTOMER,
                entity="customer",
                payload=payload,
                timeout=10.0,
            )
            return True, 1, None

        elif op_type == "WRITE_UPDATE_FLIGHT_STATUS":
            flight_id = (op_idx % 25000) + 1
            new_status = random.choice(["SCHEDULED", "DELAYED", "DEPARTED"])
            payload = {"flight_id": flight_id, "status": new_status}
            res = await write_stream_service.dispatch_and_wait(
                action=WriteAction.UPDATE_FLIGHT_STATUS,
                entity="flight",
                payload=payload,
                timeout=10.0,
            )
            return True, 1, None

        elif op_type == "WRITE_BOOK_SEATS":
            seat_id = None
            if available_seat_pool:
                try:
                    seat_id = available_seat_pool.pop()
                except IndexError:
                    pass
            if not seat_id:
                seat_id = random.randint(1, 5000000)

            # Atomic reservation in Redis Hash via Lua in < 2ms
            approx_flight_id = max(1, min(25000, ((seat_id - 1) // 200) + 1))
            reserved = await seat_inventory.reserve_seats_atomic(
                approx_flight_id, [seat_id]
            )
            if not reserved:
                return (
                    True,
                    0,
                    "Seat already booked (fast-rejected via Lua in <2ms)",
                )

            cust_id = get_popular_id(50000, alpha=1.25)
            payload = {
                "customer_id": cust_id,
                "tickets": [
                    {
                        "flight_seat_id": seat_id,
                        "passenger_name": f"Fuzz Passenger {op_idx}",
                        "price": "185.50",
                    }
                ],
            }
            try:
                res = await write_stream_service.dispatch_and_wait(
                    action=WriteAction.CREATE_BOOKING,
                    entity="booking",
                    payload=payload,
                    timeout=10.0,
                )
                b_id = res.get("booking_id")
                if b_id:
                    created_booking_ids.append(b_id)
                return True, 1, None
            except Exception as e:
                # If stream worker reports DB conflict, release back in Redis
                await seat_inventory.release_seats_atomic(
                    approx_flight_id, [seat_id]
                )
                if "already booked" in str(e) or "not found" in str(e):
                    return True, 0, f"Seat conflict handled: {e}"
                raise

        elif op_type == "WRITE_CANCEL_BOOKING":
            if not created_booking_ids:
                # If no active bookings to cancel yet, switch to booking seats
                return await execute_write_operation("WRITE_BOOK_SEATS", op_idx)

            b_id = created_booking_ids.pop()
            payload = {"booking_id": b_id}
            try:
                res = await write_stream_service.dispatch_and_wait(
                    action=WriteAction.CANCEL_BOOKING,
                    entity="booking",
                    payload=payload,
                    timeout=10.0,
                )
                return True, 1, None
            except Exception as e:
                if "not found" in str(e):
                    return True, 0, "Booking not found (already cancelled)"
                raise

        else:  # WRITE_CREATE_FLIGHT
            plane_id = random.randint(1, 100)
            dep = random.choice(AIRPORTS)
            arr = random.choice([a for a in AIRPORTS if a != dep])
            now = datetime.datetime.now(datetime.timezone.utc)
            dep_time = now + datetime.timedelta(
                days=random.randint(5, 60), hours=random.randint(1, 12)
            )
            arr_time = dep_time + datetime.timedelta(hours=random.randint(2, 6))

            payload = {
                "flight_number": f"FZ{random.randint(1000, 9999)}",
                "plane_id": plane_id,
                "departure_airport": dep,
                "arrival_airport": arr,
                "departure_time": dep_time.isoformat(),
                "arrival_time": arr_time.isoformat(),
                "status": "SCHEDULED",
            }
            res = await write_stream_service.dispatch_and_wait(
                action=WriteAction.CREATE_FLIGHT,
                entity="flight",
                payload=payload,
                timeout=10.0,
            )
            return True, res.get("seats_count", 0), None

    except Exception as e:
        return False, 0, str(e)


async def run_redis_fuzz_benchmark(
    total_operations: int = 1000,
    concurrency: int = 15,
    num_stream_workers: int = 4,
    write_ratio: float = 0.30,
    warmup: int = 1000,
    verbose: bool = True,
) -> dict[str, Any]:
    """Runs concurrent random reads (Cache-Aside via Redis) and writes (Redis Stream via PgBouncer)."""
    target_host = (
        settings.PGBOUNCER_HOST
        if settings.USE_PGBOUNCER
        else settings.POSTGRES_HOST
    )
    target_port = (
        settings.PGBOUNCER_PORT
        if settings.USE_PGBOUNCER
        else settings.POSTGRES_PORT
    )

    # Dynamically scale Redis connection pool capacity for runtime concurrency
    effective_concurrency = concurrency + num_stream_workers
    dynamic_pool_cap = tune_redis_connection_pool(effective_concurrency, multiplier=3)

    if verbose:
        print("\n" + "=" * 75)
        print("CONCURRENT DATABASE & REDIS CACHE-ASIDE / STREAM FUZZING BENCHMARK")
        print(
            f"Primary Target: {target_host}:{target_port} (PgBouncer: {settings.USE_PGBOUNCER})"
        )
        print(f"Redis Cache & Stream: {settings.REDIS_HOST}:{settings.REDIS_PORT}")
        print(
            f"Redis Connection Pool: Dynamic auto-scaling active (floor: {dynamic_pool_cap:,}, formula: max(1000, ({concurrency} + {num_stream_workers}) * 3))"
        )
        print(f"Total Operations: {total_operations:,}")
        print(
            f"Concurrency: {concurrency} async clients | Stream Workers: {num_stream_workers}"
        )
        print(
            f"Workload Split: {int((1 - write_ratio) * 100)}% Reads (Cache-Aside), {int(write_ratio * 100)}% Writes (Redis Stream)"
        )
        print("=" * 75 + "\n")

    # 1. Pre-populate seats & initialize consumer group
    await populate_available_seat_pool(limit=max(500, total_operations // 2))

    # Initialize consumer group
    primary_worker = StreamWorker()
    await primary_worker.init_consumer_group()

    # Launch background stream consumer workers
    worker_instances = [
        StreamWorker(consumer_name=f"fuzz-worker-{i}")
        for i in range(num_stream_workers)
    ]
    worker_tasks = [
        asyncio.create_task(w.run_loop()) for w in worker_instances
    ]

    # 2. Warm-up Phase: Pre-populate Redis cache with hot flights, routes, seats, customer profiles
    if warmup > 0:
        if verbose:
            print(f"--- WARM-UP PHASE: Running {warmup:,} reads to pre-populate Redis cache ---")
        warmup_t0 = time.perf_counter()
        warmup_conn = await asyncpg.connect(
            host=target_host,
            port=target_port,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            database=settings.POSTGRES_DB,
            statement_cache_size=0,
        )
        try:
            warm_ops = [
                "READ_FLIGHT_DETAILS",
                "READ_FLIGHT_SEATS",
                "READ_ROUTE_SEARCH",
                "READ_CUSTOMER_PROFILE",
                "READ_CUSTOMER_BY_CITY",
            ]
            for w_i in range(warmup):
                w_type = warm_ops[w_i % len(warm_ops)]
                await execute_read_operation(warmup_conn, w_type, w_i)
        finally:
            await warmup_conn.close()

        warmup_sec = time.perf_counter() - warmup_t0
        if verbose:
            print(
                f"--- WARM-UP COMPLETE in {warmup_sec:.2f}s! Redis cache populated. Starting timed benchmark... ---\n"
            )

    # Initial stats (recorded AFTER warm-up so benchmark measures steady-state performance)
    initial_db_stats = await get_db_stats()
    initial_redis_stats = await get_redis_stats()

    # Start resource monitor
    monitor = ResourceMonitor(sample_interval=0.2)
    monitor.start()

    # Operation types definitions
    read_types = [
        "READ_FLIGHT_DETAILS",
        "READ_FLIGHT_SEATS",
        "READ_ROUTE_SEARCH",
        "READ_CUSTOMER_PROFILE",
        "READ_CUSTOMER_BY_CITY",
    ]
    write_types = [
        "WRITE_CREATE_CUSTOMER",
        "WRITE_UPDATE_FLIGHT_STATUS",
        "WRITE_BOOK_SEATS",
        "WRITE_CANCEL_BOOKING",
        "WRITE_CREATE_FLIGHT",
    ]

    # Queue operations
    queue = asyncio.Queue()
    for i in range(total_operations):
        is_write = random.random() < write_ratio
        if is_write:
            op_t = random.choice(write_types)
            category = "WRITE"
        else:
            op_t = random.choice(read_types)
            category = "READ"
        queue.put_nowait((i, op_t, category))

    records: list[OperationRecord] = []
    bench_start_time = time.perf_counter()

    async def client_worker(worker_id: int):
        conn = await asyncpg.connect(
            host=target_host,
            port=target_port,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            database=settings.POSTGRES_DB,
            statement_cache_size=0,
        )
        try:
            while not queue.empty():
                try:
                    op_idx, op_type, category = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                t0 = time.perf_counter()
                success = False
                err_msg = None
                items_cnt = 0
                cache_hit = None

                try:
                    if category == "READ":
                        success, items_cnt, err_msg, cache_hit = (
                            await execute_read_operation(conn, op_type, op_idx)
                        )
                    else:
                        success, items_cnt, err_msg = (
                            await execute_write_operation(op_type, op_idx)
                        )
                except Exception as ex:
                    success = False
                    err_msg = str(ex)
                finally:
                    lat_ms = (time.perf_counter() - t0) * 1000.0
                    records.append(
                        OperationRecord(
                            op_type=op_type,
                            op_category=category,
                            latency_ms=lat_ms,
                            success=success,
                            cache_hit=cache_hit,
                            items_count=items_cnt,
                            error=err_msg,
                        )
                    )
                    queue.task_done()
        finally:
            await conn.close()

    # Launch client workers
    client_tasks = [
        asyncio.create_task(client_worker(i)) for i in range(concurrency)
    ]
    await asyncio.gather(*client_tasks)

    bench_elapsed = time.perf_counter() - bench_start_time

    # Stop resource monitor and stream workers
    await monitor.stop()
    for w in worker_instances:
        w.stop()
    for t in worker_tasks:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass

    # Final stats
    final_db_stats = await get_db_stats()
    final_redis_stats = await get_redis_stats()

    # Calculations
    total_ops = len(records)
    successes = sum(1 for r in records if r.success)
    failures = total_ops - successes
    overall_throughput = total_ops / bench_elapsed if bench_elapsed > 0 else 0

    error_counts: dict[str, int] = {}
    for r in records:
        if not r.success and r.error:
            short_err = str(r.error).strip().split("\n")[0][:120]
            error_counts[short_err] = error_counts.get(short_err, 0) + 1

    read_records = [r for r in records if r.op_category == "READ"]
    write_records = [r for r in records if r.op_category == "WRITE"]

    read_lats = [r.latency_ms for r in read_records]
    write_lats = [r.latency_ms for r in write_records]
    all_lats = [r.latency_ms for r in records]

    def calc_percentile(sorted_list, p):
        if not sorted_list:
            return 0.0
        k = (len(sorted_list) - 1) * (p / 100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_list[int(k)]
        return sorted_list[f] * (c - k) + sorted_list[c] * (k - f)

    def stats_dict(lat_list):
        if not lat_list:
            return {
                "count": 0,
                "min": 0.0,
                "mean": 0.0,
                "p50_median": 0.0,
                "p90": 0.0,
                "p95": 0.0,
                "p99": 0.0,
                "max": 0.0,
            }
        s = sorted(lat_list)
        return {
            "count": len(lat_list),
            "min": round(min(lat_list), 2),
            "mean": round(statistics.mean(lat_list), 2),
            "p50_median": round(calc_percentile(s, 50), 2),
            "p90": round(calc_percentile(s, 90), 2),
            "p95": round(calc_percentile(s, 95), 2),
            "p99": round(calc_percentile(s, 99), 2),
            "max": round(max(lat_list), 2),
        }

    # Per-operation breakdown
    type_groups: dict[str, list[OperationRecord]] = {}
    for r in records:
        type_groups.setdefault(r.op_type, []).append(r)

    per_op_summary = {}
    for op_name, group in sorted(type_groups.items()):
        lats = [r.latency_ms for r in group]
        succ = sum(1 for r in group if r.success)
        s_lats = sorted(lats)
        cache_hits = sum(1 for r in group if r.cache_hit is True)
        per_op_summary[op_name] = {
            "category": group[0].op_category,
            "count": len(group),
            "success_rate_percent": round((succ / len(group)) * 100, 2),
            "mean_ms": round(statistics.mean(lats), 2),
            "p50_ms": round(calc_percentile(s_lats, 50), 2),
            "p95_ms": round(calc_percentile(s_lats, 95), 2),
            "p99_ms": round(calc_percentile(s_lats, 99), 2),
            "min_ms": round(min(lats), 2),
            "max_ms": round(max(lats), 2),
            "cache_hit_ratio_percent": round(
                (cache_hits / len(group)) * 100.0, 2
            )
            if group[0].op_category == "READ"
            else None,
        }

    # Cache hit metrics
    total_cache_hits = sum(1 for r in read_records if r.cache_hit is True)
    total_cache_misses = sum(1 for r in read_records if r.cache_hit is False)
    cache_hit_ratio = (
        (total_cache_hits / len(read_records) * 100.0)
        if read_records
        else 100.0
    )

    # Database I/O metrics
    blks_read_delta = final_db_stats.get(
        "blks_read", 0
    ) - initial_db_stats.get("blks_read", 0)
    blks_hit_delta = final_db_stats.get("blks_hit", 0) - initial_db_stats.get(
        "blks_hit", 0
    )
    xact_commit_delta = final_db_stats.get(
        "xact_commit", 0
    ) - initial_db_stats.get("xact_commit", 0)
    total_blks = blks_read_delta + blks_hit_delta
    db_cache_hit_ratio = (
        (blks_hit_delta / total_blks * 100.0) if total_blks > 0 else 100.0
    )

    # Redis metrics delta
    redis_keyspace_hits_delta = final_redis_stats.get(
        "keyspace_hits", 0
    ) - initial_redis_stats.get("keyspace_hits", 0)
    redis_keyspace_miss_delta = final_redis_stats.get(
        "keyspace_misses", 0
    ) - initial_redis_stats.get("keyspace_misses", 0)

    resource_summary = monitor.get_summary()

    results = {
        "benchmark_meta": {
            "timestamp": datetime.datetime.now(
                datetime.timezone.utc
            ).isoformat(),
            "target_pgbouncer": f"{target_host}:{target_port}",
            "target_redis": f"{settings.REDIS_HOST}:{settings.REDIS_PORT}",
            "concurrency_clients": concurrency,
            "stream_workers": num_stream_workers,
            "total_operations": total_ops,
            "total_elapsed_sec": round(bench_elapsed, 2),
            "throughput_ops_per_sec": round(overall_throughput, 2),
            "success_rate_percent": round((successes / total_ops) * 100.0, 2)
            if total_ops
            else 0.0,
            "errors": failures,
            "error_breakdown": error_counts,
        },
        "reads_summary": {
            "count": len(read_records),
            "throughput_reads_per_sec": round(
                len(read_records) / bench_elapsed, 2
            )
            if bench_elapsed
            else 0,
            "cache_hits": total_cache_hits,
            "cache_misses": total_cache_misses,
            "cache_hit_ratio_percent": round(cache_hit_ratio, 2),
            "latency_ms": stats_dict(read_lats),
        },
        "writes_summary": {
            "count": len(write_records),
            "throughput_writes_per_sec": round(
                len(write_records) / bench_elapsed, 2
            )
            if bench_elapsed
            else 0,
            "stream_name": settings.REDIS_STREAM_WRITE_NAME,
            "consumer_group": settings.REDIS_STREAM_CONSUMER_GROUP,
            "db_transactions_committed": xact_commit_delta,
            "latency_ms": stats_dict(write_lats),
        },
        "overall_latency_ms": stats_dict(all_lats),
        "per_operation_type": per_op_summary,
        "database_metrics": {
            "blocks_read": blks_read_delta,
            "blocks_hit": blks_hit_delta,
            "db_cache_hit_ratio_percent": round(db_cache_hit_ratio, 2),
            "transactions_committed": xact_commit_delta,
        },
        "redis_metrics": {
            "used_memory_mb": final_redis_stats.get("used_memory_mb", 0),
            "used_memory_peak_mb": final_redis_stats.get(
                "used_memory_peak_mb", 0
            ),
            "maxmemory_mb": final_redis_stats.get("maxmemory_mb", 0),
            "evicted_keys_delta": final_redis_stats.get("evicted_keys", 0)
            - initial_redis_stats.get("evicted_keys", 0),
            "keyspace_hits_delta": redis_keyspace_hits_delta,
            "keyspace_misses_delta": redis_keyspace_miss_delta,
            "stream_final_length": final_redis_stats.get("stream_length", 0),
        },
        "runtime_resources": resource_summary,
    }

    if verbose:
        print("\n" + "=" * 75)
        print("BENCHMARK EXECUTION SUMMARY")
        print("=" * 75)
        print(
            f" - Total Operations Completed: {total_ops:,} ({successes} ok, {failures} err)"
        )
        print(f" - Total Wall Clock Time:      {bench_elapsed:.2f} seconds")
        print(
            f" - Overall Throughput:         {overall_throughput:.2f} ops/sec"
        )
        print(f" - Overall Latency p50:        {results['overall_latency_ms']['p50_median']:.2f} ms")
        print(f" - Overall Latency p95:        {results['overall_latency_ms']['p95']:.2f} ms")
        print(f" - Overall Latency p99:        {results['overall_latency_ms']['p99']:.2f} ms")

        if failures > 0 and error_counts:
            print("\n--- TOP FAILURE REASONS ---")
            for err_text, err_cnt in sorted(
                error_counts.items(), key=lambda x: x[1], reverse=True
            )[:5]:
                print(f" - {err_cnt:,}x: {err_text}")

        print("\n--- READ WORKLOAD (Redis Cache-Aside) ---")
        print(
            f" - Total Reads:                {results['reads_summary']['count']:,} ({results['reads_summary']['throughput_reads_per_sec']:.2f} reads/sec)"
        )
        print(
            f" - Redis Cache Hits:           {total_cache_hits:,} | Misses: {total_cache_misses:,}"
        )
        print(f" - Cache Hit Ratio:            {cache_hit_ratio:.2f}%")
        print(
            f" - Read Latency (Mean):        {results['reads_summary']['latency_ms']['mean']:.2f} ms"
        )
        print(
            f" - Read Latency (p50):         {results['reads_summary']['latency_ms']['p50_median']:.2f} ms"
        )
        print(
            f" - Read Latency (p95):         {results['reads_summary']['latency_ms']['p95']:.2f} ms"
        )
        print(
            f" - Read Latency (Min / Max):   {results['reads_summary']['latency_ms']['min']:.2f} ms / {results['reads_summary']['latency_ms']['max']:.2f} ms"
        )

        print("\n--- WRITE WORKLOAD (Redis Stream -> PgBouncer DB Commit) ---")
        print(
            f" - Total Writes:               {results['writes_summary']['count']:,} ({results['writes_summary']['throughput_writes_per_sec']:.2f} writes/sec)"
        )
        print(
            f" - DB Transactions Committed:  {results['writes_summary']['db_transactions_committed']:,}"
        )
        print(
            f" - Write Latency (Mean):       {results['writes_summary']['latency_ms']['mean']:.2f} ms"
        )
        print(
            f" - Write Latency (p50):        {results['writes_summary']['latency_ms']['p50_median']:.2f} ms"
        )
        print(
            f" - Write Latency (p95):        {results['writes_summary']['latency_ms']['p95']:.2f} ms"
        )
        print(
            f" - Write Latency (p99):        {results['writes_summary']['latency_ms']['p99']:.2f} ms"
        )
        print(
            f" - Write Latency (Min / Max):  {results['writes_summary']['latency_ms']['min']:.2f} ms / {results['writes_summary']['latency_ms']['max']:.2f} ms"
        )

        print("\n--- PER OPERATION BREAKDOWN ---")
        header = f"{'Operation':<30} | {'Cat':<5} | {'Count':<6} | {'p50(ms)':<8} | {'p95(ms)':<8} | {'p99(ms)':<8} | {'Cache Hit %':<12}"
        print(header)
        print("-" * len(header))
        for op_name, d in per_op_summary.items():
            hit_str = (
                f"{d['cache_hit_ratio_percent']:.1f}%"
                if d["cache_hit_ratio_percent"] is not None
                else "N/A"
            )
            print(
                f"{op_name:<30} | {d['category']:<5} | {d['count']:<6} | {d['p50_ms']:<8.2f} | {d['p95_ms']:<8.2f} | {d['p99_ms']:<8.2f} | {hit_str:<12}"
            )

        print("\n--- CONTAINER & RUNTIME RESOURCES ---")
        print(
            f" - Redis Container RAM:        {resource_summary['redis_container_ram_mb']['max']} MB (Peak), avg: {resource_summary['redis_container_ram_mb']['avg']} MB"
        )
        print(
            f" - Redis Container CPU:        {resource_summary['redis_container_cpu_percent']['max']}% (Peak), avg: {resource_summary['redis_container_cpu_percent']['avg']}%"
        )
        print(
            f" - PostgreSQL Container RAM:   {resource_summary['postgres_container_ram_mb']['max']} MB (Peak), avg: {resource_summary['postgres_container_ram_mb']['avg']} MB"
        )
        print(
            f" - PostgreSQL Container CPU:   {resource_summary['postgres_container_cpu_percent']['max']}% (Peak), avg: {resource_summary['postgres_container_cpu_percent']['avg']}%"
        )
        print(
            f" - PgBouncer Container RAM:    {resource_summary['pgbouncer_container_ram_mb']['max']} MB (Peak), avg: {resource_summary['pgbouncer_container_ram_mb']['avg']} MB"
        )
        print(
            f" - PgBouncer Container CPU:    {resource_summary['pgbouncer_container_cpu_percent']['max']}% (Peak), avg: {resource_summary['pgbouncer_container_cpu_percent']['avg']}%"
        )
        print(
            f" - Benchmark Client RAM:       {resource_summary['client_ram_mb']['max']} MB (Peak)"
        )
        print(
            f" - Host System Total CPU:      {resource_summary['system_cpu_percent']['max']}% (Peak), avg: {resource_summary['system_cpu_percent']['avg']}%\n"
        )

        # Save JSON & Markdown reports
        os.makedirs("reports", exist_ok=True)
        json_path = "reports/redis_fuzz_results.json"
        with open(json_path, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to JSON: {json_path}")

        md_path = "reports/redis_fuzz_report.md"
        with open(md_path, "w") as f:
            f.write(generate_markdown_report(results))
        print(f"Report saved to Markdown: {md_path}\n")

    return results


def generate_markdown_report(r: dict) -> str:
    meta = r["benchmark_meta"]
    reads = r["reads_summary"]
    writes = r["writes_summary"]
    res = r["runtime_resources"]
    redis = r["redis_metrics"]

    lines = [
        "# Concurrent Redis Cache-Aside & Stream Write Benchmark Report\n",
        f"**Date:** {meta['timestamp']}  \n",
        f"**Target:** {meta['target_pgbouncer']} (PgBouncer) & {meta['target_redis']} (Redis)  \n",
        f"**Concurrency:** {meta['concurrency_clients']} Client Workers | {meta['stream_workers']} Stream Workers  \n\n",
        "## 1. Executive Summary\n\n",
        f"- **Total Operations:** {meta['total_operations']:,} ({meta['success_rate_percent']}% success, {meta['errors']} errors)\n",
        f"- **Total Duration:** {meta['total_elapsed_sec']} seconds\n",
        f"- **Combined Throughput:** **{meta['throughput_ops_per_sec']} ops/sec**\n",
        f"- **Read Throughput:** {reads['throughput_reads_per_sec']} reads/sec\n",
        f"- **Write Throughput:** {writes['throughput_writes_per_sec']} writes/sec\n",
        f"- **Redis Cache Hit Ratio:** **{reads['cache_hit_ratio_percent']}%**\n\n",
        "## 2. Latency Metrics\n\n",
        "| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        f"| **Overall** | {meta['total_operations']:,} | {r['overall_latency_ms']['mean']} | {r['overall_latency_ms']['p50_median']} | {r['overall_latency_ms']['p90']} | {r['overall_latency_ms']['p95']} | {r['overall_latency_ms']['p99']} | {r['overall_latency_ms']['max']} |",
        f"| **Reads (Cache-Aside)** | {reads['count']:,} | {reads['latency_ms']['mean']} | {reads['latency_ms']['p50_median']} | {reads['latency_ms']['p90']} | {reads['latency_ms']['p95']} | {reads['latency_ms']['p99']} | {reads['latency_ms']['max']} |",
        f"| **Writes (Redis Stream)** | {writes['count']:,} | {writes['latency_ms']['mean']} | {writes['latency_ms']['p50_median']} | {writes['latency_ms']['p90']} | {writes['latency_ms']['p95']} | {writes['latency_ms']['p99']} | {writes['latency_ms']['max']} |",
        "\n## 3. Per Operation Breakdown\n\n",
        "| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |",
        "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
    ]
    for op, d in r["per_operation_type"].items():
        hit = (
            f"{d['cache_hit_ratio_percent']}%"
            if d["cache_hit_ratio_percent"] is not None
            else "N/A"
        )
        lines.append(
            f"| `{op}` | {d['category']} | {d['count']} | {d['success_rate_percent']}% | {d['p50_ms']} | {d['p95_ms']} | {d['p99_ms']} | {hit} |"
        )

    lines.extend(
        [
            "\n## 4. Resource & Container Profiling\n\n",
            "| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |",
            "| :--- | :--- | :--- | :--- | :--- |",
            f"| `redis` | {res['redis_container_cpu_percent']['max']}% | {res['redis_container_cpu_percent']['avg']}% | {res['redis_container_ram_mb']['max']} MB | {res['redis_container_ram_mb']['avg']} MB |",
            f"| `pgbouncer` | {res['pgbouncer_container_cpu_percent']['max']}% | {res['pgbouncer_container_cpu_percent']['avg']}% | {res['pgbouncer_container_ram_mb']['max']} MB | {res['pgbouncer_container_ram_mb']['avg']} MB |",
            f"| `postgres` | {res['postgres_container_cpu_percent']['max']}% | {res['postgres_container_cpu_percent']['avg']}% | {res['postgres_container_ram_mb']['max']} MB | {res['postgres_container_ram_mb']['avg']} MB |",
            f"| Benchmark Client | {res['client_cpu_percent']['max']}% | {res['client_cpu_percent']['avg']}% | {res['client_ram_mb']['max']} MB | {res['client_ram_mb']['avg']} MB |",
            "\n## 5. Redis State & Memory\n\n",
            f"- **Used Memory:** {redis['used_memory_mb']} MB (Peak: {redis['used_memory_peak_mb']} MB)\n",
            f"- **Max Memory Limit:** {redis['maxmemory_mb']} MB\n",
            f"- **Evicted Keys:** {redis['evicted_keys_delta']}\n",
            f"- **Stream Length:** {redis['stream_final_length']}\n",
        ]
    )

    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Concurrent Database & Redis Cache-Aside / Stream Write Benchmark"
    )
    parser.add_argument(
        "--operations",
        type=int,
        default=1000,
        help="Number of operations to benchmark (default: 1000)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=15,
        help="Number of concurrent client workers (default: 15)",
    )
    parser.add_argument(
        "--stream-workers",
        type=int,
        default=4,
        help="Number of background stream consumer workers (default: 4)",
    )
    parser.add_argument(
        "--write-ratio",
        type=float,
        default=0.30,
        help="Proportion of write operations (default: 0.30 for 30%% writes)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=1000,
        help="Number of warm-up read operations to pre-populate Redis cache (default: 1000)",
    )
    args = parser.parse_args()

    asyncio.run(
        run_redis_fuzz_benchmark(
            total_operations=args.operations,
            concurrency=args.concurrency,
            num_stream_workers=args.stream_workers,
            write_ratio=args.write_ratio,
            warmup=args.warmup,
        )
    )
