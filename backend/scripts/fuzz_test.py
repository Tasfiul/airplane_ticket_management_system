"""Database Fuzzing & Resource Profiling Benchmark.

Executes at least 1,000 randomized, complex SQL queries across the 5M+ row schema
through PgBouncer (transaction pooling) and PostgreSQL.
Monitors runtime CPU, RAM, Latency, and Throughput.
"""

import argparse
import asyncio
import datetime
import json
import math
import os
import random
import statistics
import subprocess
import threading
import time
from dataclasses import dataclass
import asyncpg
import psutil
from app.core.config import settings

AIRPORTS = ["JFK", "LAX", "ORD", "DFW", "DEN", "SFO", "SEA", "ATL", "MIA", "BOS"]
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


@dataclass
class QueryRecord:
    query_type: str
    latency_ms: float
    success: bool
    rows_returned: int
    error: str | None = None


def generate_random_query(query_idx: int) -> tuple[str, str, list]:
    """Generates a randomized complex query and returns (query_type, sql, params)."""
    q_type_selector = query_idx % 8

    if q_type_selector == 0:
        # 1. Route search with plane join and available seats aggregation
        dep = random.choice(AIRPORTS)
        arr = random.choice([a for a in AIRPORTS if a != dep])
        days_ahead = random.randint(0, 45)
        start_date = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=days_ahead)
        end_date = start_date + datetime.timedelta(days=random.randint(5, 20))
        sql = """
            SELECT 
                f.flight_id, f.flight_number, f.departure_airport, f.arrival_airport, f.departure_time,
                p.model,
                COUNT(fs.flight_seat_id) AS available_seats
            FROM flight f
            JOIN plane p ON f.plane_id = p.plane_id
            JOIN flight_seat fs ON f.flight_id = fs.flight_id AND fs.status = 'AVAILABLE'
            WHERE f.departure_airport = $1 
              AND f.arrival_airport = $2
              AND f.departure_time >= $3
              AND f.departure_time <= $4
            GROUP BY f.flight_id, f.flight_number, f.departure_airport, f.arrival_airport, f.departure_time, p.model
            ORDER BY f.departure_time
            LIMIT 10;
        """
        return ("Q1_ROUTE_SEARCH", sql, [dep, arr, start_date, end_date])

    elif q_type_selector == 1:
        # 2. Flight seat map & cabin class breakdown (hits 5M flight_seat)
        flight_id = random.randint(1, 25000)
        sql = """
            SELECT 
                s.seat_class,
                fs.status,
                COUNT(*) AS count
            FROM flight_seat fs
            JOIN seat s ON fs.seat_id = s.seat_id
            WHERE fs.flight_id = $1
            GROUP BY s.seat_class, fs.status
            ORDER BY s.seat_class;
        """
        return ("Q2_SEAT_CLASS_AGGREGATION", sql, [flight_id])

    elif q_type_selector == 2:
        # 3. Multi-Flight Class Inventory Query
        flight_ids = [random.randint(1, 25000) for _ in range(5)]
        seat_class = random.choice(SEAT_CLASSES)
        sql = """
            SELECT 
                fs.flight_id,
                s.seat_number,
                s.seat_class,
                fs.status
            FROM flight_seat fs
            JOIN seat s ON fs.seat_id = s.seat_id
            WHERE fs.flight_id = ANY($1::bigint[])
              AND s.seat_class = $2
            ORDER BY fs.flight_id, s.seat_number
            LIMIT 50;
        """
        return ("Q3_MULTI_FLIGHT_CLASS_INVENTORY", sql, [flight_ids, seat_class])

    elif q_type_selector == 3:
        # 4. Window functions: Route headway & ranking
        dep = random.choice(AIRPORTS)
        start_date = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=random.randint(0, 30))
        sql = """
            WITH ranked AS (
                SELECT 
                    flight_id, flight_number, departure_airport, arrival_airport, departure_time,
                    DENSE_RANK() OVER (PARTITION BY departure_airport, arrival_airport ORDER BY departure_time) as schedule_rank,
                    LEAD(departure_time) OVER (PARTITION BY departure_airport, arrival_airport ORDER BY departure_time) - departure_time AS headway
                FROM flight
                WHERE departure_airport = $1
                  AND departure_time >= $2
            )
            SELECT * FROM ranked WHERE schedule_rank <= 3 LIMIT 15;
        """
        return ("Q4_WINDOW_ROUTE_HEADWAY", sql, [dep, start_date])

    elif q_type_selector == 4:
        # 5. Hub capacity aggregation across models with HAVING
        hubs = random.sample(AIRPORTS, 3)
        start_date = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=random.randint(0, 15))
        sql = """
            SELECT 
                p.model,
                f.departure_airport,
                COUNT(DISTINCT f.flight_id) AS total_flights,
                SUM(p.total_seats) AS total_offered_seats
            FROM flight f
            JOIN plane p ON f.plane_id = p.plane_id
            WHERE f.departure_airport = ANY($1::bpchar[])
              AND f.departure_time >= $2
            GROUP BY p.model, f.departure_airport
            HAVING COUNT(DISTINCT f.flight_id) > 2
            ORDER BY total_offered_seats DESC;
        """
        return ("Q5_HUB_CAPACITY_AGGREGATION", sql, [hubs, start_date])

    elif q_type_selector == 5:
        # 6. Customer Profile & Demographic Analysis
        city, division = random.choice(CITIES)
        sql = """
            SELECT 
                c.customer_id, c.first_name, c.last_name, c.email, c.city, c.division,
                EXTRACT(YEAR FROM AGE(c.date_of_birth))::int AS age,
                COUNT(b.booking_id) AS booking_count
            FROM customer c
            LEFT JOIN booking b ON c.customer_id = b.customer_id
            WHERE c.city = $1 AND c.division = $2
            GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.city, c.division, c.date_of_birth
            ORDER BY c.customer_id
            LIMIT 10;
        """
        return ("Q6_CUSTOMER_PROFILE_SEARCH", sql, [city, division])

    elif q_type_selector == 6:
        # 7. Hub traffic flow analytical CTE
        start_date = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=random.randint(0, 10))
        end_date = start_date + datetime.timedelta(days=random.randint(15, 30))
        sql = """
            WITH dep_stats AS (
                SELECT departure_airport AS airport, COUNT(*) AS departures
                FROM flight
                WHERE departure_time >= $1 AND departure_time <= $2
                GROUP BY departure_airport
            ),
            arr_stats AS (
                SELECT arrival_airport AS airport, COUNT(*) AS arrivals
                FROM flight
                WHERE arrival_time >= $1 AND arrival_time <= $2
                GROUP BY arrival_airport
            )
            SELECT 
                COALESCE(d.airport, a.airport) AS hub,
                COALESCE(d.departures, 0) AS dep_count,
                COALESCE(a.arrivals, 0) AS arr_count,
                (COALESCE(d.departures, 0) - COALESCE(a.arrivals, 0)) AS net_flight_flow
            FROM dep_stats d
            FULL OUTER JOIN arr_stats a ON d.airport = a.airport
            ORDER BY net_flight_flow DESC;
        """
        return ("Q7_HUB_TRAFFIC_BALANCE_CTE", sql, [start_date, end_date])

    else:
        # 8. Exact Flight Detail with Cabin Inventory Aggregation
        flight_num = f"FL{random.randint(1, 25000):05d}"
        sql = """
            SELECT 
                f.flight_id,
                f.flight_number,
                f.status AS flight_status,
                f.departure_airport,
                f.arrival_airport,
                f.departure_time,
                f.arrival_time,
                (f.arrival_time - f.departure_time) AS duration,
                p.tail_number,
                p.model,
                p.total_seats,
                COUNT(CASE WHEN fs.status = 'AVAILABLE' THEN 1 END) AS available_seats,
                COUNT(CASE WHEN fs.status = 'BOOKED' THEN 1 END) AS booked_seats
            FROM flight f
            JOIN plane p ON f.plane_id = p.plane_id
            JOIN flight_seat fs ON f.flight_id = fs.flight_id
            WHERE f.flight_number = $1
            GROUP BY f.flight_id, f.flight_number, f.status, f.departure_airport, f.arrival_airport, f.departure_time, f.arrival_time, p.tail_number, p.model, p.total_seats;
        """
        return ("Q8_FLIGHT_DETAIL_INVENTORY", sql, [flight_num])


class ResourceMonitor:
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

    def _docker_stream_worker(self):
        # Baseline snapshot
        self._record_single_snapshot()
        try:
            self._docker_proc = subprocess.Popen(
                ["docker", "stats", "--format", "{{.Name}}:{{.CPUPerc}}:{{.MemUsage}}"],
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
                        mem_mb = float(mem_str.replace("GiB", "").strip()) * 1024
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
        except Exception:
            pass

    def _record_single_snapshot(self):
        try:
            out = subprocess.check_output(
                ["docker", "stats", "--no-stream", "--format", "{{.Name}}:{{.CPUPerc}}:{{.MemUsage}}"],
                stderr=subprocess.DEVNULL,
                timeout=2.0,
            ).decode().strip()
            for line in out.split("\n"):
                if not line:
                    continue
                parts = line.split(":")
                if len(parts) >= 3:
                    name = parts[0].strip()
                    cpu = float(parts[1].replace("%", "").strip()) if parts[1].replace("%", "").strip() else 0.0
                    mem_part = parts[2].split("/")[0].strip()
                    mem_mb = 0.0
                    if "GiB" in mem_part:
                        mem_mb = float(mem_part.replace("GiB", "").strip()) * 1024
                    elif "MiB" in mem_part:
                        mem_mb = float(mem_part.replace("MiB", "").strip())
                    elif "kB" in mem_part:
                        mem_mb = float(mem_part.replace("kB", "").strip()) / 1024

                    if name == "postgres":
                        self.postgres_cpu_samples.append(cpu)
                        self.postgres_ram_mb_samples.append(mem_mb)
                    elif name == "pgbouncer":
                        self.pgbouncer_cpu_samples.append(cpu)
                        self.pgbouncer_ram_mb_samples.append(mem_mb)
        except Exception:
            pass

    async def _async_monitor_loop(self):
        while self._running:
            try:
                self.client_cpu_samples.append(self.process.cpu_percent())
                self.client_ram_mb_samples.append(self.process.memory_info().rss / (1024 * 1024))
                self.system_cpu_samples.append(psutil.cpu_percent())
                self.system_ram_pct_samples.append(psutil.virtual_memory().percent)
            except Exception:
                pass
            await asyncio.sleep(self.interval)

    def start(self):
        self._running = True
        self._docker_thread = threading.Thread(target=self._docker_stream_worker, daemon=True)
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
            "postgres_container_cpu_percent": safe_stats(self.postgres_cpu_samples),
            "postgres_container_ram_mb": safe_stats(self.postgres_ram_mb_samples),
            "pgbouncer_container_cpu_percent": safe_stats(self.pgbouncer_cpu_samples),
            "pgbouncer_container_ram_mb": safe_stats(self.pgbouncer_ram_mb_samples),
        }


async def get_db_stats(host: str, port: int) -> dict:
    """Fetch pg_stat_database metrics for buffer cache and I/O analysis."""
    conn = await asyncpg.connect(
        host=host,
        port=port,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        database=settings.POSTGRES_DB,
    )
    try:
        row = await conn.fetchrow("""
            SELECT 
                blks_read, 
                blks_hit,
                xact_commit, 
                tup_returned, 
                tup_fetched
            FROM pg_stat_database 
            WHERE datname = $1;
        """, settings.POSTGRES_DB)
        return dict(row) if row else {}
    finally:
        await conn.close()


async def run_fuzz_benchmark(total_queries: int = 1000, concurrency: int = 15, verbose: bool = True):
    target_host = settings.PGBOUNCER_HOST if settings.USE_PGBOUNCER else settings.POSTGRES_HOST
    target_port = settings.PGBOUNCER_PORT if settings.USE_PGBOUNCER else settings.POSTGRES_PORT

    if verbose:
        print("\n" + "=" * 70)
        print("DATABASE FUZZING & RESOURCE PROFILING BENCHMARK")
        print(f"Target: {target_host}:{target_port} (Via {'PgBouncer' if settings.USE_PGBOUNCER else 'PostgreSQL'})")
        print(f"Total Queries: {total_queries:,} | Concurrency: {concurrency} async workers")
        print("=" * 70)

    # Initial DB stats
    initial_db_stats = await get_db_stats(settings.POSTGRES_HOST, settings.POSTGRES_PORT)

    # Start resource monitor
    monitor = ResourceMonitor(sample_interval=0.2)
    monitor.start()

    records: list[QueryRecord] = []
    queue = asyncio.Queue()
    for i in range(total_queries):
        queue.put_nowait((i, *generate_random_query(i)))

    bench_start_time = time.perf_counter()

    async def worker(worker_id: int):
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
                    q_idx, q_type, sql, params = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                t0 = time.perf_counter()
                success = True
                err_msg = None
                rows_count = 0
                try:
                    rows = await conn.fetch(sql, *params)
                    rows_count = len(rows)
                except Exception as e:
                    success = False
                    err_msg = str(e)
                finally:
                    latency = (time.perf_counter() - t0) * 1000.0  # ms
                    records.append(QueryRecord(
                        query_type=q_type,
                        latency_ms=latency,
                        success=success,
                        rows_returned=rows_count,
                        error=err_msg,
                    ))
                    queue.task_done()
        finally:
            await conn.close()

    # Launch worker tasks
    workers = [asyncio.create_task(worker(w)) for w in range(concurrency)]
    await asyncio.gather(*workers)

    bench_elapsed = time.perf_counter() - bench_start_time
    await monitor.stop()

    # Final DB stats
    final_db_stats = await get_db_stats(settings.POSTGRES_HOST, settings.POSTGRES_PORT)

    # Calculations
    latencies = [r.latency_ms for r in records]
    successes = sum(1 for r in records if r.success)
    failures = len(records) - successes
    qps = len(records) / bench_elapsed if bench_elapsed > 0 else 0

    sorted_latencies = sorted(latencies)
    def percentile(p):
        if not sorted_latencies:
            return 0.0
        k = (len(sorted_latencies) - 1) * (p / 100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_latencies[int(k)]
        return sorted_latencies[f] * (c - k) + sorted_latencies[c] * (k - f)

    p50 = percentile(50)
    p90 = percentile(90)
    p95 = percentile(95)
    p99 = percentile(99)
    min_lat = min(latencies) if latencies else 0.0
    max_lat = max(latencies) if latencies else 0.0
    mean_lat = statistics.mean(latencies) if latencies else 0.0

    # Per Query Type breakdown
    type_stats = {}
    for r in records:
        if r.query_type not in type_stats:
            type_stats[r.query_type] = []
        type_stats[r.query_type].append(r.latency_ms)

    query_type_summary = {}
    for q_type, lats in sorted(type_stats.items()):
        query_type_summary[q_type] = {
            "count": len(lats),
            "mean_ms": round(statistics.mean(lats), 2),
            "p95_ms": round(sorted(lats)[int(len(lats) * 0.95)], 2),
            "min_ms": round(min(lats), 2),
            "max_ms": round(max(lats), 2),
        }

    # Buffer cache delta
    blks_read_delta = (final_db_stats.get("blks_read", 0) - initial_db_stats.get("blks_read", 0))
    blks_hit_delta = (final_db_stats.get("blks_hit", 0) - initial_db_stats.get("blks_hit", 0))
    total_blks = blks_read_delta + blks_hit_delta
    cache_hit_ratio = (blks_hit_delta / total_blks * 100.0) if total_blks > 0 else 100.0

    resource_summary = monitor.get_summary()

    benchmark_results = {
        "benchmark_meta": {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "target": f"{target_host}:{target_port}",
            "pooler": "PgBouncer" if settings.USE_PGBOUNCER else "PostgreSQL Direct",
            "concurrency": concurrency,
            "total_queries": len(records),
            "total_elapsed_sec": round(bench_elapsed, 2),
            "throughput_qps": round(qps, 2),
            "success_rate_percent": round((successes / len(records)) * 100, 2),
            "errors": failures,
        },
        "latency_metrics_ms": {
            "min": round(min_lat, 2),
            "mean": round(mean_lat, 2),
            "p50_median": round(p50, 2),
            "p90": round(p90, 2),
            "p95": round(p95, 2),
            "p99": round(p99, 2),
            "max": round(max_lat, 2),
        },
        "per_query_type": query_type_summary,
        "database_io_metrics": {
            "blocks_read": blks_read_delta,
            "blocks_hit": blks_hit_delta,
            "cache_hit_ratio_percent": round(cache_hit_ratio, 2),
        },
        "runtime_resources": resource_summary,
    }

    # Print summary to stdout
    if verbose:
        print(f"\nExecution Summary:")
        print(f" - Total Queries Completed: {len(records):,} / {total_queries:,} ({successes} ok, {failures} err)")
        print(f" - Wall Clock Time:        {bench_elapsed:.2f} seconds")
        print(f" - Throughput:              {qps:.2f} queries/sec (QPS)")
        print(f" - Latency p50 (median):    {p50:.2f} ms")
        print(f" - Latency p95:             {p95:.2f} ms")
        print(f" - Latency p99:             {p99:.2f} ms")
        print(f" - Latency Max:             {max_lat:.2f} ms")
        print(f" - Buffer Cache Hit Ratio:  {cache_hit_ratio:.2f}%")
        print(f"\nContainer & Resource Summary:")
        print(f" - PostgreSQL Container RAM: {resource_summary['postgres_container_ram_mb']['max']} MB (Peak), avg: {resource_summary['postgres_container_ram_mb']['avg']} MB")
        print(f" - PostgreSQL Container CPU: {resource_summary['postgres_container_cpu_percent']['max']}% (Peak), avg: {resource_summary['postgres_container_cpu_percent']['avg']}%")
        print(f" - PgBouncer Container RAM:  {resource_summary['pgbouncer_container_ram_mb']['max']} MB (Peak), avg: {resource_summary['pgbouncer_container_ram_mb']['avg']} MB")
        print(f" - PgBouncer Container CPU:  {resource_summary['pgbouncer_container_cpu_percent']['max']}% (Peak), avg: {resource_summary['pgbouncer_container_cpu_percent']['avg']}%")
        print(f" - Benchmark Client RAM:     {resource_summary['client_ram_mb']['max']} MB (Peak)")
        print(f" - Host System Total CPU:    {resource_summary['system_cpu_percent']['max']}% (Peak), avg: {resource_summary['system_cpu_percent']['avg']}%")

        # Save JSON Report
        os.makedirs("reports", exist_ok=True)
        report_file = "reports/fuzz_results.json"
        with open(report_file, "w") as f:
            json.dump(benchmark_results, f, indent=2)
        print(f"\nFull results saved to {report_file}")

    return benchmark_results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database Fuzzing & Resource Profiling")
    parser.add_argument("--queries", type=int, default=1000, help="Number of queries to fuzz (default: 1000)")
    parser.add_argument("--concurrency", type=int, default=15, help="Number of concurrent workers (default: 15)")
    args = parser.parse_args()

    asyncio.run(run_fuzz_benchmark(total_queries=args.queries, concurrency=args.concurrency))
