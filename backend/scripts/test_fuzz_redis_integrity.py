"""Database Integrity & Redis Fuzzing Test Suite.

Executes the concurrent Redis Cache-Aside & Stream Write benchmark (fuzz_test_redis.py)
and runs a comprehensive database integrity audit across PostgreSQL and Redis.

Verifies:
1. Referential Integrity & Foreign Key consistency (no orphaned bookings, tickets, seats, flights).
2. Concurrency & Double-Booking Invariants (partial unique constraint, active ticket vs seat status).
3. Booking & Financial Consistency (total_amount matches sum of tickets, lifecycle states).
4. Domain Constraints (flight times, airports, enum ranges, plane-seat mappings).
5. Uniqueness Constraints (customer emails, booking codes, flight seat mappings).
6. Cross-System Consistency (Redis Stream queue drain, live Redis seat map sync with DB).
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict, dataclass, field
import datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import sys
import time
from typing import Any

# Ensure backend and src are in sys.path
_BACKEND_DIR = Path(__file__).resolve().parent.parent
_SRC_DIR = _BACKEND_DIR / "src"
for _path in [str(_BACKEND_DIR), str(_SRC_DIR)]:
    if _path not in sys.path:
        sys.path.insert(0, _path)

import asyncpg
import redis.asyncio as aioredis
from app.core.config import settings
from app.core.redis import close_redis_pool
from scripts.fuzz_test_redis import run_redis_fuzz_benchmark
from scripts.reset_test_state import reset_state


@dataclass
class IntegrityCheckResult:
    check_id: str
    category: str
    title: str
    description: str
    passed: bool
    violations_count: int
    execution_time_ms: float
    sample_violations: list[dict[str, Any]] = field(default_factory=list)
    details: str | None = None


class DatabaseIntegrityChecker:
    """Executes a comprehensive suite of integrity checks across PostgreSQL and Redis."""

    def __init__(
        self,
        target_host: str | None = None,
        target_port: int | None = None,
        redis_url: str | None = None,
    ):
        self.host = target_host or (
            settings.PGBOUNCER_HOST
            if settings.USE_PGBOUNCER
            else settings.POSTGRES_HOST
        )
        self.port = target_port or (
            settings.PGBOUNCER_PORT
            if settings.USE_PGBOUNCER
            else settings.POSTGRES_PORT
        )
        self.redis_url = redis_url or settings.redis_url

    async def _get_db_connection(self) -> asyncpg.Connection:
        """Connect with statement_cache_size=0 to support PgBouncer transaction pooling mode."""
        return await asyncpg.connect(
            host=self.host,
            port=self.port,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
            database=settings.POSTGRES_DB,
            statement_cache_size=0,
        )

    async def run_all_checks(self) -> list[IntegrityCheckResult]:
        """Runs all database integrity and cross-system consistency checks."""
        conn = await self._get_db_connection()
        redis_client = aioredis.from_url(self.redis_url)
        results: list[IntegrityCheckResult] = []

        try:
            # ---------------------------------------------------------
            # 1. Referential Integrity & Foreign Key Constraints
            # ---------------------------------------------------------
            fk_checks = [
                (
                    "fk_orphan_bookings",
                    "Referential Integrity",
                    "Orphaned Bookings (No Customer)",
                    "Checks if any booking references a non-existent customer_id",
                    "SELECT b.booking_id, b.customer_id FROM booking b LEFT JOIN customer c ON b.customer_id = c.customer_id WHERE c.customer_id IS NULL",
                ),
                (
                    "fk_orphan_tickets_booking",
                    "Referential Integrity",
                    "Orphaned Tickets (No Booking)",
                    "Checks if any ticket references a non-existent booking_id",
                    "SELECT t.ticket_id, t.booking_id FROM ticket t LEFT JOIN booking b ON t.booking_id = b.booking_id WHERE b.booking_id IS NULL",
                ),
                (
                    "fk_orphan_tickets_seat",
                    "Referential Integrity",
                    "Orphaned Tickets (No Flight Seat)",
                    "Checks if any ticket references a non-existent flight_seat_id",
                    "SELECT t.ticket_id, t.flight_seat_id FROM ticket t LEFT JOIN flight_seat fs ON t.flight_seat_id = fs.flight_seat_id WHERE fs.flight_seat_id IS NULL",
                ),
                (
                    "fk_orphan_flight_seats_flight",
                    "Referential Integrity",
                    "Orphaned Flight Seats (No Flight)",
                    "Checks if any flight_seat references a non-existent flight_id",
                    "SELECT fs.flight_seat_id, fs.flight_id FROM flight_seat fs LEFT JOIN flight f ON fs.flight_id = f.flight_id WHERE f.flight_id IS NULL",
                ),
                (
                    "fk_orphan_flight_seats_seat",
                    "Referential Integrity",
                    "Orphaned Flight Seats (No Physical Seat)",
                    "Checks if any flight_seat references a non-existent physical seat_id",
                    "SELECT fs.flight_seat_id, fs.seat_id FROM flight_seat fs LEFT JOIN seat s ON fs.seat_id = s.seat_id WHERE s.seat_id IS NULL",
                ),
                (
                    "fk_orphan_flights_plane",
                    "Referential Integrity",
                    "Orphaned Flights (No Plane)",
                    "Checks if any flight references a non-existent plane_id",
                    "SELECT f.flight_id, f.plane_id FROM flight f LEFT JOIN plane p ON f.plane_id = p.plane_id WHERE p.plane_id IS NULL",
                ),
                (
                    "fk_orphan_seats_plane",
                    "Referential Integrity",
                    "Orphaned Physical Seats (No Plane)",
                    "Checks if any physical seat references a non-existent plane_id",
                    "SELECT s.seat_id, s.plane_id FROM seat s LEFT JOIN plane p ON s.plane_id = p.plane_id WHERE p.plane_id IS NULL",
                ),
            ]

            for cid, cat, title, desc, sample_sql in fk_checks:
                t0 = time.perf_counter()
                count_sql = f"SELECT COUNT(*) FROM ({sample_sql}) AS sub_chk"
                count = await conn.fetchval(count_sql)
                samples = []
                if count > 0:
                    rows = await conn.fetch(f"{sample_sql} LIMIT 5")
                    samples = [dict(r) for r in rows]
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                results.append(
                    IntegrityCheckResult(
                        check_id=cid,
                        category=cat,
                        title=title,
                        description=desc,
                        passed=(count == 0),
                        violations_count=count,
                        execution_time_ms=round(elapsed_ms, 2),
                        sample_violations=samples,
                    )
                )

            # ---------------------------------------------------------
            # 2. Concurrency & Double-Booking Invariants
            # ---------------------------------------------------------
            concurrency_checks = [
                (
                    "chk_no_double_bookings",
                    "Concurrency Invariants",
                    "Double-Booking Prevention",
                    "Verifies no flight_seat has more than one ACTIVE ticket (uq_active_ticket_per_seat partial index)",
                    "SELECT flight_seat_id, COUNT(*) AS active_tickets FROM ticket WHERE status = 'ACTIVE' GROUP BY flight_seat_id HAVING COUNT(*) > 1",
                ),
                (
                    "chk_active_tickets_booked_status",
                    "Concurrency Invariants",
                    "Active Ticket -> BOOKED Seat Status",
                    "Verifies every flight_seat with an ACTIVE ticket is marked BOOKED in flight_seat table",
                    "SELECT t.ticket_id, t.flight_seat_id, fs.status AS seat_status FROM ticket t JOIN flight_seat fs ON t.flight_seat_id = fs.flight_seat_id WHERE t.status = 'ACTIVE' AND fs.status <> 'BOOKED'",
                ),
                (
                    "chk_booked_seats_have_active_ticket",
                    "Concurrency Invariants",
                    "BOOKED Seat -> Active Ticket Exists",
                    "Verifies every flight_seat marked BOOKED has a corresponding ACTIVE ticket",
                    "SELECT fs.flight_seat_id, fs.flight_id, fs.status FROM flight_seat fs WHERE fs.status = 'BOOKED' AND NOT EXISTS (SELECT 1 FROM ticket t WHERE t.flight_seat_id = fs.flight_seat_id AND t.status = 'ACTIVE')",
                ),
                (
                    "chk_available_seats_no_active_ticket",
                    "Concurrency Invariants",
                    "AVAILABLE Seat -> No Active Ticket",
                    "Verifies no flight_seat marked AVAILABLE has an ACTIVE ticket",
                    "SELECT fs.flight_seat_id, t.ticket_id, t.status AS ticket_status FROM flight_seat fs JOIN ticket t ON fs.flight_seat_id = t.flight_seat_id WHERE fs.status = 'AVAILABLE' AND t.status = 'ACTIVE'",
                ),
            ]

            for cid, cat, title, desc, sample_sql in concurrency_checks:
                t0 = time.perf_counter()
                count_sql = f"SELECT COUNT(*) FROM ({sample_sql}) AS sub_chk"
                count = await conn.fetchval(count_sql)
                samples = []
                if count > 0:
                    rows = await conn.fetch(f"{sample_sql} LIMIT 5")
                    samples = [dict(r) for r in rows]
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                results.append(
                    IntegrityCheckResult(
                        check_id=cid,
                        category=cat,
                        title=title,
                        description=desc,
                        passed=(count == 0),
                        violations_count=count,
                        execution_time_ms=round(elapsed_ms, 2),
                        sample_violations=samples,
                    )
                )

            # ---------------------------------------------------------
            # 3. Financial & Booking Consistency
            # ---------------------------------------------------------
            financial_checks = [
                (
                    "chk_booking_total_equals_tickets_sum",
                    "Financial & Lifecycle",
                    "Booking Total Matches Ticket Sum",
                    "Verifies booking.total_amount matches exactly the SUM(ticket.price) for each booking",
                    "SELECT b.booking_id, b.total_amount, SUM(t.price) AS tickets_sum, ABS(b.total_amount - SUM(t.price)) AS difference FROM booking b JOIN ticket t ON b.booking_id = t.booking_id GROUP BY b.booking_id, b.total_amount HAVING b.total_amount <> SUM(t.price)",
                ),
                (
                    "chk_no_empty_bookings",
                    "Financial & Lifecycle",
                    "Non-Empty Bookings",
                    "Verifies no booking exists with zero associated tickets",
                    "SELECT b.booking_id, b.booking_code, b.status FROM booking b LEFT JOIN ticket t ON b.booking_id = t.booking_id WHERE t.ticket_id IS NULL",
                ),
                (
                    "chk_cancelled_bookings_tickets_cancelled",
                    "Financial & Lifecycle",
                    "Cancelled Booking Ticket Consistency",
                    "Verifies all tickets of a CANCELLED booking are marked CANCELLED or REFUNDED",
                    "SELECT b.booking_id, t.ticket_id, b.status AS booking_status, t.status AS ticket_status FROM booking b JOIN ticket t ON b.booking_id = t.booking_id WHERE b.status = 'CANCELLED' AND t.status NOT IN ('CANCELLED', 'REFUNDED')",
                ),
                (
                    "chk_paid_bookings_tickets_active",
                    "Financial & Lifecycle",
                    "Paid Booking Ticket Consistency",
                    "Verifies all tickets of a PAID booking have ACTIVE status",
                    "SELECT b.booking_id, t.ticket_id, b.status AS booking_status, t.status AS ticket_status FROM booking b JOIN ticket t ON b.booking_id = t.booking_id WHERE b.status = 'PAID' AND t.status <> 'ACTIVE'",
                ),
                (
                    "chk_non_negative_booking_totals",
                    "Financial & Lifecycle",
                    "Non-Negative Booking Total Amount",
                    "Verifies booking.total_amount is >= 0 for all records (chk_booking_total_amount constraint)",
                    "SELECT booking_id, total_amount FROM booking WHERE total_amount < 0",
                ),
                (
                    "chk_non_negative_ticket_prices",
                    "Financial & Lifecycle",
                    "Non-Negative Ticket Prices",
                    "Verifies ticket.price is >= 0 for all records (chk_ticket_price constraint)",
                    "SELECT ticket_id, price FROM ticket WHERE price < 0",
                ),
            ]

            for cid, cat, title, desc, sample_sql in financial_checks:
                t0 = time.perf_counter()
                count_sql = f"SELECT COUNT(*) FROM ({sample_sql}) AS sub_chk"
                count = await conn.fetchval(count_sql)
                samples = []
                if count > 0:
                    rows = await conn.fetch(f"{sample_sql} LIMIT 5")
                    # Format Decimal to string for JSON serialization
                    samples = [
                        {
                            k: str(v) if isinstance(v, Decimal) else v
                            for k, v in dict(r).items()
                        }
                        for r in rows
                    ]
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                results.append(
                    IntegrityCheckResult(
                        check_id=cid,
                        category=cat,
                        title=title,
                        description=desc,
                        passed=(count == 0),
                        violations_count=count,
                        execution_time_ms=round(elapsed_ms, 2),
                        sample_violations=samples,
                    )
                )

            # ---------------------------------------------------------
            # 4. Domain & Check Constraints
            # ---------------------------------------------------------
            domain_checks = [
                (
                    "chk_flight_times_valid",
                    "Domain Constraints",
                    "Flight Arrival After Departure",
                    "Verifies arrival_time > departure_time for all flights (chk_flight_times constraint)",
                    "SELECT flight_id, flight_number, departure_time, arrival_time FROM flight WHERE arrival_time <= departure_time",
                ),
                (
                    "chk_flight_airports_distinct",
                    "Domain Constraints",
                    "Distinct Departure & Arrival Airports",
                    "Verifies departure_airport <> arrival_airport for all flights (chk_flight_airports constraint)",
                    "SELECT flight_id, departure_airport, arrival_airport FROM flight WHERE departure_airport = arrival_airport",
                ),
                (
                    "chk_flight_statuses_valid",
                    "Domain Constraints",
                    "Valid Flight Status Enum",
                    "Verifies flight.status is within SCHEDULED, DELAYED, DEPARTED, CANCELLED",
                    "SELECT flight_id, status FROM flight WHERE status NOT IN ('SCHEDULED', 'DELAYED', 'DEPARTED', 'CANCELLED')",
                ),
                (
                    "chk_booking_statuses_valid",
                    "Domain Constraints",
                    "Valid Booking Status Enum",
                    "Verifies booking.status is within PENDING, PAID, CANCELLED, REFUNDED",
                    "SELECT booking_id, status FROM booking WHERE status NOT IN ('PENDING', 'PAID', 'CANCELLED', 'REFUNDED')",
                ),
                (
                    "chk_ticket_statuses_valid",
                    "Domain Constraints",
                    "Valid Ticket Status Enum",
                    "Verifies ticket.status is within ACTIVE, CANCELLED, REFUNDED",
                    "SELECT ticket_id, status FROM ticket WHERE status NOT IN ('ACTIVE', 'CANCELLED', 'REFUNDED')",
                ),
                (
                    "chk_flight_seat_statuses_valid",
                    "Domain Constraints",
                    "Valid Flight Seat Status Enum",
                    "Verifies flight_seat.status is within AVAILABLE, BOOKED",
                    "SELECT flight_seat_id, status FROM flight_seat WHERE status NOT IN ('AVAILABLE', 'BOOKED')",
                ),
                (
                    "chk_flight_seat_plane_alignment",
                    "Domain Constraints",
                    "Flight Seat Plane Alignment",
                    "Verifies each flight_seat references a seat that belongs to the assigned flight's plane",
                    "SELECT fs.flight_seat_id, fs.flight_id, f.plane_id AS flight_plane, fs.seat_id, s.plane_id AS seat_plane FROM flight_seat fs JOIN flight f ON fs.flight_id = f.flight_id JOIN seat s ON fs.seat_id = s.seat_id WHERE s.plane_id <> f.plane_id",
                ),
            ]

            for cid, cat, title, desc, sample_sql in domain_checks:
                t0 = time.perf_counter()
                count_sql = f"SELECT COUNT(*) FROM ({sample_sql}) AS sub_chk"
                count = await conn.fetchval(count_sql)
                samples = []
                if count > 0:
                    rows = await conn.fetch(f"{sample_sql} LIMIT 5")
                    samples = [dict(r) for r in rows]
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                results.append(
                    IntegrityCheckResult(
                        check_id=cid,
                        category=cat,
                        title=title,
                        description=desc,
                        passed=(count == 0),
                        violations_count=count,
                        execution_time_ms=round(elapsed_ms, 2),
                        sample_violations=samples,
                    )
                )

            # ---------------------------------------------------------
            # 5. Uniqueness Constraints
            # ---------------------------------------------------------
            uniqueness_checks = [
                (
                    "chk_customer_email_uniqueness",
                    "Uniqueness Invariants",
                    "Customer Email Uniqueness",
                    "Verifies no duplicate email addresses exist in the customer table",
                    "SELECT email, COUNT(*) AS occurrences FROM customer GROUP BY email HAVING COUNT(*) > 1",
                ),
                (
                    "chk_booking_code_uniqueness",
                    "Uniqueness Invariants",
                    "Booking Code Uniqueness",
                    "Verifies no duplicate booking_code values exist in the booking table",
                    "SELECT booking_code, COUNT(*) AS occurrences FROM booking GROUP BY booking_code HAVING COUNT(*) > 1",
                ),
                (
                    "chk_flight_seat_pair_uniqueness",
                    "Uniqueness Invariants",
                    "Flight Seat Pair Uniqueness",
                    "Verifies no duplicate (flight_id, seat_id) pairs exist (uq_flight_seat unique constraint)",
                    "SELECT flight_id, seat_id, COUNT(*) AS occurrences FROM flight_seat GROUP BY flight_id, seat_id HAVING COUNT(*) > 1",
                ),
            ]

            for cid, cat, title, desc, sample_sql in uniqueness_checks:
                t0 = time.perf_counter()
                count_sql = f"SELECT COUNT(*) FROM ({sample_sql}) AS sub_chk"
                count = await conn.fetchval(count_sql)
                samples = []
                if count > 0:
                    rows = await conn.fetch(f"{sample_sql} LIMIT 5")
                    samples = [dict(r) for r in rows]
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                results.append(
                    IntegrityCheckResult(
                        check_id=cid,
                        category=cat,
                        title=title,
                        description=desc,
                        passed=(count == 0),
                        violations_count=count,
                        execution_time_ms=round(elapsed_ms, 2),
                        sample_violations=samples,
                    )
                )

            # ---------------------------------------------------------
            # 6. Redis Stream & Cache Cross-System Consistency
            # ---------------------------------------------------------
            # 6.1 Redis Stream Queue Drain (PEL)
            t0 = time.perf_counter()
            stream_pending_count = 0
            stream_details = "Stream consumer group verified."
            try:
                pel = await redis_client.xpending(
                    settings.REDIS_STREAM_WRITE_NAME,
                    settings.REDIS_STREAM_CONSUMER_GROUP,
                )
                stream_pending_count = pel.get("pending", 0) if isinstance(pel, dict) else 0
                if stream_pending_count > 0:
                    stream_details = f"Unacknowledged messages remaining in PEL: {stream_pending_count}"
            except Exception as e:
                # If stream does not exist or group not created, check if stream key exists
                stream_exists = await redis_client.exists(settings.REDIS_STREAM_WRITE_NAME)
                if not stream_exists:
                    stream_pending_count = 0
                    stream_details = "Stream does not exist (clean state)."
                else:
                    stream_details = f"Redis xpending check warning: {e}"

            results.append(
                IntegrityCheckResult(
                    check_id="chk_redis_stream_drained",
                    category="Cross-System Consistency",
                    title="Redis Write Stream Fully Drained",
                    description="Verifies the Redis Stream write consumer group has 0 pending unacknowledged events",
                    passed=(stream_pending_count == 0),
                    violations_count=stream_pending_count,
                    execution_time_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                    details=stream_details,
                )
            )

            # 6.2 Redis Live Seat Hash Sync with PostgreSQL
            t0 = time.perf_counter()
            desync_count = 0
            sample_desyncs: list[dict[str, Any]] = []
            cached_flight_keys = await redis_client.keys("flight:*:seats")
            checked_flights = len(cached_flight_keys)

            for key in cached_flight_keys:
                key_str = key.decode("utf-8") if isinstance(key, bytes) else key
                try:
                    flight_id = int(key_str.split(":")[1])
                except (IndexError, ValueError):
                    continue

                redis_seat_map = await redis_client.hgetall(key)
                if not redis_seat_map:
                    continue

                db_rows = await conn.fetch(
                    "SELECT flight_seat_id, status FROM flight_seat WHERE flight_id = $1",
                    flight_id,
                )
                db_map = {
                    str(r["flight_seat_id"]): r["status"] for r in db_rows
                }

                for sid_raw, st_raw in redis_seat_map.items():
                    sid = (
                        sid_raw.decode("utf-8")
                        if isinstance(sid_raw, bytes)
                        else sid_raw
                    )
                    st = (
                        st_raw.decode("utf-8")
                        if isinstance(st_raw, bytes)
                        else st_raw
                    )
                    if sid in db_map:
                        db_st = db_map[sid]
                        if st == "HELD" and db_st == "AVAILABLE":
                            continue
                        if st != db_st:
                            desync_count += 1
                            if len(sample_desyncs) < 5:
                                sample_desyncs.append(
                                    {
                                        "flight_id": flight_id,
                                        "flight_seat_id": int(sid),
                                        "redis_status": st,
                                        "db_status": db_st,
                                    }
                                )

            results.append(
                IntegrityCheckResult(
                    check_id="chk_redis_seat_map_sync",
                    category="Cross-System Consistency",
                    title="Redis Seat Hash vs PostgreSQL Sync",
                    description=f"Verifies live seat status in Redis hashes matches PostgreSQL flight_seat status across {checked_flights} cached flight seat maps",
                    passed=(desync_count == 0),
                    violations_count=desync_count,
                    execution_time_ms=round((time.perf_counter() - t0) * 1000.0, 2),
                    sample_violations=sample_desyncs,
                    details=f"Checked {checked_flights} cached flight maps; {desync_count} seat mismatches detected.",
                )
            )

        finally:
            await conn.close()
            await redis_client.aclose()

        return results


def format_integrity_report_markdown(
    results: list[IntegrityCheckResult],
    benchmark_meta: dict[str, Any] | None = None,
) -> str:
    """Formats the integrity check results into a Github Flavored Markdown report."""
    total_checks = len(results)
    passed_checks = sum(1 for r in results if r.passed)
    failed_checks = total_checks - passed_checks
    total_time_ms = sum(r.execution_time_ms for r in results)

    lines = [
        "# Database & Cross-System Integrity Audit Report\n",
        f"**Audit Date:** {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}  \n",
        f"**Total Checks:** {total_checks} | **Passed:** {passed_checks} | **Failed:** {failed_checks}  \n",
        f"**Audit Time:** {total_time_ms:.2f} ms ({total_time_ms / 1000.0:.2f} s)\n\n",
    ]

    if benchmark_meta:
        lines.extend(
            [
                "## 1. Associated Fuzz Benchmark Summary\n\n",
                f"- **Target Database:** {benchmark_meta.get('target_pgbouncer', 'N/A')}\n",
                f"- **Target Redis:** {benchmark_meta.get('target_redis', 'N/A')}\n",
                f"- **Total Benchmark Operations:** {benchmark_meta.get('total_operations', 0):,}\n",
                f"- **Benchmark Duration:** {benchmark_meta.get('total_elapsed_sec', 0)} seconds\n",
                f"- **Benchmark Throughput:** {benchmark_meta.get('throughput_ops_per_sec', 0)} ops/sec\n",
                f"- **Benchmark Success Rate:** {benchmark_meta.get('success_rate_percent', 0)}%\n\n",
            ]
        )

    lines.extend(
        [
            "## 2. Integrity Verification Matrix\n\n",
            "| ID | Category | Check Name | Status | Violations | Execution (ms) |",
            "| :--- | :--- | :--- | :---: | :---: | :---: |",
        ]
    )

    for r in results:
        status_badge = "PASS" if r.passed else "**FAIL**"
        lines.append(
            f"| `{r.check_id}` | {r.category} | {r.title} | {status_badge} | {r.violations_count} | {r.execution_time_ms:.2f} |"
        )

    if failed_checks > 0:
        lines.append("\n## 3. Detailed Failure Diagnostics\n\n")
        for r in results:
            if not r.passed:
                lines.append(f"### Violation in `{r.check_id}`: {r.title}\n")
                lines.append(f"- **Category:** {r.category}\n")
                lines.append(f"- **Description:** {r.description}\n")
                lines.append(f"- **Violations Count:** {r.violations_count}\n")
                if r.details:
                    lines.append(f"- **Details:** {r.details}\n")
                if r.sample_violations:
                    lines.append("- **Sample Offending Records:**\n")
                    lines.append("```json\n" + json.dumps(r.sample_violations, indent=2, default=str) + "\n```\n")
    else:
        lines.append("\n## 3. Integrity Verdict\n\n")
        lines.append(
            "> [!NOTE]\n"
            "> **Database Integrity Verified 100% Intact.**\n"
            "> All foreign key relationships, partial unique constraints, double-booking prevention guards, "
            "financial ledger aggregates, check constraints, and Redis-to-Postgres state projections match without errors."
        )

    return "\n".join(lines)


def print_cli_summary_table(
    results: list[IntegrityCheckResult],
    benchmark_meta: dict[str, Any] | None = None,
):
    """Outputs a formatted ASCII status table to stdout."""
    total_checks = len(results)
    passed_checks = sum(1 for r in results if r.passed)
    failed_checks = total_checks - passed_checks
    total_time_ms = sum(r.execution_time_ms for r in results)

    print("\n" + "=" * 90)
    print("DATABASE & CROSS-SYSTEM INTEGRITY VERIFICATION AUDIT")
    print("=" * 90)

    header = f"{'Check Title':<42} | {'Category':<22} | {'Status':<8} | {'Violations':<10} | {'Time (ms)':<9}"
    print(header)
    print("-" * len(header))

    for r in results:
        status_str = "[PASS]" if r.passed else "[FAIL]"
        print(
            f"{r.title:<42} | {r.category:<22} | {status_str:<8} | {r.violations_count:<10} | {r.execution_time_ms:<9.2f}"
        )

    print("-" * len(header))
    print(
        f"Summary: {passed_checks}/{total_checks} Checks Passed ({failed_checks} Failed) in {total_time_ms / 1000.0:.2f}s"
    )

    if failed_checks > 0:
        print("\n" + "!" * 90)
        print("INTEGRITY FAILURES DETECTED:")
        print("!" * 90)
        for r in results:
            if not r.passed:
                print(f"\n[FAIL] {r.title} ({r.check_id})")
                print(f"       Category:   {r.category}")
                print(f"       Violations: {r.violations_count}")
                if r.details:
                    print(f"       Details:    {r.details}")
                if r.sample_violations:
                    print("       Sample Violating Records:")
                    for s in r.sample_violations[:3]:
                        print(f"        - {s}")
        print("=" * 90 + "\n")
    else:
        print("\nAll database invariants, constraints, and Redis synchronizations PASSED!\n")


async def run_fuzz_and_verify(
    operations: int = 500,
    concurrency: int = 10,
    stream_workers: int = 4,
    write_ratio: float = 0.30,
    warmup: int = 100,
    reset_before: bool = False,
    reset_after: bool = False,
    skip_fuzz: bool = False,
    verbose: bool = True,
    json_report_path: str = "reports/database_integrity_results.json",
    md_report_path: str = "reports/database_integrity_report.md",
) -> tuple[dict[str, Any] | None, list[IntegrityCheckResult], bool]:
    """Runs fuzz_test_redis.py benchmark and validates database integrity."""
    benchmark_results: dict[str, Any] | None = None

    # 1. Optional test state reset before running
    if reset_before:
        if verbose:
            print("\n[SETUP] Resetting database test state before benchmark run...")
        await reset_state()

    # 2. Execute Fuzzing Benchmark
    if not skip_fuzz:
        if verbose:
            print(
                f"\n[RUN] Launching Redis Fuzz Benchmark: {operations:,} operations | {concurrency} clients | {stream_workers} stream workers..."
            )
        benchmark_results = await run_redis_fuzz_benchmark(
            total_operations=operations,
            concurrency=concurrency,
            num_stream_workers=stream_workers,
            write_ratio=write_ratio,
            warmup=warmup,
            verbose=verbose,
        )
        # Small settling time for any lingering async DB sockets
        await asyncio.sleep(0.5)
    else:
        if verbose:
            print("\n[SKIP] Fuzzing benchmark skipped by request (--skip-fuzz). Running integrity checks on current DB state...")

    # 3. Run Comprehensive Database Integrity Checks
    if verbose:
        print("\n[AUDIT] Commencing Database & Cross-System Integrity Verification...")

    checker = DatabaseIntegrityChecker()
    integrity_results = await checker.run_all_checks()

    all_passed = all(r.passed for r in integrity_results)
    benchmark_meta = benchmark_results.get("benchmark_meta") if benchmark_results else None

    # 4. Console output
    if verbose:
        print_cli_summary_table(integrity_results, benchmark_meta)

    # 5. Save reports
    os.makedirs(os.path.dirname(json_report_path) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(md_report_path) or ".", exist_ok=True)

    json_payload = {
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "all_passed": all_passed,
        "summary": {
            "total_checks": len(integrity_results),
            "passed": sum(1 for r in integrity_results if r.passed),
            "failed": sum(1 for r in integrity_results if not r.passed),
            "total_execution_time_ms": round(
                sum(r.execution_time_ms for r in integrity_results), 2
            ),
        },
        "benchmark_meta": benchmark_meta,
        "checks": [asdict(r) for r in integrity_results],
    }

    with open(json_report_path, "w", encoding="utf-8") as f:
        json.dump(json_payload, f, indent=2)

    md_content = format_integrity_report_markdown(integrity_results, benchmark_meta)
    with open(md_report_path, "w", encoding="utf-8") as f:
        f.write(md_content)

    if verbose:
        print(f"Integrity results saved to JSON:     {json_report_path}")
        print(f"Integrity audit report saved to MD:  {md_report_path}\n")

    # 6. Optional test state reset after running
    if reset_after:
        if verbose:
            print("\n[TEARDOWN] Resetting database test state after integrity verification...")
        await reset_state()

    # Clean up Redis pool
    await close_redis_pool()

    return benchmark_results, integrity_results, all_passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run Concurrent Redis Fuzz Benchmark and Verify Database Integrity"
    )
    parser.add_argument(
        "--operations",
        type=int,
        default=500,
        help="Number of fuzz operations (default: 500)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Number of concurrent client workers (default: 10)",
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
        default=100,
        help="Warm-up read operations to pre-populate Redis cache (default: 100)",
    )
    parser.add_argument(
        "--reset-before",
        action="store_true",
        help="Reset database test state before running benchmark",
    )
    parser.add_argument(
        "--reset-after",
        action="store_true",
        help="Reset database test state after running benchmark",
    )
    parser.add_argument(
        "--skip-fuzz",
        action="store_true",
        help="Skip benchmark execution and run integrity checks on existing DB state",
    )
    parser.add_argument(
        "--json-report",
        type=str,
        default="reports/database_integrity_results.json",
        help="Path for JSON results output",
    )
    parser.add_argument(
        "--md-report",
        type=str,
        default="reports/database_integrity_report.md",
        help="Path for Markdown report output",
    )

    args = parser.parse_args()

    _, _, passed = asyncio.run(
        run_fuzz_and_verify(
            operations=args.operations,
            concurrency=args.concurrency,
            stream_workers=args.stream_workers,
            write_ratio=args.write_ratio,
            warmup=args.warmup,
            reset_before=args.reset_before,
            reset_after=args.reset_after,
            skip_fuzz=args.skip_fuzz,
            verbose=True,
            json_report_path=args.json_report,
            md_report_path=args.md_report,
        )
    )

    sys.exit(0 if passed else 1)
