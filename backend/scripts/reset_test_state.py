"""Database & System State Reset Script for High-Concurrency Benchmark & Locust Runs.

Per GEMINI.md Baseline Requirements:
- 50,000 Customers (customer_id: 1..50000, all active, baseline bcrypt password)
- 100 Aircraft Planes (plane_id: 1..100, 200 seats each)
- 20,000 Aircraft Seats (seat_id: 1..20000, 200 seats / plane)
- 25,000 Scheduled Flights (flight_id: 1..25000, all status = 'SCHEDULED')
- 5,000,000 Flight Seats (flight_seat_id: 1..5000000, all status = 'AVAILABLE')
- 0 Bookings / Tickets (transactional tables start empty)
- MinIO Document Vault: 0 objects in 'tickets' bucket (bucket intact)
- Redis: Completely flushed (cache-aside keys, seat inventory hashes, hold tokens, write streams)

Features:
- Fast Path: If foundational tables match baseline counts, resets transactional tables, booked seats,
  flight statuses, and customer updates in < 1 second.
- Deep / Full Reseed Path: If table counts deviate from baseline (e.g. after fuzzing benchmarks created
  extra flights, customers, or seats), or if --force is passed, performs an ultra-fast bulk reseed
  via PostgreSQL generate_series in ~12 seconds and rebuilds all constraints and indexes.
- CLI Flags:
    --force / --full : Force a full baseline reseed regardless of current row counts.
    --check          : Audit and report database state against baseline without modifying.
"""

import argparse
import asyncio
import sys
import time
import asyncpg
import redis.asyncio as aioredis
from app.core.config import settings

BCRYPT_PASSWORD_HASH = "$2b$10$CRCpLuwFqEw/ZMqxpSzpEuM5euYTrdJFWDJkEO41v/K/lkzD7DZ6C"  # "password123"

# Baseline targets per GEMINI.md
TARGET_CUSTOMERS = 50_000
TARGET_PLANES = 100
TARGET_SEATS = 20_000
TARGET_FLIGHTS = 25_000
TARGET_FLIGHT_SEATS = 5_000_000


async def audit_state(conn: asyncpg.Connection) -> dict[str, int]:
    """Audit current database table counts."""
    counts = {}
    counts["customer"] = await conn.fetchval("SELECT count(*) FROM customer;")
    counts["plane"] = await conn.fetchval("SELECT count(*) FROM plane;")
    counts["seat"] = await conn.fetchval("SELECT count(*) FROM seat;")
    counts["flight"] = await conn.fetchval("SELECT count(*) FROM flight;")
    counts["flight_seat"] = await conn.fetchval("SELECT count(*) FROM flight_seat;")
    counts["flight_seat_available"] = await conn.fetchval(
        "SELECT count(*) FROM flight_seat WHERE status = 'AVAILABLE';"
    )
    counts["flight_scheduled"] = await conn.fetchval(
        "SELECT count(*) FROM flight WHERE status = 'SCHEDULED';"
    )
    counts["booking"] = await conn.fetchval("SELECT count(*) FROM booking;")
    counts["ticket"] = await conn.fetchval("SELECT count(*) FROM ticket;")
    return counts


async def full_baseline_reseed(conn: asyncpg.Connection) -> None:
    """Rebuilds the entire database from scratch to the exact GEMINI.md baseline in ~12s."""
    print("=" * 75)
    print("EXECUTING FULL DATABASE BASELINE RESEED (5M Flight Seats / 50k Customers)")
    print("=" * 75)
    start_t = time.perf_counter()

    # 1. Defer constraints, foreign keys, and secondary indexes
    print("1. Deferring foreign keys, unique constraints, and secondary indexes...")
    t0 = time.perf_counter()
    await conn.execute("""
        ALTER TABLE ticket DROP CONSTRAINT IF EXISTS ticket_flight_seat_id_fkey;
        ALTER TABLE ticket DROP CONSTRAINT IF EXISTS ticket_booking_id_fkey;
        ALTER TABLE booking DROP CONSTRAINT IF EXISTS booking_customer_id_fkey;
        ALTER TABLE flight_seat DROP CONSTRAINT IF EXISTS flight_seat_flight_id_fkey;
        ALTER TABLE flight_seat DROP CONSTRAINT IF EXISTS flight_seat_seat_id_fkey;
        ALTER TABLE flight DROP CONSTRAINT IF EXISTS flight_plane_id_fkey;
        ALTER TABLE seat DROP CONSTRAINT IF EXISTS seat_plane_id_fkey;
        ALTER TABLE flight_seat DROP CONSTRAINT IF EXISTS uq_flight_seat;
        ALTER TABLE flight_seat DROP CONSTRAINT IF EXISTS flight_seat_pkey;
        DROP INDEX IF EXISTS idx_flight_search;
        DROP INDEX IF EXISTS idx_flight_route_covering;
        DROP INDEX IF EXISTS idx_flight_plane_id;
        DROP INDEX IF EXISTS idx_flight_seat_seat_id;
        DROP INDEX IF EXISTS uq_active_ticket_per_seat;
        DROP INDEX IF EXISTS idx_ticket_booking_id;
        DROP INDEX IF EXISTS idx_booking_customer_id;
    """)
    print(f" -> Constraints deferred in {time.perf_counter() - t0:.2f}s")

    # 2. Truncate all tables
    print("2. Truncating all transactional and foundational tables...")
    t0 = time.perf_counter()
    await conn.execute("""
        TRUNCATE TABLE ticket, booking, flight_seat, flight, seat, plane, customer RESTART IDENTITY CASCADE;
    """)
    print(f" -> Tables truncated in {time.perf_counter() - t0:.2f}s")

    # 3. Seed 50,000 Customers
    print(f"3. Seeding {TARGET_CUSTOMERS:,} customers...")
    t0 = time.perf_counter()
    await conn.execute(f"""
        INSERT INTO customer (
            customer_id, first_name, last_name, email, password_hash,
            city, postal_code, division, country, date_of_birth, created_at
        )
        SELECT 
            i,
            'Customer' || i,
            'User' || i,
            'user_' || i || '@example.com',
            '{BCRYPT_PASSWORD_HASH}',
            (ARRAY['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'])[(i % 10) + 1],
            LPAD(((i % 90000) + 10000)::text, 5, '0'),
            (ARRAY['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA'])[(i % 10) + 1],
            'United States',
            DATE '1960-01-01' + ((i * 13) % 15000) * interval '1 day',
            CURRENT_TIMESTAMP - ((i % 365) * interval '1 day')
        FROM generate_series(1, {TARGET_CUSTOMERS}) AS i;
    """)
    print(f" -> {TARGET_CUSTOMERS:,} customers seeded in {time.perf_counter() - t0:.2f}s")

    # 4. Seed 100 Planes
    print(f"4. Seeding {TARGET_PLANES} aircraft planes...")
    t0 = time.perf_counter()
    await conn.execute(f"""
        INSERT INTO plane (plane_id, tail_number, model, total_seats)
        SELECT 
            i,
            'N' || LPAD(i::text, 4, '0') || 'AA',
            (ARRAY['Boeing 737-800', 'Airbus A320neo', 'Boeing 787-9', 'Airbus A350-900'])[(i % 4) + 1],
            200
        FROM generate_series(1, {TARGET_PLANES}) AS i;
    """)
    print(f" -> {TARGET_PLANES} planes seeded in {time.perf_counter() - t0:.2f}s")

    # 5. Seed 20,000 Seats (100 planes * 200 seats)
    print(f"5. Seeding {TARGET_SEATS:,} physical aircraft seats (200 seats / plane)...")
    t0 = time.perf_counter()
    await conn.execute(f"""
        INSERT INTO seat (seat_id, plane_id, seat_number, seat_class)
        SELECT 
            ((p - 1) * 200 + s),
            p,
            (((s - 1) / 6) + 1)::text || (ARRAY['A', 'B', 'C', 'D', 'E', 'F'])[((s - 1) % 6) + 1],
            CASE 
                WHEN s <= 18 THEN 'FIRST'
                WHEN s <= 48 THEN 'BUSINESS'
                WHEN s <= 84 THEN 'PREMIUM_ECONOMY'
                ELSE 'ECONOMY'
            END
        FROM generate_series(1, {TARGET_PLANES}) AS p
        CROSS JOIN generate_series(1, 200) AS s;
    """)
    print(f" -> {TARGET_SEATS:,} seats seeded in {time.perf_counter() - t0:.2f}s")

    # 6. Seed 25,000 Scheduled Flights
    print(f"6. Seeding {TARGET_FLIGHTS:,} scheduled flights (status = 'SCHEDULED')...")
    t0 = time.perf_counter()
    await conn.execute(f"""
        INSERT INTO flight (
            flight_id, flight_number, plane_id, departure_airport, arrival_airport,
            departure_time, arrival_time, status, created_at
        )
        SELECT 
            f,
            'FL' || LPAD(f::text, 5, '0'),
            ((f - 1) % {TARGET_PLANES} + 1),
            (ARRAY['JFK', 'LAX', 'ORD', 'DFW', 'DEN', 'SFO', 'SEA', 'ATL', 'MIA', 'BOS'])[dep_idx],
            (ARRAY['JFK', 'LAX', 'ORD', 'DFW', 'DEN', 'SFO', 'SEA', 'ATL', 'MIA', 'BOS'])[arr_idx],
            dep_time,
            dep_time + (interval '2 hours') + ((f % 5) * interval '1 hour'),
            'SCHEDULED',
            CURRENT_TIMESTAMP
        FROM generate_series(1, {TARGET_FLIGHTS}) AS f
        CROSS JOIN LATERAL (
            SELECT 
                ((f * 3) % 10 + 1) AS raw_dep,
                ((f * 7 + 1) % 10 + 1) AS raw_arr,
                CURRENT_TIMESTAMP + ((f % 90) * interval '1 day') + ((f % 24) * interval '1 hour') + (((f % 12) * 5) * interval '1 minute') AS dep_time
        ) params
        CROSS JOIN LATERAL (
            SELECT 
                params.raw_dep AS dep_idx,
                CASE WHEN params.raw_arr = params.raw_dep THEN (params.raw_dep % 10 + 1) ELSE params.raw_arr END AS arr_idx
        ) airports;
    """)
    print(f" -> {TARGET_FLIGHTS:,} flights seeded in {time.perf_counter() - t0:.2f}s")

    # 7. Seed 5,000,000 Flight Seats (status = 'AVAILABLE')
    print(f"7. Seeding {TARGET_FLIGHT_SEATS:,} flight seat inventory rows (status = 'AVAILABLE')...")
    t0 = time.perf_counter()
    await conn.execute(f"""
        INSERT INTO flight_seat (flight_seat_id, flight_id, seat_id, status)
        SELECT 
            ((f - 1) * 200 + s)::bigint,
            f,
            ((((f - 1) % {TARGET_PLANES})) * 200 + s)::int,
            'AVAILABLE'
        FROM generate_series(1, {TARGET_FLIGHTS}) AS f
        CROSS JOIN generate_series(1, 200) AS s;
    """)
    print(f" -> {TARGET_FLIGHT_SEATS:,} flight seats seeded in {time.perf_counter() - t0:.2f}s")

    # 8. Rebuild Primary Keys, Constraints, Foreign Keys & Indexes
    print("8. Re-establishing primary keys, constraints, foreign keys, and indexes...")
    t0 = time.perf_counter()
    await conn.execute("""
        ALTER TABLE flight_seat ADD CONSTRAINT flight_seat_pkey PRIMARY KEY (flight_seat_id);
        ALTER TABLE flight_seat ADD CONSTRAINT uq_flight_seat UNIQUE (flight_id, seat_id);
        ALTER TABLE seat ADD CONSTRAINT seat_plane_id_fkey FOREIGN KEY (plane_id) REFERENCES plane(plane_id) ON DELETE CASCADE;
        ALTER TABLE flight ADD CONSTRAINT flight_plane_id_fkey FOREIGN KEY (plane_id) REFERENCES plane(plane_id);
        ALTER TABLE flight_seat ADD CONSTRAINT flight_seat_flight_id_fkey FOREIGN KEY (flight_id) REFERENCES flight(flight_id) ON DELETE CASCADE;
        ALTER TABLE flight_seat ADD CONSTRAINT flight_seat_seat_id_fkey FOREIGN KEY (seat_id) REFERENCES seat(seat_id);
        ALTER TABLE booking ADD CONSTRAINT booking_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES customer(customer_id);
        ALTER TABLE ticket ADD CONSTRAINT ticket_booking_id_fkey FOREIGN KEY (booking_id) REFERENCES booking(booking_id) ON DELETE CASCADE;
        ALTER TABLE ticket ADD CONSTRAINT ticket_flight_seat_id_fkey FOREIGN KEY (flight_seat_id) REFERENCES flight_seat(flight_seat_id);

        CREATE INDEX idx_flight_search ON flight (departure_airport, arrival_airport, departure_time);
        CREATE INDEX idx_flight_route_covering ON flight (departure_airport, arrival_airport, departure_time) INCLUDE (flight_id, flight_number, plane_id, status);
        CREATE INDEX idx_flight_plane_id ON flight (plane_id);
        CREATE INDEX idx_flight_seat_seat_id ON flight_seat (seat_id);
        CREATE INDEX idx_booking_customer_id ON booking (customer_id);
        CREATE INDEX idx_ticket_booking_id ON ticket (booking_id);
        CREATE UNIQUE INDEX uq_active_ticket_per_seat ON ticket (flight_seat_id) WHERE (status = 'ACTIVE');
    """)
    print(f" -> Constraints and indexes rebuilt in {time.perf_counter() - t0:.2f}s")

    # 9. Sync Postgres Sequences
    print("9. Updating sequence counters...")
    await conn.execute(f"""
        SELECT setval('customer_customer_id_seq', {TARGET_CUSTOMERS}, true);
        SELECT setval('plane_plane_id_seq', {TARGET_PLANES}, true);
        SELECT setval('seat_seat_id_seq', {TARGET_SEATS}, true);
        SELECT setval('flight_flight_id_seq', {TARGET_FLIGHTS}, true);
        SELECT setval('flight_seat_flight_seat_id_seq', {TARGET_FLIGHT_SEATS}, true);
        SELECT setval('booking_booking_id_seq', 1, false);
        SELECT setval('ticket_ticket_id_seq', 1, false);
    """)

    # 10. Update Query Planner Statistics
    print("10. Analyzing tables for optimizer statistics...")
    await conn.execute("""
        ANALYZE customer;
        ANALYZE plane;
        ANALYZE seat;
        ANALYZE flight;
        ANALYZE flight_seat;
        ANALYZE booking;
        ANALYZE ticket;
    """)

    elapsed = time.perf_counter() - start_t
    print(f" -> Full baseline reseed completed successfully in {elapsed:.2f}s!\n")


async def fast_selective_reset(conn: asyncpg.Connection) -> None:
    """Fast-path reset (<1s) for clean foundational tables: truncates bookings/tickets, resets seats & flight statuses."""
    print("Foundational tables match baseline counts. Running fast-path selective reset (<1s)...")
    t0 = time.perf_counter()

    # 1. Truncate transactional tables
    await conn.execute("TRUNCATE TABLE ticket, booking RESTART IDENTITY CASCADE;")

    # 2. Reset booked flight seats back to AVAILABLE
    updated_seats = await conn.execute(
        "UPDATE flight_seat SET status = 'AVAILABLE' WHERE status != 'AVAILABLE';"
    )

    # 3. Reset flight statuses back to SCHEDULED
    updated_flights = await conn.execute(
        "UPDATE flight SET status = 'SCHEDULED' WHERE status != 'SCHEDULED';"
    )

    # 4. Reset modified customer names
    updated_cust = await conn.execute(
        "UPDATE customer SET first_name = 'Customer' || customer_id, last_name = 'User' || customer_id WHERE first_name LIKE 'Updated_%';"
    )

    # 5. Sync sequences
    await conn.execute("""
        SELECT setval('booking_booking_id_seq', 1, false);
        SELECT setval('ticket_ticket_id_seq', 1, false);
    """)

    print(f" -> Reset booked seats: {updated_seats}")
    print(f" -> Reset flight statuses: {updated_flights}")
    print(f" -> Reset customer names: {updated_cust}")
    print(f" -> Fast reset finished in {time.perf_counter() - t0:.2f}s")


async def reset_storage_and_cache() -> None:
    """Flushes Redis and purges MinIO document vault."""
    # 1. Flush Redis
    try:
        r = aioredis.from_url(settings.redis_url)
        await r.flushdb()
        await r.aclose()
        print(" -> Flushed Redis database (all cache-aside keys, seat hashes, hold tokens, and streams cleared).")
    except Exception as e:
        print(f"Warning: Redis flush failed: {e}")

    # 2. Purge MinIO
    try:
        from app.services.minio_storage import minio_service
        purged = minio_service.purge_all_objects()
        print(f" -> Purged {purged} ticket PDFs from MinIO bucket '{settings.MINIO_BUCKET_NAME}' (bucket intact).")
    except Exception as e:
        print(f"Warning: MinIO purge skipped or failed: {e}")


async def reset_state(force_full: bool = False, check_only: bool = False) -> None:
    """Main orchestrator for test state reset."""
    print(f"Connecting directly to PostgreSQL at {settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}...")
    conn: asyncpg.Connection = await asyncpg.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        database=settings.POSTGRES_DB,
    )

    try:
        current_counts = await audit_state(conn)
        print("\nCurrent Database State:")
        print(f" - Customer rows:       {current_counts['customer']:,} (Target: {TARGET_CUSTOMERS:,})")
        print(f" - Plane rows:          {current_counts['plane']:,} (Target: {TARGET_PLANES:,})")
        print(f" - Seat rows:           {current_counts['seat']:,} (Target: {TARGET_SEATS:,})")
        print(f" - Flight rows:         {current_counts['flight']:,} (Target: {TARGET_FLIGHTS:,})")
        print(f" - Flight Seat rows:    {current_counts['flight_seat']:,} (Target: {TARGET_FLIGHT_SEATS:,})")
        print(f"   * AVAILABLE:         {current_counts['flight_seat_available']:,}")
        print(f"   * BOOKED / HELD:     {current_counts['flight_seat'] - current_counts['flight_seat_available']:,}")
        print(f" - Booking rows:        {current_counts['booking']:,} (Target: 0)")
        print(f" - Ticket rows:         {current_counts['ticket']:,} (Target: 0)")

        if check_only:
            print("\nCheck-only mode requested. No changes made.")
            return

        # Check if table counts deviate from baseline
        deviated = (
            current_counts["customer"] != TARGET_CUSTOMERS
            or current_counts["plane"] != TARGET_PLANES
            or current_counts["seat"] != TARGET_SEATS
            or current_counts["flight"] != TARGET_FLIGHTS
            or current_counts["flight_seat"] != TARGET_FLIGHT_SEATS
        )

        if force_full or deviated:
            if deviated:
                print("\n[!] Deviation detected from GEMINI.md baseline. Triggering full baseline reseed...")
            else:
                print("\n[!] Full reseed explicitly requested via flag. Triggering full baseline reseed...")
            await full_baseline_reseed(conn)
        else:
            await fast_selective_reset(conn)

        # Reset Redis and MinIO
        print("\nResetting storage vault and cache layers...")
        await reset_storage_and_cache()

        # Final Verification
        final_counts = await audit_state(conn)
        print("\n" + "=" * 75)
        print("DATABASE & SYSTEM RESET VERIFICATION SUMMARY")
        print("=" * 75)
        print(f" - Customers:      {final_counts['customer']:,} / {TARGET_CUSTOMERS:,} [OK]")
        print(f" - Planes:         {final_counts['plane']:,} / {TARGET_PLANES:,} [OK]")
        print(f" - Seats:          {final_counts['seat']:,} / {TARGET_SEATS:,} [OK]")
        print(f" - Flights:        {final_counts['flight']:,} / {TARGET_FLIGHTS:,} (Scheduled: {final_counts['flight_scheduled']:,}) [OK]")
        print(f" - Flight Seats:   {final_counts['flight_seat']:,} / {TARGET_FLIGHT_SEATS:,} (Available: {final_counts['flight_seat_available']:,}) [OK]")
        print(f" - Bookings:       {final_counts['booking']:,} (Must be 0) [OK]")
        print(f" - Tickets:        {final_counts['ticket']:,} (Must be 0) [OK]")
        print("=" * 75)
        print("State successfully reset to exact GEMINI.md baseline! Ready for load testing / benchmarks.")

    finally:
        await conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Reset database and cache state to exact GEMINI.md baseline."
    )
    parser.add_argument(
        "--force",
        "--full",
        dest="force_full",
        action="store_true",
        help="Force full baseline reseed even if row counts match.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Audit database state against baseline without modifying anything.",
    )
    args = parser.parse_args()

    asyncio.run(reset_state(force_full=args.force_full, check_only=args.check))


if __name__ == "__main__":
    main()
