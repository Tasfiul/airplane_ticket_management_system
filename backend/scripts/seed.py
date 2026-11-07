"""Database Seeding Script for High-Scale Airline Ticket Management System.

Target Volume:
- 50,000 Customers
- 100 Aircraft Planes
- 20,000 Aircraft Seats (200 seats per plane)
- 25,000 Scheduled Flights
- 5,000,000 Flight Seats (status = 'AVAILABLE')
- 0 Bookings / Tickets (transactional, starts empty)

Features:
- Idempotent: checks if data already exists before running.
- Defer constraints & secondary indexes during bulk insert for maximum write throughput.
- Re-enables all constraints, foreign keys, indexes, and updates sequences.
"""

import asyncio
import time
import asyncpg
from app.core.config import settings

BCRYPT_PASSWORD_HASH = "$2b$10$CRCpLuwFqEw/ZMqxpSzpEuM5euYTrdJFWDJkEO41v/K/lkzD7DZ6C"  # "password123"


async def seed():
    print(f"Connecting directly to PostgreSQL at {settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}...")
    conn: asyncpg.Connection = await asyncpg.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        database=settings.POSTGRES_DB,
    )

    try:
        # 1. Idempotency Check
        already_seeded = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM flight_seat LIMIT 1);"
        )
        if already_seeded:
            print("Database is already seeded with flight_seat inventory. Skipping seed.")
            return

        print("Starting high-scale database seeding...")
        start_time = time.perf_counter()

        # 2. Defer foreign keys, unique constraints, and secondary indexes
        print("Temporarily deferring foreign keys and secondary indexes for bulk speed...")
        await conn.execute("""
            ALTER TABLE ticket DROP CONSTRAINT IF EXISTS ticket_flight_seat_id_fkey;
            ALTER TABLE seat DROP CONSTRAINT IF EXISTS seat_plane_id_fkey;
            ALTER TABLE flight DROP CONSTRAINT IF EXISTS flight_plane_id_fkey;
            ALTER TABLE flight_seat DROP CONSTRAINT IF EXISTS flight_seat_flight_id_fkey;
            ALTER TABLE flight_seat DROP CONSTRAINT IF EXISTS flight_seat_seat_id_fkey;
            ALTER TABLE flight_seat DROP CONSTRAINT IF EXISTS uq_flight_seat;
            ALTER TABLE flight_seat DROP CONSTRAINT IF EXISTS flight_seat_pkey;
            DROP INDEX IF EXISTS idx_flight_search;
            DROP INDEX IF EXISTS idx_flight_plane_id;
            DROP INDEX IF EXISTS idx_flight_seat_seat_id;
        """)

        # 3. Seed 50,000 Customers
        print("Seeding 50,000 customers...")
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
            FROM generate_series(1, 50000) AS i;
        """)
        print(f" -> 50,000 customers seeded in {time.perf_counter() - t0:.2f}s")

        # 4. Seed 100 Planes
        print("Seeding 100 physical planes...")
        t0 = time.perf_counter()
        await conn.execute("""
            INSERT INTO plane (plane_id, tail_number, model, total_seats)
            SELECT 
                i,
                'N' || LPAD(i::text, 4, '0') || 'AA',
                (ARRAY['Boeing 737-800', 'Airbus A320neo', 'Boeing 787-9', 'Airbus A350-900'])[(i % 4) + 1],
                200
            FROM generate_series(1, 100) AS i;
        """)
        print(f" -> 100 planes seeded in {time.perf_counter() - t0:.2f}s")

        # 5. Seed 20,000 Seats (100 planes * 200 seats)
        print("Seeding 20,000 physical aircraft seats (200 seats / plane)...")
        t0 = time.perf_counter()
        await conn.execute("""
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
            FROM generate_series(1, 100) AS p
            CROSS JOIN generate_series(1, 200) AS s;
        """)
        print(f" -> 20,000 seats seeded in {time.perf_counter() - t0:.2f}s")

        # 6. Seed 25,000 Scheduled Flights
        print("Seeding 25,000 scheduled flights...")
        t0 = time.perf_counter()
        await conn.execute("""
            INSERT INTO flight (
                flight_id, flight_number, plane_id, departure_airport, arrival_airport,
                departure_time, arrival_time, status, created_at
            )
            SELECT 
                f,
                'FL' || LPAD(f::text, 5, '0'),
                ((f - 1) % 100 + 1),
                (ARRAY['JFK', 'LAX', 'ORD', 'DFW', 'DEN', 'SFO', 'SEA', 'ATL', 'MIA', 'BOS'])[dep_idx],
                (ARRAY['JFK', 'LAX', 'ORD', 'DFW', 'DEN', 'SFO', 'SEA', 'ATL', 'MIA', 'BOS'])[arr_idx],
                dep_time,
                dep_time + (interval '2 hours') + ((f % 5) * interval '1 hour'),
                'SCHEDULED',
                CURRENT_TIMESTAMP
            FROM generate_series(1, 25000) AS f
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
        print(f" -> 25,000 flights seeded in {time.perf_counter() - t0:.2f}s")

        # 7. Seed 5,000,000 Flight Seats (status = 'AVAILABLE')
        print("Seeding 5,000,000 flight seat inventory rows (status = 'AVAILABLE')...")
        t0 = time.perf_counter()
        await conn.execute("""
            INSERT INTO flight_seat (flight_seat_id, flight_id, seat_id, status)
            SELECT 
                ((f - 1) * 200 + s)::bigint,
                f,
                ((((f - 1) % 100)) * 200 + s)::int,
                'AVAILABLE'
            FROM generate_series(1, 25000) AS f
            CROSS JOIN generate_series(1, 200) AS s;
        """)
        print(f" -> 5,000,000 flight seats seeded in {time.perf_counter() - t0:.2f}s")

        # 8. Rebuild Primary Keys, Constraints, Foreign Keys & Secondary Indexes
        print("Re-establishing primary keys, constraints, foreign keys, and indexes...")
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
        print("Updating sequence counters...")
        await conn.execute("""
            SELECT setval('customer_customer_id_seq', (SELECT COALESCE(MAX(customer_id), 1) FROM customer));
            SELECT setval('plane_plane_id_seq', (SELECT COALESCE(MAX(plane_id), 1) FROM plane));
            SELECT setval('seat_seat_id_seq', (SELECT COALESCE(MAX(seat_id), 1) FROM seat));
            SELECT setval('flight_flight_id_seq', (SELECT COALESCE(MAX(flight_id), 1) FROM flight));
            SELECT setval('flight_seat_flight_seat_id_seq', (SELECT COALESCE(MAX(flight_seat_id), 1) FROM flight_seat));
        """)

        # 10. Update Query Planner Statistics
        print("Analyzing tables for optimizer statistics...")
        await conn.execute("""
            ANALYZE customer;
            ANALYZE plane;
            ANALYZE seat;
            ANALYZE flight;
            ANALYZE flight_seat;
        """)

        total_elapsed = time.perf_counter() - start_time
        print(f"\n========================================================")
        print(f"DATABASE SEEDING COMPLETED SUCCESSFULLY IN {total_elapsed:.2f}s!")
        print(f"========================================================")

        # 11. Print Counts
        customer_count = await conn.fetchval("SELECT count(*) FROM customer;")
        plane_count = await conn.fetchval("SELECT count(*) FROM plane;")
        seat_count = await conn.fetchval("SELECT count(*) FROM seat;")
        flight_count = await conn.fetchval("SELECT count(*) FROM flight;")
        flight_seat_count = await conn.fetchval("SELECT count(*) FROM flight_seat;")
        booking_count = await conn.fetchval("SELECT count(*) FROM booking;")
        ticket_count = await conn.fetchval("SELECT count(*) FROM ticket;")

        print(f"Customer rows:    {customer_count:,}")
        print(f"Plane rows:       {plane_count:,}")
        print(f"Seat rows:        {seat_count:,}")
        print(f"Flight rows:      {flight_count:,}")
        print(f"Flight Seat rows: {flight_seat_count:,}")
        print(f"Booking rows:     {booking_count:,} (must be 0)")
        print(f"Ticket rows:      {ticket_count:,} (must be 0)")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(seed())
