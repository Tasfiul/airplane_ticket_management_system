"""Test State Reset Script for High-Concurrency Benchmark & Locust Runs.

Per GEMINI.md:
Between test runs, reset state without re-seeding by:
1. Truncating 'booking' and 'ticket' tables.
2. Resetting any booked 'flight_seat' rows back to 'AVAILABLE'.
3. Flushing Redis ephemeral locks (if Redis is running).
"""

import asyncio
import asyncpg
import redis.asyncio as aioredis
from app.core.config import settings


async def reset_state():
    print(f"Connecting to database {settings.POSTGRES_DB} to reset test state...")
    conn: asyncpg.Connection = await asyncpg.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        database=settings.POSTGRES_DB,
    )

    try:
        # 1. Truncate transactional tables
        print("Truncating transactional tables ('booking', 'ticket')...")
        await conn.execute("TRUNCATE TABLE ticket, booking RESTART IDENTITY CASCADE;")

        # 2. Reset booked seats back to AVAILABLE
        print("Resetting booked flight_seat rows back to 'AVAILABLE'...")
        updated = await conn.execute(
            "UPDATE flight_seat SET status = 'AVAILABLE' WHERE status != 'AVAILABLE';"
        )
        print(f" -> {updated}")

        # 3. Flush Redis Ephemeral Locks (optional/if available)
        try:
            r = aioredis.from_url(settings.redis_url)
            await r.flushdb()
            await r.aclose()
            print("Flushed Redis ephemeral locks and cache.")
        except Exception as e:
            print(f"Redis not available or flush skipped: {e}")

        # Verify
        bookings = await conn.fetchval("SELECT count(*) FROM booking;")
        tickets = await conn.fetchval("SELECT count(*) FROM ticket;")
        booked_seats = await conn.fetchval("SELECT count(*) FROM flight_seat WHERE status != 'AVAILABLE';")
        total_seats = await conn.fetchval("SELECT count(*) FROM flight_seat;")

        print("\nReset Summary:")
        print(f" - Bookings: {bookings}")
        print(f" - Tickets: {tickets}")
        print(f" - Booked Seats: {booked_seats}")
        print(f" - Total Flight Seats (intact): {total_seats:,}")
        print("State reset successfully ready for new test run!")

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(reset_state())
