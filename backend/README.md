# Backend Service - Airplane Ticket Management System

FastAPI asynchronous API service and background Redis Stream ingestion workers for high-concurrency airline booking.

## Architecture

* **Framework:** FastAPI with SQLAlchemy 2.0 Async & Alembic.
* **Database Connection:** Routed through PgBouncer in `transaction` mode using `asyncpg` (`statement_cache_size=0`).
* **Caching & Queuing:** Redis 7 with Cache-Aside, Redis Streams for writes, and atomic Lua script seat reservation.
* **Accelerators:** Native C `hiredis` RESP parser + Rust `orjson` JSON serialization.

## Key Directory Structure

```text
backend/
├── src/app/
│   ├── core/               # App configuration, database connection, Redis pool, rate limiting
│   ├── models/             # SQLAlchemy ORM models (customer, flight, plane, seat, booking, ticket)
│   ├── routers/            # FastAPI endpoint handlers (flights, customers, bookings)
│   ├── schemas/            # Strict Pydantic v2 validation schemas
│   ├── services/           # Business logic (cache, seat_inventory, stream_worker, write_stream)
│   ├── main.py             # FastAPI entrypoint
│   └── worker.py           # Standalone stream worker entrypoint
├── migrations/             # Alembic migration revisions
├── scripts/                # Benchmarking and seeding scripts
│   ├── fuzz_test_redis.py  # Mixed concurrent read/write benchmark
│   ├── test_stream_timing.py # Stream queue wait & transaction timing
│   └── reset_test_state.py # State reset utility
└── tests/                  # Pytest automated test suite
```

## Running Tests & Benchmarks

```bash
# Run test suite
uv run pytest tests/

# Run 10k mixed benchmark at optimal concurrency
uv run python scripts/fuzz_test_redis.py --operations 10000 --concurrency 100 --write-ratio 0.3
```
