# Backend Service - Airplane Ticket Management System

FastAPI asynchronous API service, MinIO document vault, and background Redis Stream ingestion workers for high-concurrency airline booking.

## Architecture & Technology Stack

* **Framework:** FastAPI with SQLAlchemy 2.0 Async & Alembic.
* **Database Connection:** Routed through PgBouncer in `transaction` mode using `asyncpg` (`statement_cache_size=0`).
* **Caching & Shock Absorption:** Redis 7 with Cache-Aside, Redis Streams for writes, and atomic Lua scripts for sub-2ms seat holds.
* **Document Vault:** MinIO S3 object storage for dynamic, zero-disk in-memory PDF boarding pass generation and presigned download URLs.
* **Accelerators:** Native C `hiredis` RESP parser + Rust `orjson` JSON serialization.

## Key Directory Structure

```text
backend/
├── src/app/
│   ├── core/               # App configuration, database connection, Redis pool, security/JWT
│   ├── models/             # SQLAlchemy ORM models (customer, flight, plane, seat, booking, ticket)
│   ├── routers/            # FastAPI endpoint handlers (auth, customers, flights, bookings, tickets, admin)
│   ├── schemas/            # Strict Pydantic v2 validation schemas
│   ├── services/           # Business logic (cache, minio_storage, seat_inventory, stream_worker, write_stream)
│   ├── main.py             # FastAPI entrypoint
│   └── worker.py           # Standalone stream worker entrypoint
├── migrations/             # Alembic migration revisions
├── scripts/                # Benchmarking and seeding scripts
│   ├── fuzz_test_redis.py  # Mixed concurrent read/write benchmark
│   ├── test_stream_timing.py # Stream queue wait & transaction timing
│   └── reset_test_state.py # State reset utility (PostgreSQL tables, Redis locks, MinIO bucket purge)
└── tests/                  # Pytest automated test suite (Phases 1-5, dynamic batching, fuzzing, 37 tests)
```

## Running Tests & Benchmarks

```bash
# Run complete test suite (37 tests across all 5 phases)
uv run pytest tests/ -v

# Run mixed fuzz benchmark
uv run python scripts/fuzz_test_redis.py --operations 5000 --concurrency 40 --write-ratio 0.3 --stream-workers 0

# Run stream queue & transaction timing benchmark
uv run python scripts/test_stream_timing.py --operations 200 --concurrency 20
```
