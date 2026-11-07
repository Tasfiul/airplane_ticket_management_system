# Backend Service - Airplane Ticket Management System

FastAPI asynchronous API service, MinIO document vault, and background Redis Stream ingestion workers for high-concurrency airline booking.

## Architecture & Technology Stack

* **Framework:** FastAPI with SQLAlchemy 2.0 Async & Alembic.
* **Database Connection:** Routed through PgBouncer in `transaction` mode using `asyncpg` (`statement_cache_size=0`).
* **Caching & Shock Absorption:** Redis 7 with Cache-Aside, Redis Streams for writes, atomic Lua scripts for sub-2ms seat holds, and atomic Sliding Window Counter rate limiting.
* **Document Vault:** MinIO S3 object storage for dynamic, zero-disk in-memory PDF boarding pass generation and presigned download URLs.
* **Accelerators:** Native C `hiredis` RESP parser + Rust `orjson` JSON serialization.

## Key Directory Structure

```text
backend/
├── src/app/
│   ├── core/               # App configuration, database connection, Redis pool, security/JWT, rate limiter
│   ├── models/             # SQLAlchemy ORM models (customer, flight, plane, seat, booking, ticket)
│   ├── routers/            # FastAPI endpoint handlers (auth, customers, flights, bookings, tickets, admin)
│   ├── schemas/            # Strict Pydantic v2 validation schemas
│   ├── services/           # Business logic (cache, minio_storage, seat_inventory, stream_worker, write_stream)
│   ├── main.py             # FastAPI entrypoint (middleware & lifespan)
│   └── worker.py           # Standalone stream worker entrypoint
├── migrations/             # Alembic migration revisions
├── scripts/                # Benchmarking and seeding scripts
│   ├── fuzz_test_redis.py  # Mixed concurrent read/write benchmark
│   ├── run_schemathesis.py # Automated Schemathesis OpenAPI fuzzing runner
│   ├── test_stream_timing.py # Stream queue wait & transaction timing
│   └── reset_test_state.py # State reset utility (PostgreSQL tables, Redis locks, MinIO bucket purge)
├── schemathesis.toml       # Schemathesis configuration & warning suppressions
└── tests/                  # Pytest automated test suite (Phases 1-5, rate limiting, fuzzing, 73 tests)
```

## Running Tests & Benchmarks

```bash
# Run complete test suite (73 tests across all 5 phases & rate limiter)
uv run pytest tests/ -v

# Run Schemathesis OpenAPI fuzzing
uv run python scripts/run_schemathesis.py --max-examples 10 --request-timeout 15 --rate-limit auto

# Run mixed fuzz benchmark
uv run python scripts/fuzz_test_redis.py --operations 5000 --concurrency 40 --write-ratio 0.3 --stream-workers 0

# Run stream queue & transaction timing benchmark
uv run python scripts/test_stream_timing.py --operations 200 --concurrency 20
```
