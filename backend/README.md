# Backend Service - Airplane Ticket Management System

FastAPI asynchronous API service, Nginx reverse proxy, MinIO document vault, and background Redis Stream ingestion workers for high-concurrency airline booking.

## Architecture & Technology Stack

* **Ingress / Reverse Proxy:** Nginx with keepalive upstream pools, dynamic configuration templating via environment variables, Gzip compression, and connection monitoring (`/nginx_status`).
* **Framework:** FastAPI with SQLAlchemy 2.0 Async & Alembic.
* **Database Connection:** Routed through PgBouncer in `transaction` mode using `asyncpg` (`statement_cache_size=0`).
* **Caching & Shock Absorption:** Redis 7 with Cache-Aside, Redis Streams for writes, atomic Lua scripts for sub-2ms seat holds, and atomic Sliding Window Counter rate limiting.
* **Document Vault:** MinIO S3 object storage for dynamic, zero-disk in-memory PDF boarding pass generation and presigned download URLs.
* **Accelerators:** Native C `hiredis` RESP parser + Rust `orjson` JSON serialization.

## Key Directory Structure

```text
backend/
├── nginx/                  # Nginx reverse proxy configuration & entrypoint templates
│   ├── docker-entrypoint.d/# Custom envsubst injection for global worker parameters
│   ├── templates/          # Virtual host templates with upstream keepalive pools
│   └── nginx.conf.template # High-concurrency core Nginx event configuration
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
└── tests/                  # Pytest automated test suite (Phases 1-5, rate limiting, fuzzing, Nginx, 80 tests)
```

## Running Tests & Benchmarks

```bash
# Run complete test suite (80 tests across all 5 phases, rate limiter, and Nginx proxy)
uv run pytest tests/ -v

# Run Schemathesis OpenAPI fuzzing
uv run python scripts/run_schemathesis.py --max-examples 10 --request-timeout 15 --rate-limit auto

# Run mixed fuzz benchmark
uv run python scripts/fuzz_test_redis.py --operations 5000 --concurrency 40 --write-ratio 0.3 --stream-workers 0

# Run stream queue & transaction timing benchmark
uv run python scripts/test_stream_timing.py --operations 200 --concurrency 20

# Run production-scale Locust multi-persona load tests through Nginx (port 80)
./run_locust.sh
# Or run headless:
uv run locust --config locust.conf --headless -u 1500 -r 50 -t 2m --requests-per-minute 20
```

## Production-Scale Benchmark Highlights (5.2M Row Database)

Benchmarked against 5.2 million real records on a single laptop WSL2 environment (12 cores, 82% RAM baseline, co-located Locust load generator):

* **100 Users**: 34.4 req/s | Median: 10 ms | p95: 39 ms | **0.00% errors**
* **500 Users**: 174.5 req/s | Median: 21 ms | p95: 120 ms | **0.00% errors**
* **1,000 Users**: 333.6 req/s | Median: 66 ms | p95: 330 ms | **0.00% errors**
* **1,500 Users**: **515.7 req/s** | Median: 160 ms | p95: 1,200 ms | **0.00% errors**
* **2,000 Users**: 459.1 req/s | Median: 330 ms | 97.14% success rate under complete single-box hardware saturation (100% read success)

### Core Architectural Resiliency
1. **CQRS Write-Decoupling**: Write operations funnel into Redis Streams without blocking HTTP client loops.
2. **Atomic In-Memory Seat Holds**: Sub-2ms seat holds via Redis Lua prevent database row locking.
3. **Index-Only Flight Searches**: Covering index `idx_flight_route_covering` delivers 2–5ms cache-miss queries across 26,000 flights.
4. **$O(1)$ Sliding Window Rate Limiting**: Atomic Lua approximation prevents memory bloat while enforcing strict RFC 6585 headers.
5. **Zero-Disk S3 PDF Streaming**: In-memory ReportLab generation and presigned S3 URLs prevent disk I/O thrashing.
