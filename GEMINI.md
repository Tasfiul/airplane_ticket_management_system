# Project Overview
Backend high-concurrency API service and document vault for an airplane ticket management system built with FastAPI, Python, PostgreSQL (with SQLAlchemy 2.0 Async & Alembic migrations), PgBouncer, Redis, MinIO, and Nginx.

## Coding Conventions
- Always use a clean Layered Architecture (`routers/`, `schemas/`, `models/`, `services/`, `core/`, etc).
- Use explicit relative or absolute imports; never use wildcard imports (`from module import *`).
- Use `async def` for all routes and I/O operations (DB, Redis, MinIO).
- Always follow strict Pydantic v2 validation.
- Raise `HTTPException` (or custom domain exceptions registered with FastAPI exception handlers) with appropriate HTTP status codes and structured `detail` payloads.

## Database Handling
- Use SQLAlchemy 2.0 async syntax with `asyncpg`. Always avoid N+1 queries and prefer eager loading (`selectinload` / `joinedload`).
- **PgBouncer:** Route application database traffic through **PgBouncer** (in `transaction` pooling mode) to handle high concurrency, and configure `asyncpg` with `statement_cache_size=0` to prevent prepared statement conflicts.
- Implement rate limiting via **sliding window counter** using **Redis** for api calls.
- **High-Scale Seeding (5M Flight Seats / 50k Customers):**
  - Target seed volume: ~5,000,000 rows in the `flight_seat` table (initialized with `status = 'AVAILABLE'`), backed by foundational records in `plane`, `seat`, and `flight` (e.g., 100 planes × 200 seats = 20,000 `seat` rows across 25,000 scheduled `flight` rows), and ~50,000 rows in the `customer` table.
  - The transactional tables (`booking` and `ticket`) **MUST** start empty (0 rows) and are populated only during active checkout flows.
  - The database **MUST** be seeded using **PostgreSQL `COPY` (streaming via `STDIN` / `copy_records_to_table`)** in dependency order (`customer` -> `plane` -> `seat` -> `flight` -> `flight_seat`) for maximum write speed.
  - Create bare tables first and defer all foreign keys, `UNIQUE` constraints, and secondary indexes (`idx_flight_search`, `uq_active_ticket_per_seat`, etc.) until **after** the `COPY` stream completes.
- **Selective Persistence & Test Reset:**
  - Persist PostgreSQL data using a Docker volume so the 5M `flight_seat` inventory rows, parent `flight`/`plane`/`seat` records, and 50k `customer` rows remain permanently seeded across restarts (make the seed script idempotent by checking `SELECT EXISTS (SELECT 1 FROM flight_seat LIMIT 1)` so it skips seeding if data already exists).
  - Between test runs, reset state without re-seeding by truncating `booking` and `ticket`, resetting booked `flight_seat` rows back to `'AVAILABLE'`, and flushing Redis ephemeral locks.


## MinIO (Document Vault)
- Run MinIO in `docker-compose.yml` (ports `9000` for API and `9001` for Web UI) with an empty default bucket initialized on startup—do NOT pre-seed files.
- Only generate and upload ticket PDF documents to MinIO dynamically when a ticket purchase is completed.
- Generate ticket PDFs in memory (`io.BytesIO`) and upload them asynchronously (or via FastAPI `BackgroundTasks`) so file I/O never blocks high-concurrency booking requests.
- When running the test-reset script to clear orders between Locust runs, also purge all objects inside the MinIO bucket while keeping the bucket itself intact.

## Testing & QA
- **Schemathesis:** Use Schemathesis against `/openapi.json` for automated API fuzzing and edge-case error checking across all endpoints.
- **Locust (Multi-Endpoint Load Testing):**
  - Never test just a single endpoint or static ID. The `locustfile.py` **MUST** simulate realistic user journeys across **all API endpoints** using weighted `@task(N)` decorators.
  - Randomize user credentials (across the 50k users) and ticket IDs/search filters (across the 5M tickets) on every request to avoid unrealistic cache hits, and group dynamic URLs using Locust's `name` parameter.

## Docker
- Create or modify the `Dockerfile` and `docker-compose.yml` alongside infrastructure changes.
- Connect all containers (`fastapi`, `postgres`, `pgbouncer`, `redis`, `minio`, `nginx`) on a shared bridge network with healthchecks.
- Follow multi-stage `Dockerfile` builds.
- Expose host ports for `postgres`, `pgbouncer`, `redis`, and `minio` in `docker-compose.yml` and support `.env` host overrides (`localhost` vs Docker service names) so FastAPI, Alembic, and tests can run locally inside a WSL `.venv` against the Dockerized backing services.

##Conventions to imporve database query performance
Follow these conventions and make necessary changes according to the project: 
- Move Seat Inventory Checks into Redis (Atomic Lua / Hash):
Instead of invalidating READ_FLIGHT_SEATS on every booking and falling back to Postgres, store flight seat maps as a Redis Hash or Bitfield (HSET flight:{id}:seats 12A booked). Have WRITE_BOOK_SEATS atomically reserve the seat in Redis via a Lua script in under 2 ms, then push the persistence event to the Redis Stream asynchronously.

- Batch Redis Stream Worker Commits (Dynamic Micro-Batching):
Dynamically adapt batching based on Redis Stream consumer group queue depth (lag): flush immediately with minimal batch size and 0 ms accumulation delay when queue depth is low (<= queue_depth_threshold) to minimize single-request latency; batch up to max_batch_size messages (accumulating within batch_timeout_ms) only when queue depth rises (> queue_depth_threshold) to maximize throughput and minimize PgBouncer/Postgres transaction overhead. All parameters are configurable via .env (STREAM_WORKER_MIN_BATCH_SIZE, STREAM_WORKER_MAX_BATCH_SIZE, STREAM_WORKER_QUEUE_DEPTH_THRESHOLD, STREAM_WORKER_BATCH_TIMEOUT_MS, STREAM_WORKER_BLOCK_MS) with fallback defaults.


- Run EXPLAIN (ANALYZE, BUFFERS) on READ_ROUTE_SEARCH:
Add a composite index covering your route filter columns—typically (origin, destination, departure_date) INCLUDE (flight_id, status, price)—so Postgres serves cache misses via an Index-Only Scan in 2–5 ms instead of 559 ms.

- Add a Warm-up Phase and Zipfian Key Distribution to fuzz_test_redis.py:
Run 1,000 warm-up reads before starting the benchmark timer, and weight random ID selection toward a cluster of "popular" flights (random.paretovariate or numpy.random.zipf) with --operations 10000 --concurrency 100 to see how PgBouncer and Redis behave once the cache is actually populated.