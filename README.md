# Airplane Ticket Management System

High-concurrency backend API service, document vault, and asynchronous write ingestion engine for an airplane ticket management system built with **FastAPI**, **PostgreSQL** (SQLAlchemy 2.0 Async + asyncpg), **PgBouncer**, **Redis**, and **MinIO**.

---

## 1. System Architecture & Component Decoupling

The platform decouples stateless HTTP request handling from background database write ingestion and document storage using a shared bridge network (`ticket_network`):

```text
                   ┌──────────────────────────────────────────────┐
                   │               FastAPI (api)                  │
                   │    HTTP REST API / In-Memory Validation      │
                   └──────┬─────────────────────┬──────────┬──────┘
                          │ Cache-Aside Reads   │          │ Presigned S3 Redirect
                          ▼                     │          ▼
                   ┌──────────────┐             │   ┌──────────────┐
                   │    Redis     │             │   │    MinIO     │
                   │  Cache-Aside │             │   │Document Vault│
                   └──────────────┘             │   │(Port 9000/01)│
                                                ▼   └──────────────┘
                                         ┌──────────────┐  ▲
                                         │ Redis Stream │  │ Background PDF
                                         │(db_writes)   │  │ Uploads
                                         └──────┬───────┘  │
                                                │          │
                                                ▼          │
                                         ┌──────────────┐  │
                                         │Stream Workers│──┘
                                         │(stream-worker│
                                         └──────┬───────┘
                                                │ Dynamic Micro-Batching
                                                │ (UNNEST / COPY STDIN)
                                                ▼
                                         ┌──────────────┐
                                         │  PgBouncer   │
                                         │(Transaction) │
                                         └──────┬───────┘
                                                │
                                                ▼
                                         ┌──────────────┐
                                         │  PostgreSQL  │
                                         │  (Durable)   │
                                         └──────────────┘
```

* **`api` Container**: Stateless HTTP service running Uvicorn + FastAPI on port `8000`. Handles reads via cache-aside, places ephemeral seat holds via Redis Lua, and dispatches write events to Redis Streams.
* **`stream-worker` Container**: Dedicated background worker pool (`STREAM_WORKERS_COUNT`, default: 4) consuming from consumer group `db_writers`. Executes bulk transactions via PgBouncer, pipelines immediate PubSub notifications, and asynchronously uploads boarding pass PDFs to MinIO.
* **`minio` Container**: S3-compatible object storage document vault running on port `9000` (API) and `9001` (Web UI) with bucket `tickets` initialized on boot.
* **`pgbouncer`**: Transaction pooling proxy on port `6432`. Enforces connection limits and allows high client concurrency with minimal backend Postgres connections (`statement_cache_size=0`).
* **`postgres`**: Relational database running PostgreSQL 16 on port `5432` with a persistent Docker volume.
* **`redis`**: In-memory data store for cache-aside reads, stream queuing, and atomic seat locking on port `6379`.

---

## 2. Complete 5-Phase API Matrix

All planned modules and endpoints across the 5 phases are fully implemented, benchmarked, and verified:

| Phase | Module | Method | Endpoint | HTTP Code | Architecture Pattern / Storage Strategy |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Phase 1** | **Auth & Profiles** | `POST` | `/api/v1/auth/register` | `201 Created` | Redis Stream (`CREATE_CUSTOMER`) |
| | | `POST` | `/api/v1/auth/login` | `200 OK` | Cache-Aside Read + Bcrypt -> JSON JWT |
| | | `GET` | `/api/v1/customers/me` | `200 OK` | Cache-Aside Read (from JWT `sub`) |
| | | `PATCH` | `/api/v1/customers/me` | `200 OK` | Redis Stream (`UPDATE_CUSTOMER`) |
| | | `GET` | `/api/v1/customers/me/bookings` | `200 OK` | Cache-Aside Read (Paginated) |
| **Phase 2** | **Flight Discovery** | `GET` | `/api/v1/flights/search` | `200 OK` | Composite Covering Index-Only Scan + Cache-Aside |
| | | `GET` | `/api/v1/flights/{flight_id}` | `200 OK` | Cache-Aside (`cache:flight:{id}`) |
| | | `GET` | `/api/v1/flights/airports` | `200 OK` | Cached Airport Autocomplete List |
| **Phase 3** | **Seat Map & Holds** | `GET` | `/api/v1/flights/{id}/seats` | `200 OK` | Redis Hash Sub-Millisecond Status & Metadata |
| | | `POST` | `/api/v1/flights/{id}/seats/hold` | `200 OK` | Redis Atomic Lua Script + 10m TTL Key |
| | | `POST` | `/api/v1/flights/{id}/seats/release`| `200 OK` | Redis Atomic Lua Script Release |
| | **Checkout & Bookings** | `POST` | `/api/v1/bookings` | `202 Accepted` | Redis Stream (`CREATE_BOOKING`, status: `PENDING`) |
| | | `POST` | `/api/v1/bookings/{id}/confirm` | `201 Created` | Redis Stream (`CONFIRM_PURCHASE`) + Sync DB Commit |
| | | `GET` | `/api/v1/bookings/{id}` | `200 OK` | Cache-Aside Read |
| | | `GET` | `/api/v1/bookings/lookup` | `200 OK` | PNR Search (`booking_code` + `last_name`) |
| | | `POST` | `/api/v1/bookings/{id}/cancel` | `200 OK` | Redis Stream (`CANCEL_BOOKING`) + Lua Seat Release |
| **Phase 4** | **Tickets & Vault** | `GET` | `/api/v1/tickets/{ticket_id}` | `200 OK` | Cache-Aside Read |
| | | `GET` | `/api/v1/tickets/{ticket_id}/pdf` | `307 Redirect`| Presigned MinIO S3 Download URL (15m expiry) |
| **Phase 5** | **Fleet & Admin** | `POST` | `/api/v1/admin/planes` | `201 Created` | Redis Stream (`CREATE_PLANE`) + COPY seat layout |
| | | `GET` | `/api/v1/admin/planes` | `200 OK` | Cache-Aside Read |
| | | `POST` | `/api/v1/flights` | `201 Created` | Redis Stream (`CREATE_FLIGHT`) + COPY flight_seat |
| | | `PATCH` | `/api/v1/flights/{id}/status` | `200 OK` | Redis Stream (`UPDATE_FLIGHT_STATUS`) + Route Eviction |
| | | `GET` | `/api/v1/admin/metrics` | `200 OK` | Direct Memory Stream Lag & PgBouncer Pools |

---

## 3. Techniques Used to Reduce Latency & Improve Performance

### A. Immediate Post-Commit Client Notification Pipeline
* **Zero Client Delay**: In `execute_and_commit_batch()`, the stream worker issues `pipe.set(f"write_res:{id}")`, `pipe.publish(f"write_res_chan:{id}")`, and `pipe.xack(...)` **immediately** once the PostgreSQL transaction commits.
* **Decoupled Background Tasks**: Background tasks (`post_commit_actions` like cache invalidations and Lua state updates) execute asynchronously in parallel via `asyncio.create_task` and `asyncio.gather`, eliminating 70–100ms of client-perceived latency.

### B. Redis Write-Behind Shock Absorber & Atomic Lua Guards
* **Sub-2ms Atomic Seat Reservations**: Hot-spot ticket contention is resolved directly in Redis Hashes (`flight:{id}:seats`) via Lua script (`reserve_seats_atomic`). Conflicting booking requests are rejected in $<2\text{ ms}$ in memory before ever hitting PostgreSQL.
* **Ephemeral Seat Holds with TTL**: Seat holds during checkout use atomic Lua scripts to store reservation state in Redis with a 10-minute TTL, avoiding phantom tickets and database row-lock hold times.
* **High-Batch Redis SCAN**: Route cache pattern eviction uses `count=1000` (up from 100), reducing Redis SCAN network roundtrips by $10\times$.

### C. Dynamic Micro-Batching & Bulk Database Writes
* **Queue-Depth Batch Scaling**: Workers dynamically adapt batch sizes (between `STREAM_WORKER_MIN_BATCH_SIZE` and `STREAM_WORKER_MAX_BATCH_SIZE`). Under low load, writes flush immediately with 0ms accumulation delay; under heavy load, workers batch up to 50 operations within `STREAM_WORKER_BATCH_TIMEOUT_MS` to maximize transaction throughput.
* **Native Binary Streaming (`COPY ... FROM STDIN`)**: Customer creation, flight creation, and aircraft/seat inventory stream directly via asyncpg binary protocol (`copy_records_to_table`), completely bypassing row-by-row `INSERT` overhead.
* **Array Unnesting (`UNNEST`)**: Batch updates for customer profiles and flight statuses execute as single-statement bulk updates using `UNNEST($1::bigint[], ...)`.
* **Deterministic Deadlock Prevention**: Batch updates sort entity IDs in ascending order prior to executing queries, ensuring all concurrent workers acquire row locks in an identical sequence.

### D. Covering Index-Only Scans for Flight Search
* Route queries utilize composite covering index `idx_flight_route_covering` `(departure_airport, arrival_airport, departure_time) INCLUDE (flight_id, flight_number, status, created_at)`, allowing PostgreSQL to serve cache misses via pure Index-Only Scan in **2–5 ms**.

### E. Zero-Disk In-Memory PDF Vault Generation
* **In-Memory Rendering**: Boarding pass PDFs are generated dynamically in memory using ReportLab on an `io.BytesIO` stream without ever writing files to the container filesystem.
* **Decoupled MinIO Upload**: Boarding pass uploads to MinIO are scheduled in worker threads (`asyncio.to_thread`) after transaction commit, adding **0 ms penalty** to transaction commit durations.
* **Presigned S3 URLs via HTTP 307 Redirect**: `GET /tickets/{id}/pdf` issues an authenticated presigned URL (15-minute expiry) and returns an HTTP 307 Temporary Redirect, completely offloading PDF download bandwidth from FastAPI to MinIO.

### F. Native C & Rust Deserialization Acceleration
* **`hiredis` (C-extension)**: Replaces `redis-py`'s pure-Python RESP parser with `_AsyncHiredisParser`, accelerating stream reads and 200-seat hash queries by 78%.
* **`orjson` (Rust-compiled JSON)**: Replaced standard Python `json.loads` / `json.dumps` across `CacheService`, `SeatInventoryService`, and stream workers, yielding lower serialization CPU overhead.

### G. Dynamic Redis Connection Pool & Resilient Event Loop Detection
* Custom `DynamicConnectionPool` scales pool capacity dynamically based on runtime client concurrency (`max(1000, (concurrency + workers) * 3)`).
* Automatic event loop detection in `get_redis_client()` seamlessly resets connection pools across isolated `pytest` async tasks while preserving single-pool performance in production.

---

## 4. Verified Performance Benchmarks

All benchmarks were run against the live PostgreSQL + PgBouncer + Redis Docker environment with a 70% Read / 30% Write workload:

### A. Concurrency Scaling Matrix

| Concurrency | Total Ops | Throughput | Read p50 | Write p50 | Overall p50 | Overall p95 | Overall p99 | Success Rate |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **40 clients** | 5,000 | **1,216.88 ops/sec** | **4.56 ms** | **80.02 ms** | **6.10 ms** | **148.98 ms** | **212.04 ms** | **100.0%** |
| **80 clients** | 5,000 | **1,385.16 ops/sec** | **10.72 ms** | **125.81 ms** | **15.49 ms** | **213.91 ms** | **311.80 ms** | **100.0%** |
| **100 clients** | 5,000 | **1,311.31 ops/sec** | **17.91 ms** | **130.60 ms** | **24.30 ms** | **245.62 ms** | **397.06 ms** | **100.0%** |

### B. Per-Operation Performance Breakdown (40 Clients, 5,000 Ops)

```text
Operation                      | Category | Count  | Latency p50 | Latency p95 | Cache Hit %
-----------------------------------------------------------------------------------------
AUTH_LOGIN                     | READ     |   352  |     6.03 ms |    44.50 ms |       59.1%
READ_AIRPORTS_CATALOG          | READ     |   363  |     3.98 ms |    35.99 ms |      100.0%
READ_CUSTOMER_BOOKINGS         | READ     |   371  |     4.01 ms |    35.53 ms |       97.0%
READ_CUSTOMER_BY_CITY          | READ     |   349  |     3.95 ms |    35.54 ms |      100.0%
READ_CUSTOMER_ME               | READ     |   333  |     4.31 ms |    37.81 ms |       66.7%
READ_CUSTOMER_PROFILE          | READ     |   338  |     4.86 ms |    39.58 ms |       65.1%
READ_FEATURED_ROUTES           | READ     |   348  |     3.97 ms |    36.72 ms |      100.0%
READ_FLIGHT_DETAILS            | READ     |   361  |     4.27 ms |    38.86 ms |       95.8%
READ_FLIGHT_SEATS              | READ     |   361  |     9.19 ms |    45.11 ms |       96.7%
READ_ROUTE_SEARCH              | READ     |   358  |     4.31 ms |    37.07 ms |       84.9%
WRITE_BOOK_SEATS               | WRITE    |   228  |     3.64 ms |    30.19 ms |         N/A
WRITE_CANCEL_BOOKING           | WRITE    |   244  |     3.70 ms |    35.73 ms |         N/A
WRITE_CREATE_CUSTOMER          | WRITE    |   247  |   113.47 ms |   198.76 ms |         N/A
WRITE_CREATE_FLIGHT            | WRITE    |   242  |   129.10 ms |   222.58 ms |         N/A
WRITE_UPDATE_CUSTOMER          | WRITE    |   241  |   110.28 ms |   209.26 ms |         N/A
WRITE_UPDATE_FLIGHT_STATUS     | WRITE    |   264  |   116.22 ms |   210.02 ms |         N/A
```

---

## 5. Quick Start & Execution

### Prerequisites
* Docker & Docker Compose
* Python 3.12+ and `uv` package manager

### 1. Start Infrastructure & Containers
```bash
cd backend

# Build and start all 6 containers (api, stream-worker, minio, pgbouncer, postgres, redis)
docker compose up -d --build

# Verify container health
curl -s http://localhost:8000/health
```

### 2. Local Environment Setup & Migrations
```bash
cp .env.example .env
uv sync

# Run database migrations
uv run alembic upgrade head
```

### 3. Run Test Suite
```bash
# Execute unit and integration tests (37 passed across all 5 phases in ~25s)
uv run pytest tests/ -v
```

### 4. Run Concurrent Benchmarks
```bash
# High-concurrency mixed read/write fuzzing benchmark
uv run python scripts/fuzz_test_redis.py --operations 5000 --concurrency 40 --write-ratio 0.3 --stream-workers 0

# Stream queue wait time & transaction duration benchmark
uv run python scripts/test_stream_timing.py --operations 200 --concurrency 20
```

### 5. Document Vault Verification (MinIO)
Check stored ticket PDF documents:
```bash
uv run python -c "
from app.services.minio_storage import minio_service
from app.core.config import settings
print('Bucket exists:', minio_service.client.bucket_exists(settings.MINIO_BUCKET_NAME))
for obj in minio_service.client.list_objects(settings.MINIO_BUCKET_NAME, recursive=True):
    print(f' - {obj.object_name} ({obj.size} bytes)')
"
```
MinIO Web Console is available at `http://localhost:9001` (user: `minioadmin`, pass: `minioadmin`).
