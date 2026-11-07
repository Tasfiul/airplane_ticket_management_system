# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T10:48:09.224082+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 40 Client Workers | 0 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 5,000 (100.0% success, 0 errors)

- **Total Duration:** 5.5 seconds

- **Combined Throughput:** **909.36 ops/sec**

- **Read Throughput:** 634.55 reads/sec

- **Write Throughput:** 274.81 writes/sec

- **Redis Cache Hit Ratio:** **87.19%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 5,000 | 42.86 | 3.42 | 188.36 | 259.14 | 390.09 | 486.65 |
| **Reads (Cache-Aside)** | 3,489 | 6.69 | 2.45 | 17.64 | 27.64 | 53.62 | 88.12 |
| **Writes (Redis Stream)** | 1,511 | 126.38 | 108.57 | 289.53 | 342.69 | 430.75 | 486.65 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 341 | 100.0% | 3.15 | 38.42 | 62.69 | 55.13% |
| `READ_AIRPORTS_CATALOG` | READ | 339 | 100.0% | 2.1 | 25.45 | 43.38 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 354 | 100.0% | 2.15 | 18.54 | 33.56 | 98.59% |
| `READ_CUSTOMER_BY_CITY` | READ | 353 | 100.0% | 2.11 | 24.99 | 48.2 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 352 | 100.0% | 3.72 | 34.28 | 60.15 | 67.05% |
| `READ_CUSTOMER_PROFILE` | READ | 350 | 100.0% | 2.42 | 34.31 | 64.46 | 67.71% |
| `READ_FEATURED_ROUTES` | READ | 362 | 100.0% | 2.11 | 24.29 | 43.11 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 329 | 100.0% | 2.1 | 22.5 | 39.61 | 98.78% |
| `READ_FLIGHT_SEATS` | READ | 357 | 100.0% | 4.41 | 29.73 | 43.62 | 99.72% |
| `READ_ROUTE_SEARCH` | READ | 352 | 100.0% | 2.37 | 23.24 | 43.6 | 84.38% |
| `WRITE_BOOK_SEATS` | WRITE | 266 | 100.0% | 1.95 | 19.49 | 32.12 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 286 | 100.0% | 1.97 | 27.45 | 41.01 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 209 | 100.0% | 184.59 | 324.03 | 400.74 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 249 | 100.0% | 220.59 | 406.81 | 467.54 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 251 | 100.0% | 177.47 | 343.98 | 424.36 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 250 | 100.0% | 201.47 | 396.97 | 448.23 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 46.27% | 23.42% | 28.17 MB | 27.85 MB |
| `pgbouncer` | 14.65% | 7.59% | 12.16 MB | 12.14 MB |
| `postgres` | 165.23% | 84.5% | 1661.95 MB | 1660.59 MB |
| Benchmark Client | 114.6% | 66.55% | 103.94 MB | 103.08 MB |

## 5. Redis State & Memory


- **Used Memory:** 15.43 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 22863
