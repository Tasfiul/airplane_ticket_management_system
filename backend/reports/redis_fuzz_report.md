# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T08:58:26.212918+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 40 Client Workers | 4 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 5,000 (100.0% success, 0 errors)

- **Total Duration:** 4.4 seconds

- **Combined Throughput:** **1135.36 ops/sec**

- **Read Throughput:** 807.46 reads/sec

- **Write Throughput:** 327.89 writes/sec

- **Redis Cache Hit Ratio:** **87.23%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 5,000 | 34.21 | 7.0 | 119.6 | 166.87 | 283.36 | 422.66 |
| **Reads (Cache-Aside)** | 3,556 | 11.11 | 5.3 | 29.67 | 38.76 | 57.2 | 116.47 |
| **Writes (Redis Stream)** | 1,444 | 91.08 | 72.44 | 195.86 | 265.02 | 344.34 | 422.66 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 329 | 100.0% | 10.0 | 42.71 | 60.16 | 55.93% |
| `READ_AIRPORTS_CATALOG` | READ | 388 | 100.0% | 4.43 | 30.14 | 42.09 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 362 | 100.0% | 4.12 | 33.83 | 42.27 | 96.96% |
| `READ_CUSTOMER_BY_CITY` | READ | 366 | 100.0% | 4.25 | 36.38 | 50.19 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 337 | 100.0% | 5.69 | 36.45 | 56.84 | 70.62% |
| `READ_CUSTOMER_PROFILE` | READ | 354 | 100.0% | 6.16 | 44.79 | 63.05 | 66.1% |
| `READ_FEATURED_ROUTES` | READ | 347 | 100.0% | 4.15 | 35.96 | 47.52 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 371 | 100.0% | 4.69 | 34.94 | 47.34 | 96.23% |
| `READ_FLIGHT_SEATS` | READ | 339 | 100.0% | 10.71 | 44.79 | 57.6 | 96.46% |
| `READ_ROUTE_SEARCH` | READ | 363 | 100.0% | 5.62 | 41.51 | 63.59 | 85.4% |
| `WRITE_BOOK_SEATS` | WRITE | 229 | 100.0% | 4.78 | 39.19 | 45.43 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 211 | 100.0% | 4.82 | 36.25 | 44.58 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 253 | 100.0% | 109.32 | 285.35 | 357.62 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 252 | 100.0% | 137.75 | 292.14 | 357.14 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 251 | 100.0% | 115.55 | 263.93 | 344.84 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 248 | 100.0% | 117.34 | 275.81 | 316.3 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 34.85% | 9.9% | 14.57 MB | 14.29 MB |
| `pgbouncer` | 28.63% | 7.68% | 11.66 MB | 11.63 MB |
| `postgres` | 395.28% | 103.23% | 1551.36 MB | 1547.78 MB |
| Benchmark Client | 125.1% | 111.76% | 95.43 MB | 94.38 MB |

## 5. Redis State & Memory


- **Used Memory:** 4.4 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 1004
