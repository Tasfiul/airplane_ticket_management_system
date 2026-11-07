# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T11:08:52.456731+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 40 Client Workers | 0 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 5,000 (100.0% success, 0 errors)

- **Total Duration:** 4.09 seconds

- **Combined Throughput:** **1222.85 ops/sec**

- **Read Throughput:** 857.22 reads/sec

- **Write Throughput:** 365.63 writes/sec

- **Redis Cache Hit Ratio:** **86.22%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 5,000 | 31.28 | 5.22 | 116.41 | 152.51 | 229.86 | 369.41 |
| **Reads (Cache-Aside)** | 3,505 | 8.87 | 3.83 | 24.89 | 35.32 | 56.6 | 102.86 |
| **Writes (Redis Stream)** | 1,495 | 83.83 | 83.09 | 175.01 | 201.88 | 309.86 | 369.41 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 359 | 100.0% | 6.42 | 44.07 | 61.94 | 56.27% |
| `READ_AIRPORTS_CATALOG` | READ | 335 | 100.0% | 3.23 | 29.19 | 43.73 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 343 | 100.0% | 3.36 | 31.07 | 46.97 | 99.13% |
| `READ_CUSTOMER_BY_CITY` | READ | 340 | 100.0% | 3.24 | 27.33 | 41.5 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 355 | 100.0% | 4.38 | 38.78 | 61.14 | 65.63% |
| `READ_CUSTOMER_PROFILE` | READ | 354 | 100.0% | 4.84 | 42.94 | 65.3 | 65.54% |
| `READ_FEATURED_ROUTES` | READ | 375 | 100.0% | 3.13 | 28.96 | 45.96 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 347 | 100.0% | 2.8 | 30.79 | 50.63 | 97.69% |
| `READ_FLIGHT_SEATS` | READ | 339 | 100.0% | 6.86 | 37.81 | 54.11 | 97.35% |
| `READ_ROUTE_SEARCH` | READ | 358 | 100.0% | 3.44 | 35.55 | 65.8 | 82.68% |
| `WRITE_BOOK_SEATS` | WRITE | 233 | 100.0% | 3.12 | 31.53 | 47.65 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 240 | 100.0% | 2.95 | 34.21 | 46.32 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 258 | 100.0% | 113.15 | 205.21 | 338.73 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 261 | 100.0% | 124.8 | 261.17 | 363.27 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 251 | 100.0% | 112.6 | 199.58 | 282.78 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 252 | 100.0% | 115.2 | 226.63 | 269.93 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 40.42% | 15.53% | 16.87 MB | 16.64 MB |
| `pgbouncer` | 26.43% | 9.17% | 12.16 MB | 11.98 MB |
| `postgres` | 257.28% | 88.47% | 1718.27 MB | 1715.88 MB |
| Benchmark Client | 121.0% | 89.57% | 104.03 MB | 103.26 MB |

## 5. Redis State & Memory


- **Used Memory:** 4.78 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 2016
