# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T13:58:17.115200+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 40 Client Workers | 0 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 5,000 (100.0% success, 0 errors)

- **Total Duration:** 4.08 seconds

- **Combined Throughput:** **1225.72 ops/sec**

- **Read Throughput:** 849.67 reads/sec

- **Write Throughput:** 376.05 writes/sec

- **Redis Cache Hit Ratio:** **85.89%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 5,000 | 31.41 | 5.73 | 119.67 | 147.57 | 217.26 | 312.22 |
| **Reads (Cache-Aside)** | 3,466 | 8.75 | 3.9 | 24.82 | 32.72 | 49.87 | 108.32 |
| **Writes (Redis Stream)** | 1,534 | 82.61 | 82.5 | 165.14 | 200.85 | 266.86 | 312.22 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 334 | 100.0% | 5.83 | 38.33 | 57.56 | 53.59% |
| `READ_AIRPORTS_CATALOG` | READ | 345 | 100.0% | 3.12 | 29.66 | 44.16 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 346 | 100.0% | 3.21 | 27.51 | 42.84 | 95.95% |
| `READ_CUSTOMER_BY_CITY` | READ | 327 | 100.0% | 3.39 | 27.68 | 40.37 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 329 | 100.0% | 5.8 | 36.27 | 57.34 | 66.26% |
| `READ_CUSTOMER_PROFILE` | READ | 358 | 100.0% | 4.12 | 34.89 | 48.94 | 64.8% |
| `READ_FEATURED_ROUTES` | READ | 350 | 100.0% | 3.01 | 26.03 | 36.17 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 351 | 100.0% | 3.27 | 26.45 | 43.01 | 96.3% |
| `READ_FLIGHT_SEATS` | READ | 371 | 100.0% | 7.1 | 33.19 | 57.88 | 96.5% |
| `READ_ROUTE_SEARCH` | READ | 355 | 100.0% | 3.89 | 34.0 | 45.69 | 83.94% |
| `WRITE_BOOK_SEATS` | WRITE | 238 | 100.0% | 2.87 | 23.75 | 36.77 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 243 | 100.0% | 3.32 | 27.43 | 44.92 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 272 | 100.0% | 115.65 | 216.84 | 264.25 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 245 | 100.0% | 120.52 | 214.72 | 270.97 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 283 | 100.0% | 115.8 | 221.0 | 276.63 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 253 | 100.0% | 115.74 | 207.54 | 263.74 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 34.78% | 12.4% | 15.75 MB | 15.43 MB |
| `pgbouncer` | 23.32% | 7.87% | 11.84 MB | 11.71 MB |
| `postgres` | 253.14% | 86.92% | 1357.82 MB | 1353.05 MB |
| Benchmark Client | 112.3% | 86.66% | 105.13 MB | 104.16 MB |

## 5. Redis State & Memory


- **Used Memory:** 4.98 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 1071
