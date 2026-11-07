# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T10:02:41.831985+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 40 Client Workers | 4 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 5,000 (100.0% success, 0 errors)

- **Total Duration:** 4.4 seconds

- **Combined Throughput:** **1136.26 ops/sec**

- **Read Throughput:** 798.79 reads/sec

- **Write Throughput:** 337.47 writes/sec

- **Redis Cache Hit Ratio:** **86.91%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 5,000 | 34.04 | 8.11 | 107.7 | 164.39 | 241.7 | 337.6 |
| **Reads (Cache-Aside)** | 3,515 | 13.13 | 6.07 | 36.07 | 45.12 | 64.63 | 81.31 |
| **Writes (Redis Stream)** | 1,485 | 83.54 | 64.85 | 193.43 | 228.81 | 282.5 | 337.6 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 355 | 100.0% | 12.63 | 50.8 | 67.18 | 56.62% |
| `READ_AIRPORTS_CATALOG` | READ | 346 | 100.0% | 4.8 | 40.11 | 57.4 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 360 | 100.0% | 4.88 | 39.54 | 48.33 | 96.67% |
| `READ_CUSTOMER_BY_CITY` | READ | 355 | 100.0% | 5.22 | 37.27 | 50.43 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 357 | 100.0% | 8.59 | 50.66 | 65.23 | 66.39% |
| `READ_CUSTOMER_PROFILE` | READ | 342 | 100.0% | 6.36 | 46.17 | 69.01 | 70.47% |
| `READ_FEATURED_ROUTES` | READ | 344 | 100.0% | 5.28 | 44.55 | 58.46 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 346 | 100.0% | 5.14 | 37.75 | 64.1 | 97.11% |
| `READ_FLIGHT_SEATS` | READ | 341 | 100.0% | 11.77 | 50.71 | 69.46 | 97.36% |
| `READ_ROUTE_SEARCH` | READ | 369 | 100.0% | 5.76 | 43.72 | 66.34 | 85.37% |
| `WRITE_BOOK_SEATS` | WRITE | 243 | 100.0% | 4.91 | 39.62 | 52.94 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 241 | 100.0% | 5.0 | 42.3 | 65.53 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 270 | 100.0% | 96.95 | 231.92 | 268.78 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 236 | 100.0% | 122.38 | 247.26 | 315.17 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 237 | 100.0% | 106.3 | 227.55 | 292.05 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 258 | 100.0% | 106.55 | 246.26 | 291.56 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 40.94% | 11.34% | 17.32 MB | 16.86 MB |
| `pgbouncer` | 31.37% | 8.34% | 12.31 MB | 12.12 MB |
| `postgres` | 370.43% | 96.86% | 1579.01 MB | 1574.66 MB |
| Benchmark Client | 126.4% | 112.14% | 105.44 MB | 104.31 MB |

## 5. Redis State & Memory


- **Used Memory:** 4.69 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 2031
