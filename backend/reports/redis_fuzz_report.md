# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T15:16:33.440184+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 40 Client Workers | 0 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 5,000 (100.0% success, 0 errors)

- **Total Duration:** 3.98 seconds

- **Combined Throughput:** **1255.92 ops/sec**

- **Read Throughput:** 875.38 reads/sec

- **Write Throughput:** 380.54 writes/sec

- **Redis Cache Hit Ratio:** **87.0%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 5,000 | 30.83 | 5.23 | 119.11 | 150.29 | 225.21 | 268.75 |
| **Reads (Cache-Aside)** | 3,485 | 8.54 | 3.73 | 24.16 | 31.44 | 50.52 | 131.18 |
| **Writes (Redis Stream)** | 1,515 | 82.11 | 82.03 | 174.07 | 211.38 | 246.04 | 268.75 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 338 | 100.0% | 4.66 | 42.5 | 55.9 | 58.58% |
| `READ_AIRPORTS_CATALOG` | READ | 353 | 100.0% | 2.93 | 27.35 | 41.86 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 327 | 100.0% | 3.51 | 28.05 | 41.51 | 96.64% |
| `READ_CUSTOMER_BY_CITY` | READ | 339 | 100.0% | 2.97 | 27.64 | 37.66 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 385 | 100.0% | 4.5 | 40.32 | 54.22 | 68.31% |
| `READ_CUSTOMER_PROFILE` | READ | 360 | 100.0% | 3.47 | 26.34 | 58.34 | 73.06% |
| `READ_FEATURED_ROUTES` | READ | 339 | 100.0% | 3.18 | 28.13 | 42.24 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 328 | 100.0% | 3.36 | 28.08 | 38.99 | 95.73% |
| `READ_FLIGHT_SEATS` | READ | 385 | 100.0% | 6.62 | 37.73 | 50.8 | 95.58% |
| `READ_ROUTE_SEARCH` | READ | 331 | 100.0% | 3.77 | 30.46 | 47.58 | 84.29% |
| `WRITE_BOOK_SEATS` | WRITE | 270 | 100.0% | 2.95 | 29.2 | 73.25 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 229 | 100.0% | 2.87 | 29.48 | 40.54 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 267 | 100.0% | 114.7 | 214.05 | 247.07 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 243 | 100.0% | 131.4 | 233.89 | 251.27 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 234 | 100.0% | 114.66 | 227.25 | 253.6 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 272 | 100.0% | 110.98 | 223.94 | 251.99 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 32.85% | 11.72% | 17.21 MB | 16.15 MB |
| `pgbouncer` | 21.27% | 7.21% | 11.08 MB | 10.88 MB |
| `postgres` | 285.6% | 97.49% | 1226.75 MB | 1224.7 MB |
| Benchmark Client | 112.2% | 86.38% | 104.42 MB | 103.6 MB |

## 5. Redis State & Memory


- **Used Memory:** 5.19 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 1034
