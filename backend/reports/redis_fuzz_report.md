# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T15:41:34.519953+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 40 Client Workers | 0 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 5,000 (100.0% success, 0 errors)

- **Total Duration:** 3.82 seconds

- **Combined Throughput:** **1308.99 ops/sec**

- **Read Throughput:** 931.47 reads/sec

- **Write Throughput:** 377.51 writes/sec

- **Redis Cache Hit Ratio:** **86.62%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 5,000 | 29.53 | 4.6 | 119.67 | 159.04 | 217.08 | 306.53 |
| **Reads (Cache-Aside)** | 3,558 | 8.16 | 3.44 | 24.44 | 32.28 | 49.28 | 87.94 |
| **Writes (Redis Stream)** | 1,442 | 82.26 | 72.9 | 173.56 | 201.4 | 260.37 | 306.53 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 344 | 100.0% | 4.61 | 42.83 | 51.02 | 55.81% |
| `READ_AIRPORTS_CATALOG` | READ | 316 | 100.0% | 2.97 | 28.26 | 47.12 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 386 | 100.0% | 2.84 | 29.45 | 36.76 | 97.67% |
| `READ_CUSTOMER_BY_CITY` | READ | 372 | 100.0% | 2.81 | 27.39 | 44.64 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 335 | 100.0% | 3.88 | 41.4 | 51.36 | 66.27% |
| `READ_CUSTOMER_PROFILE` | READ | 356 | 100.0% | 3.88 | 41.48 | 48.24 | 67.42% |
| `READ_FEATURED_ROUTES` | READ | 365 | 100.0% | 2.66 | 26.48 | 39.07 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 371 | 100.0% | 3.03 | 27.18 | 37.35 | 97.04% |
| `READ_FLIGHT_SEATS` | READ | 360 | 100.0% | 7.04 | 34.3 | 49.48 | 97.22% |
| `READ_ROUTE_SEARCH` | READ | 353 | 100.0% | 3.5 | 27.42 | 58.59 | 81.59% |
| `WRITE_BOOK_SEATS` | WRITE | 243 | 100.0% | 2.88 | 30.84 | 42.32 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 230 | 100.0% | 2.94 | 29.39 | 52.43 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 248 | 100.0% | 108.5 | 209.77 | 264.69 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 241 | 100.0% | 136.29 | 220.19 | 262.38 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 247 | 100.0% | 125.13 | 212.61 | 267.87 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 233 | 100.0% | 108.86 | 222.21 | 265.54 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 33.36% | 11.88% | 16.34 MB | 15.92 MB |
| `pgbouncer` | 24.79% | 8.38% | 14.9 MB | 12.17 MB |
| `postgres` | 292.6% | 99.76% | 1277.95 MB | 1270.1 MB |
| Benchmark Client | 122.2% | 86.6% | 104.46 MB | 103.45 MB |

## 5. Redis State & Memory


- **Used Memory:** 4.96 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 984
