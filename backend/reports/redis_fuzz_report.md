# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T16:39:35.111269+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 40 Client Workers | 0 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 5,000 (100.0% success, 0 errors)

- **Total Duration:** 3.23 seconds

- **Combined Throughput:** **1548.33 ops/sec**

- **Read Throughput:** 1206.15 reads/sec

- **Write Throughput:** 342.18 writes/sec

- **Redis Cache Hit Ratio:** **90.01%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 5,000 | 24.52 | 5.19 | 92.75 | 132.66 | 182.45 | 266.4 |
| **Reads (Cache-Aside)** | 3,895 | 8.5 | 3.96 | 24.55 | 33.39 | 54.49 | 74.73 |
| **Writes (Redis Stream)** | 1,105 | 80.99 | 82.24 | 162.56 | 181.28 | 218.15 | 266.4 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 382 | 100.0% | 5.0 | 34.94 | 58.55 | 67.8% |
| `READ_AIRPORTS_CATALOG` | READ | 392 | 100.0% | 3.38 | 29.55 | 38.7 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 398 | 100.0% | 3.57 | 30.33 | 41.94 | 98.99% |
| `READ_CUSTOMER_BY_CITY` | READ | 402 | 100.0% | 3.26 | 29.58 | 47.64 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 409 | 100.0% | 4.04 | 34.94 | 59.74 | 74.57% |
| `READ_CUSTOMER_PROFILE` | READ | 386 | 100.0% | 4.26 | 28.76 | 52.91 | 76.17% |
| `READ_FEATURED_ROUTES` | READ | 374 | 100.0% | 2.98 | 29.23 | 41.98 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 358 | 100.0% | 3.32 | 30.65 | 56.58 | 98.6% |
| `READ_FLIGHT_SEATS` | READ | 413 | 100.0% | 7.2 | 42.18 | 53.26 | 99.03% |
| `READ_ROUTE_SEARCH` | READ | 381 | 100.0% | 4.21 | 37.89 | 55.56 | 85.04% |
| `WRITE_AUTH_REGISTER` | WRITE | 141 | 100.0% | 119.77 | 194.82 | 229.03 | N/A |
| `WRITE_BOOK_SEATS` | WRITE | 146 | 100.0% | 3.59 | 38.71 | 51.96 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 145 | 100.0% | 2.77 | 25.52 | 38.76 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 172 | 100.0% | 95.84 | 178.14 | 218.33 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 159 | 100.0% | 125.4 | 195.86 | 243.27 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 164 | 100.0% | 105.3 | 182.04 | 206.15 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 178 | 100.0% | 96.89 | 183.28 | 251.21 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 40.08% | 20.29% | 19.16 MB | 18.95 MB |
| `pgbouncer` | 25.36% | 14.15% | 10.8 MB | 10.76 MB |
| `postgres` | 239.76% | 119.88% | 1246.21 MB | 1245.7 MB |
| Benchmark Client | 123.3% | 97.97% | 103.97 MB | 103.05 MB |

## 5. Redis State & Memory


- **Used Memory:** 7.04 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 4069
