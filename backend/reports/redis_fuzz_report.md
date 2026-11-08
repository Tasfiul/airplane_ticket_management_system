# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-08T07:45:18.767559+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 40 Client Workers | 0 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 5,000 (100.0% success, 0 errors)

- **Total Duration:** 4.0 seconds

- **Combined Throughput:** **1251.16 ops/sec**

- **Read Throughput:** 871.06 reads/sec

- **Write Throughput:** 380.1 writes/sec

- **Redis Cache Hit Ratio:** **88.05%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 5,000 | 31.03 | 5.97 | 112.07 | 137.61 | 206.6 | 288.17 |
| **Reads (Cache-Aside)** | 3,481 | 9.5 | 3.93 | 27.47 | 36.3 | 54.32 | 72.7 |
| **Writes (Redis Stream)** | 1,519 | 80.36 | 85.46 | 157.92 | 185.33 | 266.16 | 288.17 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 337 | 100.0% | 5.5 | 43.11 | 56.56 | 59.35% |
| `READ_AIRPORTS_CATALOG` | READ | 340 | 100.0% | 3.52 | 32.05 | 44.46 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 352 | 100.0% | 3.12 | 33.67 | 44.59 | 98.01% |
| `READ_CUSTOMER_BY_CITY` | READ | 342 | 100.0% | 3.52 | 31.96 | 46.03 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 338 | 100.0% | 3.67 | 38.14 | 55.97 | 70.12% |
| `READ_CUSTOMER_PROFILE` | READ | 395 | 100.0% | 4.61 | 37.1 | 57.7 | 71.9% |
| `READ_FEATURED_ROUTES` | READ | 337 | 100.0% | 3.28 | 31.09 | 44.7 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 335 | 100.0% | 3.68 | 38.83 | 45.32 | 98.21% |
| `READ_FLIGHT_SEATS` | READ | 339 | 100.0% | 7.15 | 40.79 | 51.26 | 98.23% |
| `READ_ROUTE_SEARCH` | READ | 366 | 100.0% | 4.08 | 36.54 | 63.91 | 86.89% |
| `WRITE_AUTH_REGISTER` | WRITE | 212 | 100.0% | 101.67 | 194.85 | 264.79 | N/A |
| `WRITE_BOOK_SEATS` | WRITE | 237 | 100.0% | 3.21 | 30.8 | 42.56 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 190 | 100.0% | 3.21 | 33.93 | 42.12 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 227 | 100.0% | 111.43 | 217.2 | 277.85 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 209 | 100.0% | 114.84 | 208.74 | 278.05 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 218 | 100.0% | 99.23 | 185.23 | 264.24 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 226 | 100.0% | 105.81 | 188.32 | 240.61 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 38.17% | 13.54% | 15.32 MB | 15.19 MB |
| `pgbouncer` | 22.5% | 8.94% | 10.36 MB | 10.31 MB |
| `postgres` | 258.7% | 88.75% | 1009.0 MB | 1008.33 MB |
| Benchmark Client | 117.9% | 88.42% | 104.29 MB | 103.48 MB |

## 5. Redis State & Memory


- **Used Memory:** 6.39 MB (Peak: 15.4 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 1876
