# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T12:16:34.535532+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 8 Client Workers | 2 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 60 (100.0% success, 0 errors)

- **Total Duration:** 0.12 seconds

- **Combined Throughput:** **502.02 ops/sec**

- **Read Throughput:** 359.78 reads/sec

- **Write Throughput:** 142.24 writes/sec

- **Redis Cache Hit Ratio:** **95.35%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 60 | 10.61 | 1.97 | 33.22 | 44.32 | 57.24 | 70.61 |
| **Reads (Cache-Aside)** | 43 | 2.72 | 1.34 | 6.89 | 10.88 | 13.64 | 14.04 |
| **Writes (Redis Stream)** | 17 | 30.55 | 30.33 | 47.28 | 52.48 | 66.98 | 70.61 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 6 | 100.0% | 1.42 | 6.78 | 6.94 | 83.33% |
| `READ_AIRPORTS_CATALOG` | READ | 7 | 100.0% | 1.95 | 7.26 | 8.86 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 5 | 100.0% | 0.97 | 3.21 | 3.64 | 80.0% |
| `READ_CUSTOMER_BY_CITY` | READ | 1 | 100.0% | 1.21 | 1.21 | 1.21 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 4 | 100.0% | 1.05 | 5.78 | 6.4 | 100.0% |
| `READ_CUSTOMER_PROFILE` | READ | 3 | 100.0% | 0.92 | 1.72 | 1.79 | 100.0% |
| `READ_FEATURED_ROUTES` | READ | 4 | 100.0% | 1.62 | 9.7 | 10.79 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 4 | 100.0% | 0.77 | 1.61 | 1.7 | 100.0% |
| `READ_FLIGHT_SEATS` | READ | 5 | 100.0% | 2.61 | 13.85 | 14.0 | 100.0% |
| `READ_ROUTE_SEARCH` | READ | 4 | 100.0% | 1.32 | 1.92 | 1.99 | 100.0% |
| `WRITE_BOOK_SEATS` | WRITE | 1 | 100.0% | 8.16 | 8.16 | 8.16 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 3 | 100.0% | 5.9 | 6.93 | 7.02 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 3 | 100.0% | 30.33 | 46.19 | 47.6 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 3 | 100.0% | 43.21 | 67.87 | 70.06 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 3 | 100.0% | 29.59 | 42.73 | 43.89 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 4 | 100.0% | 31.97 | 44.66 | 46.4 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 0.47% | 0.47% | 15.13 MB | 15.13 MB |
| `pgbouncer` | 0.01% | 0.01% | 8.55 MB | 8.55 MB |
| `postgres` | 0.1% | 0.1% | 1327.1 MB | 1327.1 MB |
| Benchmark Client | 0.0% | 0.0% | 136.49 MB | 136.49 MB |

## 5. Redis State & Memory


- **Used Memory:** 3.94 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 1346
