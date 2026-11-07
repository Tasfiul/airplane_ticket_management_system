# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T15:45:52.080568+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 8 Client Workers | 2 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 60 (100.0% success, 0 errors)

- **Total Duration:** 0.13 seconds

- **Combined Throughput:** **461.17 ops/sec**

- **Read Throughput:** 292.08 reads/sec

- **Write Throughput:** 169.1 writes/sec

- **Redis Cache Hit Ratio:** **92.11%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 60 | 12.04 | 1.66 | 42.06 | 50.92 | 78.61 | 84.94 |
| **Reads (Cache-Aside)** | 38 | 4.7 | 1.47 | 14.55 | 16.07 | 20.02 | 22.25 |
| **Writes (Redis Stream)** | 22 | 24.74 | 15.49 | 67.5 | 73.97 | 82.69 | 84.94 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 2 | 100.0% | 8.71 | 15.49 | 16.09 | 100.0% |
| `READ_AIRPORTS_CATALOG` | READ | 6 | 100.0% | 0.85 | 1.49 | 1.59 | 100.0% |
| `READ_CUSTOMER_BY_CITY` | READ | 8 | 100.0% | 2.73 | 12.76 | 14.01 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 6 | 100.0% | 2.54 | 18.82 | 21.56 | 66.67% |
| `READ_CUSTOMER_PROFILE` | READ | 2 | 100.0% | 1.27 | 1.31 | 1.32 | 100.0% |
| `READ_FEATURED_ROUTES` | READ | 1 | 100.0% | 14.08 | 14.08 | 14.08 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 5 | 100.0% | 1.29 | 4.92 | 5.01 | 100.0% |
| `READ_FLIGHT_SEATS` | READ | 5 | 100.0% | 4.68 | 15.85 | 16.0 | 100.0% |
| `READ_ROUTE_SEARCH` | READ | 3 | 100.0% | 4.55 | 7.75 | 8.03 | 66.67% |
| `WRITE_BOOK_SEATS` | WRITE | 5 | 100.0% | 1.1 | 1.39 | 1.41 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 6 | 100.0% | 0.53 | 6.87 | 8.5 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 2 | 100.0% | 25.33 | 28.26 | 28.52 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 5 | 100.0% | 69.45 | 82.8 | 84.51 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 2 | 100.0% | 35.15 | 41.78 | 42.37 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 2 | 100.0% | 41.3 | 41.94 | 42.0 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 0.47% | 0.47% | 14.33 MB | 14.33 MB |
| `pgbouncer` | 0.02% | 0.02% | 10.75 MB | 10.75 MB |
| `postgres` | 0.2% | 0.2% | 1278.98 MB | 1278.98 MB |
| Benchmark Client | 0.0% | 0.0% | 151.45 MB | 151.45 MB |

## 5. Redis State & Memory


- **Used Memory:** 3.43 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 1004
