# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-05T05:39:58.650487+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 8 Client Workers | 2 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 60 (100.0% success, 0 errors)

- **Total Duration:** 0.41 seconds

- **Combined Throughput:** **145.87 ops/sec**

- **Read Throughput:** 94.82 reads/sec

- **Write Throughput:** 51.05 writes/sec

- **Redis Cache Hit Ratio:** **64.1%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 60 | 48.88 | 9.87 | 174.6 | 298.39 | 338.54 | 370.65 |
| **Reads (Cache-Aside)** | 39 | 13.3 | 6.84 | 37.62 | 60.95 | 61.2 | 61.32 |
| **Writes (Redis Stream)** | 21 | 114.97 | 35.76 | 300.11 | 316.23 | 359.77 | 370.65 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `READ_CUSTOMER_BY_CITY` | READ | 13 | 100.0% | 2.34 | 60.97 | 61.0 | 53.85% |
| `READ_CUSTOMER_PROFILE` | READ | 9 | 100.0% | 3.12 | 18.34 | 21.49 | 88.89% |
| `READ_FLIGHT_DETAILS` | READ | 3 | 100.0% | 0.38 | 0.38 | 0.38 | 100.0% |
| `READ_FLIGHT_SEATS` | READ | 8 | 100.0% | 14.07 | 53.79 | 59.81 | 87.5% |
| `READ_ROUTE_SEARCH` | READ | 6 | 100.0% | 14.56 | 28.33 | 30.8 | 0.0% |
| `WRITE_BOOK_SEATS` | WRITE | 2 | 100.0% | 2.02 | 3.05 | 3.14 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 7 | 100.0% | 0.77 | 20.1 | 21.44 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 7 | 100.0% | 163.51 | 310.85 | 315.15 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 5 | 100.0% | 295.54 | 356.54 | 367.83 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 0.4% | 0.4% | 9.58 MB | 9.58 MB |
| `pgbouncer` | 0.03% | 0.03% | 10.17 MB | 10.17 MB |
| `postgres` | 19.14% | 9.58% | 173.1 MB | 173.1 MB |
| Benchmark Client | 93.1% | 59.27% | 146.77 MB | 146.4 MB |

## 5. Redis State & Memory


- **Used Memory:** 1.83 MB (Peak: 3.94 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 12
