# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-06T14:03:49.232395+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 90 Client Workers | 5 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 10,000 (100.0% success, 0 errors)

- **Total Duration:** 21.8 seconds

- **Combined Throughput:** **458.71 ops/sec**

- **Read Throughput:** 324.08 reads/sec

- **Write Throughput:** 134.63 writes/sec

- **Redis Cache Hit Ratio:** **80.48%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 10,000 | 192.53 | 12.3 | 766.33 | 1475.37 | 2173.36 | 2488.83 |
| **Reads (Cache-Aside)** | 7,065 | 17.2 | 9.26 | 41.63 | 55.33 | 96.59 | 297.36 |
| **Writes (Redis Stream)** | 2,935 | 614.58 | 269.11 | 1904.42 | 2070.22 | 2363.95 | 2488.83 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `READ_CUSTOMER_BY_CITY` | READ | 1388 | 100.0% | 4.71 | 44.03 | 70.54 | 100.0% |
| `READ_CUSTOMER_PROFILE` | READ | 1391 | 100.0% | 4.6 | 43.88 | 84.07 | 98.85% |
| `READ_FLIGHT_DETAILS` | READ | 1405 | 100.0% | 4.81 | 46.47 | 94.73 | 98.43% |
| `READ_FLIGHT_SEATS` | READ | 1433 | 100.0% | 19.89 | 73.47 | 96.8 | 99.02% |
| `READ_ROUTE_SEARCH` | READ | 1448 | 100.0% | 20.31 | 75.27 | 104.98 | 8.36% |
| `WRITE_BOOK_SEATS` | WRITE | 607 | 100.0% | 4.64 | 47.43 | 90.35 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 599 | 100.0% | 4.34 | 46.0 | 76.95 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 604 | 100.0% | 903.98 | 2152.76 | 2441.87 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 569 | 100.0% | 872.7 | 2233.59 | 2389.58 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 556 | 100.0% | 851.55 | 2233.59 | 2411.72 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 27.25% | 20.57% | 15.91 MB | 14.67 MB |
| `pgbouncer` | 11.23% | 7.2% | 12.11 MB | 11.22 MB |
| `postgres` | 73.96% | 59.64% | 983.7 MB | 973.55 MB |
| Benchmark Client | 140.3% | 121.09% | 93.05 MB | 89.71 MB |

## 5. Redis State & Memory


- **Used Memory:** 8.46 MB (Peak: 23.32 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 4572
