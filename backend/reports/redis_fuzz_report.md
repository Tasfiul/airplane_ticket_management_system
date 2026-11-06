# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-06T13:36:59.203268+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 90 Client Workers | 5 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 10,000 (100.0% success, 0 errors)

- **Total Duration:** 21.87 seconds

- **Combined Throughput:** **457.3 ops/sec**

- **Read Throughput:** 318.09 reads/sec

- **Write Throughput:** 139.2 writes/sec

- **Redis Cache Hit Ratio:** **80.92%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 10,000 | 193.58 | 14.45 | 898.74 | 1245.67 | 1912.79 | 2121.93 |
| **Reads (Cache-Aside)** | 6,956 | 18.93 | 10.11 | 46.91 | 61.86 | 104.87 | 257.9 |
| **Writes (Redis Stream)** | 3,044 | 592.67 | 381.94 | 1564.31 | 1822.77 | 2028.42 | 2121.93 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `READ_CUSTOMER_BY_CITY` | READ | 1398 | 100.0% | 4.98 | 45.33 | 85.18 | 100.0% |
| `READ_CUSTOMER_PROFILE` | READ | 1373 | 100.0% | 5.06 | 49.03 | 88.71 | 97.96% |
| `READ_FLIGHT_DETAILS` | READ | 1411 | 100.0% | 4.91 | 50.41 | 84.94 | 97.52% |
| `READ_FLIGHT_SEATS` | READ | 1424 | 100.0% | 22.22 | 81.17 | 123.25 | 99.09% |
| `READ_ROUTE_SEARCH` | READ | 1350 | 100.0% | 23.72 | 78.2 | 116.7 | 7.33% |
| `WRITE_BOOK_SEATS` | WRITE | 627 | 100.0% | 4.8 | 44.2 | 65.55 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 586 | 100.0% | 5.21 | 43.59 | 74.12 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 609 | 100.0% | 931.77 | 1913.31 | 2079.35 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 620 | 100.0% | 972.81 | 1922.42 | 2062.82 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 602 | 100.0% | 930.53 | 1922.67 | 2078.36 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 25.29% | 19.05% | 15.51 MB | 14.24 MB |
| `pgbouncer` | 12.35% | 6.18% | 10.31 MB | 10.24 MB |
| `postgres` | 81.16% | 63.62% | 484.9 MB | 474.42 MB |
| Benchmark Client | 139.2% | 120.98% | 92.91 MB | 89.73 MB |

## 5. Redis State & Memory


- **Used Memory:** 7.69 MB (Peak: 23.32 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 4445
