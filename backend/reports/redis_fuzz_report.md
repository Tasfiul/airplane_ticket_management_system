# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T06:58:15.405672+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 200 Client Workers | 4 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 10,000 (100.0% success, 0 errors)

- **Total Duration:** 7.03 seconds

- **Combined Throughput:** **1422.27 ops/sec**

- **Read Throughput:** 987.34 reads/sec

- **Write Throughput:** 434.93 writes/sec

- **Redis Cache Hit Ratio:** **94.02%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 10,000 | 127.81 | 50.6 | 283.74 | 341.47 | 707.46 | 2497.47 |
| **Reads (Cache-Aside)** | 6,942 | 93.13 | 43.41 | 251.82 | 282.9 | 361.69 | 666.68 |
| **Writes (Redis Stream)** | 3,058 | 206.56 | 139.93 | 368.5 | 493.47 | 1993.31 | 2497.47 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `READ_CUSTOMER_BY_CITY` | READ | 1410 | 100.0% | 39.06 | 260.03 | 300.08 | 100.0% |
| `READ_CUSTOMER_PROFILE` | READ | 1391 | 100.0% | 39.34 | 257.65 | 301.93 | 99.21% |
| `READ_FLIGHT_DETAILS` | READ | 1395 | 100.0% | 39.22 | 265.59 | 310.67 | 98.64% |
| `READ_FLIGHT_SEATS` | READ | 1322 | 100.0% | 84.45 | 305.27 | 444.66 | 98.87% |
| `READ_ROUTE_SEARCH` | READ | 1424 | 100.0% | 44.33 | 305.96 | 381.38 | 74.02% |
| `WRITE_BOOK_SEATS` | WRITE | 649 | 100.0% | 39.01 | 266.08 | 357.58 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 635 | 100.0% | 39.9 | 271.35 | 310.79 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 556 | 100.0% | 188.15 | 557.45 | 1997.55 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 595 | 100.0% | 206.12 | 755.33 | 2201.4 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 623 | 100.0% | 185.2 | 842.28 | 2207.58 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 43.88% | 26.58% | 24.27 MB | 23.0 MB |
| `pgbouncer` | 26.38% | 15.97% | 11.15 MB | 10.85 MB |
| `postgres` | 338.4% | 206.37% | 1456.13 MB | 1450.21 MB |
| Benchmark Client | 127.8% | 111.67% | 96.96 MB | 94.02 MB |

## 5. Redis State & Memory


- **Used Memory:** 11.78 MB (Peak: 16.52 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 3514
