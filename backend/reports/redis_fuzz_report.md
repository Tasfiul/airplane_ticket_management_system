# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T07:30:36.706696+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 40 Client Workers | 4 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 5,000 (100.0% success, 0 errors)

- **Total Duration:** 3.94 seconds

- **Combined Throughput:** **1269.78 ops/sec**

- **Read Throughput:** 888.34 reads/sec

- **Write Throughput:** 381.44 writes/sec

- **Redis Cache Hit Ratio:** **96.23%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 5,000 | 30.34 | 7.3 | 89.84 | 137.02 | 243.26 | 604.03 |
| **Reads (Cache-Aside)** | 3,498 | 12.01 | 6.21 | 35.63 | 42.42 | 55.29 | 82.21 |
| **Writes (Redis Stream)** | 1,502 | 73.02 | 48.2 | 159.98 | 197.87 | 498.61 | 604.03 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `READ_CUSTOMER_BY_CITY` | READ | 683 | 100.0% | 5.16 | 40.04 | 48.69 | 100.0% |
| `READ_CUSTOMER_PROFILE` | READ | 731 | 100.0% | 5.46 | 39.73 | 49.28 | 98.36% |
| `READ_FLIGHT_DETAILS` | READ | 665 | 100.0% | 5.45 | 39.69 | 50.18 | 96.69% |
| `READ_FLIGHT_SEATS` | READ | 729 | 100.0% | 11.71 | 48.28 | 60.81 | 98.49% |
| `READ_ROUTE_SEARCH` | READ | 690 | 100.0% | 5.72 | 41.63 | 60.43 | 87.39% |
| `WRITE_BOOK_SEATS` | WRITE | 293 | 100.0% | 5.37 | 38.38 | 48.55 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 312 | 100.0% | 5.21 | 38.78 | 48.52 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 286 | 100.0% | 92.93 | 279.55 | 536.11 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 301 | 100.0% | 111.71 | 257.04 | 523.38 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 310 | 100.0% | 88.58 | 205.26 | 448.73 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 36.51% | 13.23% | 15.42 MB | 14.95 MB |
| `pgbouncer` | 31.35% | 10.85% | 12.15 MB | 11.78 MB |
| `postgres` | 365.32% | 124.72% | 1558.53 MB | 1556.82 MB |
| Benchmark Client | 127.0% | 113.05% | 84.88 MB | 83.4 MB |

## 5. Redis State & Memory


- **Used Memory:** 4.63 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 897
