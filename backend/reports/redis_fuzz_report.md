# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-06T17:00:41.948106+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 8 Client Workers | 2 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 60 (100.0% success, 0 errors)

- **Total Duration:** 0.24 seconds

- **Combined Throughput:** **253.3 ops/sec**

- **Read Throughput:** 156.2 reads/sec

- **Write Throughput:** 97.1 writes/sec

- **Redis Cache Hit Ratio:** **78.38%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 60 | 25.15 | 8.32 | 119.89 | 142.56 | 153.84 | 163.34 |
| **Reads (Cache-Aside)** | 37 | 6.76 | 8.05 | 11.12 | 13.04 | 14.76 | 15.64 |
| **Writes (Redis Stream)** | 23 | 54.73 | 27.06 | 145.22 | 147.11 | 159.8 | 163.34 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `READ_CUSTOMER_BY_CITY` | READ | 4 | 100.0% | 8.3 | 10.01 | 10.19 | 100.0% |
| `READ_CUSTOMER_PROFILE` | READ | 10 | 100.0% | 2.76 | 9.63 | 10.52 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 9 | 100.0% | 8.05 | 11.25 | 11.45 | 77.78% |
| `READ_FLIGHT_SEATS` | READ | 9 | 100.0% | 8.36 | 13.68 | 15.24 | 88.89% |
| `READ_ROUTE_SEARCH` | READ | 5 | 100.0% | 9.58 | 13.16 | 13.19 | 0.0% |
| `WRITE_BOOK_SEATS` | WRITE | 3 | 100.0% | 5.54 | 6.24 | 6.3 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 7 | 100.0% | 1.98 | 7.46 | 9.2 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 3 | 100.0% | 133.02 | 144.64 | 145.68 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 6 | 100.0% | 90.73 | 158.1 | 162.3 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 4 | 100.0% | 88.84 | 145.64 | 146.92 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 3.77% | 2.75% | 10.3 MB | 10.19 MB |
| `pgbouncer` | 2.94% | 2.94% | 11.96 MB | 11.96 MB |
| `postgres` | 0.32% | 0.32% | 1104.9 MB | 1104.9 MB |
| Benchmark Client | 74.7% | 37.35% | 134.25 MB | 134.19 MB |

## 5. Redis State & Memory


- **Used Memory:** 4.62 MB (Peak: 23.32 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 5524
