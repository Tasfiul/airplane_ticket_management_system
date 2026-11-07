# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T12:06:01.778553+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 8 Client Workers | 2 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 60 (100.0% success, 0 errors)

- **Total Duration:** 0.2 seconds

- **Combined Throughput:** **305.12 ops/sec**

- **Read Throughput:** 223.76 reads/sec

- **Write Throughput:** 81.37 writes/sec

- **Redis Cache Hit Ratio:** **79.55%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 60 | 10.36 | 1.74 | 30.8 | 51.1 | 96.63 | 159.29 |
| **Reads (Cache-Aside)** | 44 | 3.91 | 1.6 | 11.27 | 12.8 | 18.77 | 19.8 |
| **Writes (Redis Stream)** | 16 | 28.09 | 15.68 | 53.06 | 79.64 | 143.36 | 159.29 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 5 | 100.0% | 1.78 | 12.19 | 12.85 | 100.0% |
| `READ_AIRPORTS_CATALOG` | READ | 2 | 100.0% | 0.93 | 1.08 | 1.09 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 7 | 100.0% | 1.49 | 4.52 | 4.56 | 71.43% |
| `READ_CUSTOMER_BY_CITY` | READ | 7 | 100.0% | 0.93 | 5.42 | 5.47 | 71.43% |
| `READ_CUSTOMER_ME` | READ | 1 | 100.0% | 10.87 | 10.87 | 10.87 | 100.0% |
| `READ_CUSTOMER_PROFILE` | READ | 5 | 100.0% | 1.21 | 4.13 | 4.62 | 80.0% |
| `READ_FEATURED_ROUTES` | READ | 6 | 100.0% | 1.03 | 9.12 | 11.09 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 2 | 100.0% | 2.58 | 4.53 | 4.7 | 50.0% |
| `READ_FLIGHT_SEATS` | READ | 4 | 100.0% | 2.79 | 10.2 | 11.2 | 100.0% |
| `READ_ROUTE_SEARCH` | READ | 5 | 100.0% | 6.99 | 19.32 | 19.7 | 40.0% |
| `WRITE_BOOK_SEATS` | WRITE | 4 | 100.0% | 1.25 | 1.76 | 1.83 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 4 | 100.0% | 1.11 | 1.46 | 1.51 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 1 | 100.0% | 29.51 | 29.51 | 29.51 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 3 | 100.0% | 53.02 | 53.09 | 53.09 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 3 | 100.0% | 32.54 | 146.61 | 156.75 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 1 | 100.0% | 30.78 | 30.78 | 30.78 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 3.73% | 3.73% | 12.86 MB | 12.86 MB |
| `pgbouncer` | 0.02% | 0.02% | 8.0 MB | 8.0 MB |
| `postgres` | 4.0% | 4.0% | 1338.37 MB | 1338.37 MB |
| Benchmark Client | 0.0% | 0.0% | 136.24 MB | 136.24 MB |

## 5. Redis State & Memory


- **Used Memory:** 2.3 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 26
