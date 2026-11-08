# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-08T16:03:10.025534+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 6 Client Workers | 2 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 60 (100.0% success, 0 errors)

- **Total Duration:** 0.2 seconds

- **Combined Throughput:** **299.74 ops/sec**

- **Read Throughput:** 164.86 reads/sec

- **Write Throughput:** 134.88 writes/sec

- **Redis Cache Hit Ratio:** **60.61%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 60 | 16.22 | 8.43 | 36.23 | 48.61 | 95.05 | 97.68 |
| **Reads (Cache-Aside)** | 33 | 4.89 | 4.42 | 9.71 | 11.81 | 14.53 | 15.78 |
| **Writes (Redis Stream)** | 27 | 30.07 | 27.11 | 58.35 | 87.77 | 96.52 | 97.68 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 7 | 100.0% | 4.79 | 13.53 | 15.33 | 28.57% |
| `READ_AIRPORTS_CATALOG` | READ | 2 | 100.0% | 0.7 | 0.71 | 0.71 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 5 | 100.0% | 4.57 | 7.82 | 8.44 | 80.0% |
| `READ_CUSTOMER_BY_CITY` | READ | 3 | 100.0% | 7.2 | 11.4 | 11.77 | 0.0% |
| `READ_CUSTOMER_ME` | READ | 3 | 100.0% | 1.12 | 8.86 | 9.55 | 66.67% |
| `READ_CUSTOMER_PROFILE` | READ | 1 | 100.0% | 0.57 | 0.57 | 0.57 | 100.0% |
| `READ_FEATURED_ROUTES` | READ | 4 | 100.0% | 4.91 | 9.54 | 9.62 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 3 | 100.0% | 2.89 | 8.61 | 9.12 | 66.67% |
| `READ_FLIGHT_SEATS` | READ | 1 | 100.0% | 6.44 | 6.44 | 6.44 | 0.0% |
| `READ_ROUTE_SEARCH` | READ | 4 | 100.0% | 4.88 | 11.37 | 11.7 | 75.0% |
| `WRITE_AUTH_REGISTER` | WRITE | 6 | 100.0% | 26.13 | 29.98 | 30.12 | N/A |
| `WRITE_BOOK_SEATS` | WRITE | 3 | 100.0% | 0.89 | 6.21 | 6.68 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 3 | 100.0% | 0.9 | 1.04 | 1.05 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 5 | 100.0% | 21.27 | 27.46 | 28.21 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 2 | 100.0% | 95.45 | 97.46 | 97.64 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 5 | 100.0% | 34.56 | 37.82 | 38.18 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 3 | 100.0% | 47.22 | 72.27 | 74.5 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 0.58% | 0.58% | 239.0 MB | 239.0 MB |
| `pgbouncer` | 0.02% | 0.02% | 8.12 MB | 8.12 MB |
| `postgres` | 4.0% | 2.0% | 828.3 MB | 828.25 MB |
| Benchmark Client | 0.0% | 0.0% | 156.5 MB | 156.5 MB |

## 5. Redis State & Memory


- **Used Memory:** 227.58 MB (Peak: 395.4 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 23706
