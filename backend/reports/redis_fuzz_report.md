# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T08:32:51.178223+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 40 Client Workers | 4 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 5,000 (100.0% success, 0 errors)

- **Total Duration:** 4.54 seconds

- **Combined Throughput:** **1100.87 ops/sec**

- **Read Throughput:** 765.99 reads/sec

- **Write Throughput:** 334.89 writes/sec

- **Redis Cache Hit Ratio:** **85.37%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 5,000 | 35.1 | 10.36 | 106.3 | 145.9 | 258.55 | 361.22 |
| **Reads (Cache-Aside)** | 3,479 | 15.56 | 7.42 | 40.02 | 49.36 | 76.55 | 142.07 |
| **Writes (Redis Stream)** | 1,521 | 79.81 | 61.21 | 184.16 | 233.49 | 297.9 | 361.22 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 411 | 100.0% | 14.67 | 58.02 | 74.57 | 57.91% |
| `READ_CUSTOMER_BOOKINGS` | READ | 440 | 100.0% | 5.99 | 43.36 | 61.49 | 98.41% |
| `READ_CUSTOMER_BY_CITY` | READ | 456 | 100.0% | 6.18 | 44.53 | 57.19 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 456 | 100.0% | 7.9 | 49.63 | 78.18 | 69.08% |
| `READ_CUSTOMER_PROFILE` | READ | 419 | 100.0% | 7.95 | 51.89 | 76.25 | 72.55% |
| `READ_FLIGHT_DETAILS` | READ | 457 | 100.0% | 5.99 | 44.15 | 58.5 | 98.03% |
| `READ_FLIGHT_SEATS` | READ | 417 | 100.0% | 13.19 | 52.68 | 81.91 | 98.8% |
| `READ_ROUTE_SEARCH` | READ | 423 | 100.0% | 6.7 | 51.28 | 67.81 | 86.05% |
| `WRITE_BOOK_SEATS` | WRITE | 240 | 100.0% | 5.46 | 41.18 | 54.31 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 266 | 100.0% | 5.77 | 42.65 | 49.54 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 261 | 100.0% | 87.99 | 252.41 | 310.12 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 244 | 100.0% | 114.74 | 263.68 | 305.21 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 251 | 100.0% | 102.43 | 261.64 | 294.29 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 259 | 100.0% | 106.77 | 246.34 | 315.69 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 34.95% | 9.97% | 15.45 MB | 15.33 MB |
| `pgbouncer` | 29.57% | 7.92% | 11.91 MB | 11.7 MB |
| `postgres` | 329.14% | 87.0% | 1540.1 MB | 1533.44 MB |
| Benchmark Client | 124.0% | 110.55% | 93.29 MB | 92.1 MB |

## 5. Redis State & Memory


- **Used Memory:** 5.19 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 2016
