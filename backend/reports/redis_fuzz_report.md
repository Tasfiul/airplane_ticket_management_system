# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-07T17:10:43.862273+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 40 Client Workers | 0 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 5,000 (100.0% success, 0 errors)

- **Total Duration:** 3.74 seconds

- **Combined Throughput:** **1335.21 ops/sec**

- **Read Throughput:** 1068.44 reads/sec

- **Write Throughput:** 266.78 writes/sec

- **Redis Cache Hit Ratio:** **90.08%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 5,000 | 28.63 | 4.4 | 87.87 | 144.9 | 259.24 | 913.41 |
| **Reads (Cache-Aside)** | 4,001 | 7.71 | 3.71 | 22.41 | 32.27 | 49.58 | 101.96 |
| **Writes (Redis Stream)** | 999 | 112.4 | 87.85 | 208.37 | 259.34 | 816.77 | 913.41 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 386 | 100.0% | 4.21 | 35.22 | 58.9 | 69.95% |
| `READ_AIRPORTS_CATALOG` | READ | 401 | 100.0% | 2.96 | 24.38 | 46.78 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 421 | 100.0% | 3.61 | 29.89 | 44.54 | 96.44% |
| `READ_CUSTOMER_BY_CITY` | READ | 416 | 100.0% | 3.25 | 26.37 | 36.83 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 386 | 100.0% | 3.54 | 35.93 | 52.26 | 79.53% |
| `READ_CUSTOMER_PROFILE` | READ | 399 | 100.0% | 4.1 | 33.29 | 63.2 | 74.19% |
| `READ_FEATURED_ROUTES` | READ | 411 | 100.0% | 3.45 | 29.59 | 46.12 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 405 | 100.0% | 3.14 | 28.85 | 44.44 | 96.05% |
| `READ_FLIGHT_SEATS` | READ | 362 | 100.0% | 7.0 | 41.1 | 62.11 | 95.86% |
| `READ_ROUTE_SEARCH` | READ | 414 | 100.0% | 3.83 | 31.33 | 46.1 | 87.2% |
| `WRITE_AUTH_REGISTER` | WRITE | 157 | 100.0% | 108.74 | 242.96 | 806.76 | N/A |
| `WRITE_BOOK_SEATS` | WRITE | 148 | 100.0% | 2.87 | 25.04 | 44.83 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 138 | 100.0% | 3.1 | 33.02 | 44.26 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 134 | 100.0% | 115.05 | 760.23 | 811.34 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 152 | 100.0% | 137.01 | 263.89 | 772.69 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 129 | 100.0% | 113.36 | 794.32 | 874.99 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 141 | 100.0% | 119.7 | 263.6 | 848.09 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 17.19% | 8.84% | 16.0 MB | 15.41 MB |
| `pgbouncer` | 23.17% | 11.6% | 11.23 MB | 10.93 MB |
| `postgres` | 291.61% | 145.83% | 1257.47 MB | 1249.28 MB |
| Benchmark Client | 116.5% | 81.12% | 104.04 MB | 102.8 MB |

## 5. Redis State & Memory


- **Used Memory:** 4.63 MB (Peak: 52.24 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 739
