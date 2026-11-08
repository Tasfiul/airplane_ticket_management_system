# Concurrent Redis Cache-Aside & Stream Write Benchmark Report

**Date:** 2026-11-08T06:43:57.315211+00:00  

**Target:** localhost:6432 (PgBouncer) & localhost:6379 (Redis)  

**Concurrency:** 40 Client Workers | 0 Stream Workers  


## 1. Executive Summary


- **Total Operations:** 5,000 (100.0% success, 0 errors)

- **Total Duration:** 1.99 seconds

- **Combined Throughput:** **2512.57 ops/sec**

- **Read Throughput:** 2270.86 reads/sec

- **Write Throughput:** 241.71 writes/sec

- **Redis Cache Hit Ratio:** **93.58%**


## 2. Latency Metrics


| Workload Type | Count | Mean (ms) | p50 Median (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max (ms) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Overall** | 5,000 | 15.16 | 6.82 | 37.86 | 53.15 | 123.3 | 203.74 |
| **Reads (Cache-Aside)** | 4,519 | 10.83 | 6.64 | 27.02 | 38.09 | 52.03 | 79.99 |
| **Writes (Redis Stream)** | 481 | 55.81 | 45.87 | 124.77 | 141.59 | 177.19 | 203.74 |

## 3. Per Operation Breakdown


| Operation | Category | Count | Success Rate | p50 (ms) | p95 (ms) | p99 (ms) | Cache Hit % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `AUTH_LOGIN` | READ | 443 | 100.0% | 6.76 | 42.65 | 54.6 | 79.46% |
| `READ_AIRPORTS_CATALOG` | READ | 469 | 100.0% | 6.44 | 39.54 | 50.6 | 100.0% |
| `READ_CUSTOMER_BOOKINGS` | READ | 496 | 100.0% | 6.29 | 42.97 | 51.91 | 98.79% |
| `READ_CUSTOMER_BY_CITY` | READ | 463 | 100.0% | 6.19 | 35.6 | 49.64 | 100.0% |
| `READ_CUSTOMER_ME` | READ | 425 | 100.0% | 6.43 | 34.84 | 51.69 | 83.29% |
| `READ_CUSTOMER_PROFILE` | READ | 460 | 100.0% | 6.78 | 38.09 | 59.59 | 82.39% |
| `READ_FEATURED_ROUTES` | READ | 443 | 100.0% | 6.25 | 36.43 | 51.04 | 100.0% |
| `READ_FLIGHT_DETAILS` | READ | 417 | 100.0% | 6.4 | 38.47 | 49.85 | 98.56% |
| `READ_FLIGHT_SEATS` | READ | 430 | 100.0% | 13.54 | 44.26 | 56.64 | 99.3% |
| `READ_ROUTE_SEARCH` | READ | 473 | 100.0% | 6.31 | 37.89 | 52.27 | 93.23% |
| `WRITE_AUTH_REGISTER` | WRITE | 50 | 100.0% | 58.04 | 135.29 | 149.7 | N/A |
| `WRITE_BOOK_SEATS` | WRITE | 71 | 100.0% | 6.61 | 46.22 | 50.89 | N/A |
| `WRITE_CANCEL_BOOKING` | WRITE | 81 | 100.0% | 6.5 | 33.83 | 42.96 | N/A |
| `WRITE_CREATE_CUSTOMER` | WRITE | 60 | 100.0% | 66.89 | 149.93 | 166.45 | N/A |
| `WRITE_CREATE_FLIGHT` | WRITE | 71 | 100.0% | 86.7 | 164.86 | 177.66 | N/A |
| `WRITE_UPDATE_CUSTOMER` | WRITE | 72 | 100.0% | 71.35 | 150.65 | 179.67 | N/A |
| `WRITE_UPDATE_FLIGHT_STATUS` | WRITE | 76 | 100.0% | 58.91 | 145.03 | 191.89 | N/A |

## 4. Resource & Container Profiling


| Container / Process | Peak CPU (%) | Avg CPU (%) | Peak RAM (MB) | Avg RAM (MB) |
| :--- | :--- | :--- | :--- | :--- |
| `redis` | 31.99% | 16.24% | 15.15 MB | 15.15 MB |
| `pgbouncer` | 20.99% | 12.12% | 11.23 MB | 11.22 MB |
| `postgres` | 185.08% | 92.6% | 1030.14 MB | 1030.14 MB |
| Benchmark Client | 121.9% | 103.43% | 105.11 MB | 104.05 MB |

## 5. Redis State & Memory


- **Used Memory:** 6.76 MB (Peak: 15.4 MB)

- **Max Memory Limit:** 512.0 MB

- **Evicted Keys:** 0

- **Stream Length:** 2688
