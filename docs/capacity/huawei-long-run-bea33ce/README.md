# Huawei long-run capacity validation - commit bea33ce

## Result

The measured clean operating point for this exact single-backend topology is **400 HTTP RPS for 30 minutes**. The run completed 720,000 requests with no generator drops, transport errors, unexpected HTTP responses, lock waits, missing seat maps, or duplicate seat ownership. Read p95 was 13.052 ms and hold p95 was 45.354 ms.

At **500 HTTP RPS for 30 minutes**, latency remained within the provisional targets and all accepted holds remained correct, but 29 of 45,000 hold attempts returned admission HTTP 503. This is 0.0644% of hold attempts and 0.0032% of all requests. The strict zero-unexpected-error gate failed, so 500 RPS is a measured failure boundary rather than an accepted production capacity.

A controlled two-worker repeat kept the aggregate database pool and hold admission budgets fixed. It reduced admission rejections from 29 to 9, but did not eliminate them and increased hold p95 from 60.692 ms to 89.427 ms. The default remains one API worker until another change is justified and validated.

## Topology and workload

- Backend ECS: Huawei Cloud `c6.xlarge.2`, 4 vCPU and 8 GiB RAM.
- Generator ECS: Huawei Cloud `c6.2xlarge.2`, 8 vCPU and 16 GiB RAM.
- Traffic used the private network; generator and backend were separate machines.
- Backend ran API, PgBouncer, PostgreSQL 17.6, Redis, Kafka, publisher, consumer, simulator, maintenance, reconciler, and Prometheus on one ECS.
- Fixture: 800 shows, 300 seats per show, and 8,000 viewers.
- Mix: 95% conditional seat-map reads and 5% independent-seat holds.
- Each stage used four generator processes, a synchronized start, no client retry, 5-second client/server keep-alive, and transport diagnostics.
- Hold TTL was 120 seconds. Durability verification ran after the expiry drain.

## Comparison

| API configuration | Offered load | Requests | Read p95 | Hold p95 | Drops | Transport errors | Hold 503 | Workload gate |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| 1 worker, pool 12, admission 8 | 400 RPS x 30 min | 720,000 | 13.052 ms | 45.354 ms | 0 | 0 | 0 / 36,000 | Pass |
| 1 worker, pool 12, admission 8 | 500 RPS x 30 min | 900,000 | 19.272 ms | 60.692 ms | 0 | 0 | 29 / 45,000 | Fail |
| 2 workers, pool 6 each, admission 4 each | 500 RPS x 30 min | 900,000 | 11.857 ms | 89.427 ms | 0 | 0 | 9 / 45,000 | Fail |

The two-worker configuration reduced 503 responses by 69.0% and read p95 by 38.5%, while hold p95 increased by 47.3%. This mixed result does not support changing the production default.

## Database, reconciliation, and CPU evidence

| Measurement | 400, 1 worker | 500, 1 worker | 500, 2 workers |
|---|---:|---:|---:|
| Mean DB transaction | 17.281 ms | 21.994 ms | unavailable across process-local metrics |
| Mean commit | 1.889 ms | 2.091 ms | unavailable across process-local metrics |
| Mean pool return | 0.0097 ms | 0.0102 ms | unavailable across process-local metrics |
| Peak DB connections | 13 | 14 | 13 |
| Peak active DB connections | 6 | 5 | 6 |
| Peak lock waiters | 0 | 0 | 0 |
| Peak reconciliation age | 20.736 s | 20.419 s | 20.633 s |
| Peak missing seat maps | 0 | 0 | 0 |
| Peak overdue active holds | 7 | 16 | 26 |
| Peak overdue age | 0.339 s | 0.546 s | 1.079 s |
| Mean API container CPU | 59.6% | 69.5% | 73.9% |
| Mean PostgreSQL container CPU | 86.3% | 99.1% | 99.5% |
| Mean Redis container CPU | 13.3% | 13.0% | 12.7% |

Docker CPU percentages use 100% for one vCPU. The backend has four vCPUs, so a component at 100% is using about one quarter of total host CPU capacity. PostgreSQL was the heaviest single component, but host CPU was not exhausted.

The two-worker observer started about 110 seconds after the generator because the first observer process contained the pre-fix PostgreSQL 17 query. The retained observer has 915 valid samples and covers the remaining load plus expiry drain. API Prometheus counters are process-local and alternate between workers, so database timing deltas are intentionally omitted for that run. Generator status and latency, PostgreSQL activity, Redis state, reconciliation, CPU, and post-run durability remain valid.

## Correctness and recovery gates

All three post-expiry checks passed:

- 400 RPS: 36,000 acknowledged holds, 36,000 idempotency records, holds, and orders; zero broken links and zero overlapping seat intervals.
- 500 RPS, one worker: 44,971 acknowledged holds with matching durable records; zero broken links and zero overlapping seat intervals.
- 500 RPS, two workers: 44,991 acknowledged holds with matching durable records; zero broken links and zero overlapping seat intervals.
- Every run ended with zero active or overdue selected holds and zero unpublished outbox, pending refresh, and dead-letter records.

The transient overdue samples were bounded and drained. They do not indicate double-booking.

## Capacity interpretation

This evidence supports **400 RPS as the current demonstrated clean ceiling** for the measured read-heavy mix on the combined 4-vCPU backend ECS. A cautious operating budget for this same topology is about **250-300 RPS** to retain 25-38% traffic headroom. This is a planning estimate, not another tested point.

The result is not a universal production maximum. The backend still uses single instances of PostgreSQL, Redis, and Kafka on the same ECS and does not include TLS, a production load balancer, multi-AZ failover, or managed-service latency. A separated production topology must be measured again. Nothing in this report demonstrates 100,000 RPS.

## Evidence

Compact conclusions are in `*-analysis.json`; generator gates are in `*-generator-summary.json`; correctness is in `*-durability.json`. Full two-second database/cache observations and Docker CPU samples are retained for audit. Load manifests and viewer tokens are intentionally excluded.
