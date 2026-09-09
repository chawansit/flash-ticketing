# Concentrated traffic and burst validation

Executed on 2026-09-09. Concentrated seat-map reads passed 400 HTTP RPS for five minutes; the continuous burst passed. Both contention waves had exactly one HTTP and durable winner, but admission rejections and client response latency failed the contention availability/performance targets. This is not a new production maximum.

| Scenario | HTTP requests | Read p95 | Hold p95 | Outcome |
|---|---:|---:|---:|---|
| Hot reads, 200 RPS, 5 min | 60,000 | 8.96 ms | 24.68 ms | Pass, zero errors/drops |
| Hot reads, 400 RPS, 5 min | 120,000 | 12.85 ms | 44.60 ms | Pass, zero errors/drops |
| Burst 100 / 400 / 100 RPS, 4 min | 60,000 | 11.53 ms | 37.12 ms | Pass, zero errors/drops |

| Burst phase | Duration | Read p95 | Hold p95 |
|---|---:|---:|---:|
| Baseline, 100 RPS | 60s | 10.47 ms | 25.58 ms |
| Peak, 400 RPS | 120s | 11.28 ms | 38.19 ms |
| Recovery, 100 RPS | 60s | 12.68 ms | 31.85 ms |

| Seat contenders | Winners | Expected 409 conflicts | ADMISSION_FULL | Failed-response p95 | Client dispatch spread |
|---|---:|---:|---:|---:|---:|
| 100 | 1 | 89 | 10 | 317.22 ms | 42.10 ms |
| 1,000 | 1 | 957 | 42 | 3,635.98 ms | 402.92 ms |

Both contention waves passed immediate PostgreSQL ownership verification: one idempotency result, hold and order, and exactly one active hold/order on the seat, matching the acknowledged winner. Neither wave met the no-admission-rejection gate or the failed-response target below 100–200ms. No transport failures occurred. Server logs corroborated all 1,100 responses.

Across both contention waves, server-measured 409 duration p95 was 26ms (max 78.5ms); 503 p95 was 0.21ms (max 0.77ms). These measurements exclude time before application middleware. Client latency also includes connection establishment, scheduling and waiting outside that measurement. No particular bottleneck can be assigned from this gap alone; a warm-connection, multi-process contention comparison and ingress timing are the next diagnostic steps.

All **12,002 acknowledged holds** across 14 run IDs matched PostgreSQL holds, orders and idempotency records. Final verification found zero broken links, active/overdue holds or pending orders; outbox, refresh and dead-letter queues were zero. The earlier draining snapshot retains in-lifetime holds and is not the final gate.


## Topology and scope
Same Huawei backend (4 vCPU / 8 GiB) and separate generator (8 vCPU / 16 GiB), private network, 800 shows x 300 seats, 8,000 viewers. Backend source 6d1cde1; 120-second holds, admission eight, pool maximum 12, existing reconciliation and browse-body cache. No payment load or production maximum claim.

## Workloads
- Hot reads: configured 90% of availability reads target one shared show. Holds stay uniformly distributed across disjoint show partitions. 95/5 reads/holds, 200 then 400 RPS, five minutes each. The fixture retains background reconciliation for all 800 shows.
- Contention: one fresh seat for each 100- and 1,000-viewer client barrier. No retries. Exactly-one-winner correctness is separate from admission, rate limiting and expected seat conflicts. Client dispatch spread is reported; server arrivals are not guaranteed simultaneous.
- Burst: four workers, continuous 100 RPS for 60s, 400 RPS for 120s, then 100 RPS for 60s. Uniform 95/5 workload; per-phase latency and statuses retained.

## Acceptance
Throughput phases require reads p95 <=150ms, holds p95 <=300ms, zero unexpected responses or generator drops, exact accounting and worker start skew <=100ms. Percentiles are worst-worker values, not merged percentiles. Contention requires one HTTP winner and one durable hold/order with matching ownership; other rejection classes are reported separately. Database verification follows hold expiry.

## Observer limitation
The initial observer launch was corrected during the 200-RPS stage: two overlapping pooled observers were stopped, and a single direct-PostgreSQL observer started. The initial shared snapshot is not used as valid monitoring evidence. Direct observer coverage for 200 RPS is partial; Prometheus and HTTP results cover the whole stage. Subsequent scenarios use the corrected observer.

See [ADR 0018](../../adr/0018-concentrated-traffic-validation.md).

## Backend observations
Within the measured windows, direct PostgreSQL samples showed peak connections of 12 / 14 / 14 for 200 RPS / 400 RPS / burst; no sampled lock waiters or missing maps. Maximum reconciliation ages were 20.35 / 20.74 / 20.27 seconds, below the unchanged 30-second cache TTL. Maximum observed overdue-hold cleanup age was 0.243 / 0.289 / 0.224 seconds. Sampling every two seconds cannot exclude shorter lock waits. See [window-filtered observations](observation-summary.json); the 200-RPS direct-observer coverage is partial as described above. These results do not establish a 5–10x scaling relationship.

## Evidence and reproducibility
- Per-worker reports and logs: [200 RPS](concentrated-200/summary.json), [400 RPS](concentrated-400/summary.json), [burst](concentrated-burst/summary.json).
- Contention client details: [100](contention-100.json), [1,000](contention-1000.json); immediate durable ownership: [100](contention-100-durable.json), [1,000](contention-1000-durable.json).
- [Final durability](concentrated-final-durability.json) and [earlier draining snapshot](concentrated-draining-durability.json).
- [Backend source hashes and settings](concentrated-config.json), [direct observer](concentrated-direct-observations.json), [server contention latency](concentrated-contention-server-latency.json).
- Prometheus range exports and server log analyses are stored alongside this report. Raw API logs remain on the test ECS; only compact analyses are published.
- [Seven local generator tests passed](generator-tests.log). The complete application suite was not rerun; runtime application source was unchanged. PostgreSQL ownership and expiry checks were executed against the cloud stack.
- [Exact executed generator scripts](concentrated-runtime-harness/http_load_generator.py) are retained. The repository generator subsequently tightens only the oversized-burst sample guard (full schedule instead of base rate times wall duration); all executed workloads are below both bounds.

After measurement, benchmark containers and observers were stopped, private manifests deleted, and the temporary forwarding rule removed. ECS instances and synthetic data volumes remain available.

The prior uniform 400-RPS 30-minute result remains the sustained baseline. These five-minute hot-read stages do not establish sustained skew capacity, concentrated successful-booking throughput, payment capacity, HA, all-location sizing or 100,000 RPS.
