# Huawei RDS capacity validation

ADR 0035 prepared a matched test that moves PostgreSQL from the shared backend
ECS to Huawei RDS while retaining local PgBouncer and an aggregate application
pool budget of twelve.

The 2026-09-13 diagnostic produced one clean **750 RPS for 30 minutes** run for
the 95% conditional seat-map read / 5% unique-seat hold workload: zero unexpected
errors and drops, read/hold p95 8.251/51.050 ms, exact durability, zero overlap and
drained queues.

A [fresh 750 RPS repeatability control](2026-09-14-750-repeatability/README.md)
then failed on one `ADMISSION_FULL` response about three seconds after load began.
The partial audit remained correct, but the strict availability gate failed. The
clean 750 result is therefore demonstrated once but is not yet a repeatable
zero-error boundary. **600 RPS for 30 minutes remains the highest repeatable clean
baseline** until ADR 0039 completes its safety and confirmation stages.

The [600 RPS repeat and 750 RPS boundary report](2026-09-13-600-repeat-750-stage/README.md)
remains the historical pre-correction baseline. The earlier
[400 RPS RDS control](2026-09-13-control/README.md) documents the first bounded
admission comparison.
## Completed preparation

- Added `compose.rds.yaml`; default rendering excludes the local PostgreSQL service, points PgBouncer upstream at RDS with certificate verification and directs migrations straight to RDS.
- Added a credential-safe preflight for DNS, TCP, TLS, primary/read-write state, version, encoding, connection budget, latency and post-migration schema checks.
- Added a PostgreSQL statistics and `EXPLAIN ANALYZE` collector plus a deterministic local/RDS comparator.
- Verified the override with placeholder values: PgBouncer and migration have no dependency on local PostgreSQL; API uses PgBouncer; local PostgreSQL appears only with the explicit `local-database` profile.

## Local control, 2026-09-12 UTC

The preflight ran against the isolated local PostgreSQL 17.6 container with its local-only plaintext exception. It passed five migration checksums, 17 required tables and nine required indexes. Median TCP, connect and read-only transaction times were 0.530 ms, 8.750 ms and 0.693 ms. TLS was false, as expected for this Docker-network control; TLS remains mandatory by default for RDS.

The idle profiler completed without mutation. The 151 MB database reported zero cumulative deadlocks. Representative PostgreSQL execution times were:

| Query | Execution time | Top node |
|---|---:|---|
| Event lookup | 0.010 ms | Index Scan |
| Seat lock | 0.090 ms | LockRows |
| Active hold owner | 0.020 ms | LockRows |
| Expiry candidate | 0.040 ms | Limit |
| Outbox pending | 0.010 ms | Limit |
| Reconciliation due | 0.190 ms | Limit |

These are idle single executions with warm buffers. Cumulative database/WAL counters predate this capture and are retained only as a baseline snapshot. They must not be compared as stage deltas. Sustained matched load and RDS service metrics will determine the capacity result.

## Regression validation

Compose structural checks passed for local-PostgreSQL exclusion, dependency removal, PgBouncer upstream certificate verification, CA mounts and runtime routing. The full suites passed: 75 unit tests and 57 integration tests, with only two existing dependency deprecation warnings per suite. Repository-wide Ruff checks passed with the known Windows Docker executable-bit rule excluded.

## Latest safety stage

The [2026-09-19 fresh-fixture 750 RPS safety stage](2026-09-19-750-safety/README.md)
completed 450,000 responses with one unexpected `503 DATABASE_UNAVAILABLE`.
Worst-worker read/hold p95 was 4.225/37.785 ms with zero generator drops.
An exact post-drain audit found all 22,499 acknowledged holds durable, zero
overlapping seat intervals and empty queues. Availability and unattended-workflow
gates failed, so this is not a passing capacity level. The highest repeatable
clean baseline remains 600 RPS for 30 minutes.

ADR 0041 amends the unattended runner so future load SSH timeouts still collect
completed evidence and attempt the post-TTL audit. The next stage remains a
fresh 750 RPS ten-minute safety test only after a reproducible deployment and
healthy maintenance/reconciliation pools. No 30-minute or 800 RPS stage is
authorized by the 2026-09-19 result.

## 2026-09-20 matched-revision safety control

The [new 750 RPS, ten-minute control](2026-09-20-750-clean-workload/README.md)
completed all 450,000 responses with zero unexpected responses or drops,
22,500 successful holds, read/hold p95 8.503/51.384 ms and exact post-expiry
integrity with zero overlaps or queue backlog. It still failed the unattended
execution gate: the operator SSH call timed out after the generator had finished.
ADR 0043 changes that long-call mechanism. Until the new workflow passes a
fresh stage and the 30-minute confirmation passes, 600 RPS remains the highest
repeatable clean 30-minute baseline.

## 2026-09-20 detached-job safety result

The [matched-revision 750 RPS detached-job stage](2026-09-20-750-detached-failure/README.md)
completed 450,000 responses but returned five unexpected hold 503s in one
brief database-pool/commit spike. Exact durability, zero double-booking,
queue drain and rollback passed, and the new short-poll workflow completed
without an SSH timeout. The strict availability gate failed, so no longer
or higher-rate stage followed. The highest repeatable clean 30-minute
baseline remains 600 RPS.
## 2026-09-20 100 ms WAL controls

The [matched 600 and 750 RPS, three-minute diagnostics](2026-09-20-100ms-wal-controls/README.md)
passed all short-stage safety gates with exact post-expiry audits and zero
booking overlap. At 750 RPS, however, 31 successful commits exceeded 100 ms;
the longest took 322.913 ms. A 100 ms RDS observer sampled up to 15 concurrent
`WALWrite` waiters during the spike, with no WAL-buffer-full event or checkpoint
in either stage. The default Huawei PostgreSQL 17 and 18 parameter exports do
not expose `track_wal_io_timing`. These short runs narrow the commit bottleneck
but do not overturn the failed 750 RPS ten-minute gate or promote capacity.

The [direct RDS versus PgBouncer WAL path probe](2026-09-20-wal-path-ab/README.md) reproduced intermittent long commits without PgBouncer and correlates them with sampled WAL waits (20 September 2026).

The [PID-correlated WAL probe](wal-pid-probe.md) completed a [bounded direct-RDS run](2026-09-20-wal-pid/README.md): every slow commit had a WAL wait sampled on the same backend.

## 2026-09-22 1,000 RPS maintenance-drain diagnostic

The [two-maintenance-worker comparison](2026-09-22-1000-maintenance-drain/README.md)
kept overdue holds below one second at peak in the 15-minute stage and verified
44,990 acknowledged holds with zero booking overlap. The eight-worker generator
delivered all 900,000 scheduled requests. Ten final admission responses and 237
pending seat-refresh requests at the fixed 180-second audit failed the strict
and candidate SLO gates. No 30-minute capacity promotion was made.

## 2026-09-27 1,000 RPS split-maintenance safety diagnostic

The [dedicated refresh/expiry safety stage](2026-09-27-1000-split-maintenance/README.md)
delivered all 300,000 requests with zero generator drops or transport errors,
20.012/90.615 ms worst-worker read/hold p95, exact durability for 14,997 holds,
zero overlap and every queue drained. Three final `ADMISSION_FULL` responses
(0.001%) fail strict capacity certification but remain below the separate 0.01%
recovery-SLO diagnostic budget. This five-minute diagnostic validates the new
background topology; it does not establish sustained 1,000 RPS capacity. A matched
single-retry stage recovered one of three first-attempt rejections and left two
final errors (0.000667%); exact durability and queue drain passed again. More
retries are not adopted.

## 2026-09-27 Redis-first 1,000 RPS safety validation

The [Redis-first safety stage](2026-09-27-redis-first-1000-rps/README.md) delivered all 300,000
no-retry requests with zero errors or drops, 103.682/235.254 ms worst-worker read/hold p95,
exact PostgreSQL durability for all 15,000 HTTP 202 provisional holds, zero ownership overlap
and zero queues at the fixed audit. The passing topology used two reservation writers, two
Kafka consumers, split refresh/expiry workers and eight generator processes. Rollback restored
the PostgreSQL reservation path. This is a five-minute safety result; managed Redis failover,
writer-restart recovery and sustained confirmation remain outstanding.

## 2026-09-27 Redis reservation-writer restart drill

The [live writer-restart drill](2026-09-27-redis-writer-restart/README.md) stopped one of two
reservation writers for 20 seconds during a five-minute 1,000 RPS Redis-first stage. All 15,000
provisional holds became durable with zero broken links, overlap or queue backlog, so recovery
passed. Two generator scheduling drops failed the independent strict capacity gate; this result
does not replace the earlier no-fault 1,000 RPS safety pass.

## 2026-09-27 Huawei DCS switchover drill

The [managed-DCS switchover drill](2026-09-27-dcs-failover/README.md) delivered all 300,000 requests at 500 RPS and observed the Redis endpoint recover in about 500 ms, but the reservation writers had an approximately 129-second persistence gap. Only 12,632 of 14,612 HTTP 202 provisional acknowledgements became durable; 1,980 expired before persistence. Zero overlap, queue drain and rollback passed, but exact durability failed. Redis-first production activation remains blocked pending the bounded recovery change in ADR 0060 and a passing repeat failover drill.


## 2026-09-27 Huawei DCS bounded-recovery repeat

The [bounded writer-recovery drill](2026-09-27-dcs-failover-recovery/README.md) reduced the prior writer stall and persisted all 45 Redis commands within the hold lifetime. Zero overlap, post-TTL cleanup and queue drain passed. The strict activation gate still failed because two hold requests returned HTTP 503 although both commands later became durable, producing 45 durable commands for 43 HTTP 202 acknowledgements. Redis-first activation remains blocked until the API provides an explicit same-idempotency-key resolution path for ambiguous failover outcomes.

## 2026-09-27 Huawei DCS same-key replay drill

The [bounded same-key replay drill](2026-09-27-dcs-same-key-replay/README.md)
recovered both ambiguous hold outcomes and all three transient seat-map errors
during a real managed-DCS switchover. All 45 holds became durable with zero
double-booking or queue backlog, so low-rate recovery correctness passed. The
two durable replay responses took 6.238 and 7.192 seconds. ADR 0062 records that
declared-failover latency as non-gating evidence. This result does not activate
Redis-first for production or certify representative-load failover correctness.

## 2026-09-28 Redis-first write-mix boundary

The [write-mix capacity sequence](2026-09-28-write-mix-ceiling/README.md)
establishes a 15-minute planning point of **1,000 total RPS with 970 seat-map
reads/s and 30 holds/s**. The stage delivered all 900,000 requests with zero
drops or errors, 28.53/51.15 ms worst-worker read/hold p95, exact durability for
27,000 holds, zero overlap and drained queues. A 60 hold/s stage passed for three
minutes but failed over 15 minutes, persisting only 34,781 of 53,997 provisional
holds; 70 hold/s is therefore only a three-minute burst boundary. PostgreSQL
reservation persistence and WAL waits remain the sustained write bottleneck.

## 2026-09-28 Redis-first three-writer 1,000 RPS safety stage

The [three-writer safety stage](2026-09-28-redis-three-writer-1000rps-3m/README.md)
sent all 180,000 requests in three minutes with zero drops or errors,
88.374/188.229 ms worst-worker read/hold p95, exact durability for all 10,800
provisional holds, zero overlap and drained queues. Three reservation writers
reduced average command age to 1.392 seconds and removed the 115 expiries seen
with two writers at the same 6% write mix. This is a short safety result; the
15-minute production planning point remains 1,000 total RPS with 30 writes/s
until a sustained 60-write/s confirmation passes.

## 2026-09-28 Redis-first split-background sustained diagnostic

The [15-minute split-background diagnostic](2026-09-28-redis-split-background-1000rps-15m/README.md)
drained every correctness queue and made all 53,998 delivered holds durable, but
67 generator drops and 161.799/343.946 ms read/hold p95 failed request gates.
The topology is rejected for sustained 60-write/s capacity; the planning point
remains 1,000 RPS with 30 writes/s.
