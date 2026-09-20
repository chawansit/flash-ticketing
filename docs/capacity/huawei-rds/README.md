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

The [PID-correlated WAL probe](wal-pid-probe.md) is locally validated and ready for a bounded direct-RDS run once ECS access is restored.
