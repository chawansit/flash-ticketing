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

## Next evidence

ADR 0039 is the active controlled candidate: admission five per API with database pool three unchanged. Run 750 RPS for ten minutes, then a fresh 30-minute confirmation only if every gate passes. Do not proceed to 800 RPS until both stages pass. Full commands and failure gates are in `docs/rds-runbook.md`.
