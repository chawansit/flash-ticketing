# Huawei RDS capacity validation

ADR 0035 prepared a matched test that moves PostgreSQL from the shared backend ECS to Huawei RDS while retaining local PgBouncer and an aggregate application pool budget of twelve. Cloud execution is now complete through the first failed staged gate.

The latest [admission-8 staged report](2026-09-13-admission8-stages/README.md) establishes **500 RPS for 30 minutes** as the highest clean operating point measured for the 95% conditional seat-map read / 5% unique-seat hold workload. All 900,000 requests completed with zero errors or drops; read p95 was 8.420 ms, hold p95 was 46.999 ms and all 45,000 acknowledged holds passed the durable overlap audit. The 600 RPS stage failed because the generator dropped one late read, while all 54,000 holds and backend correctness checks passed. Escalation stopped before 750 RPS.

The earlier [400 RPS RDS control](2026-09-13-control/README.md) used admission six and recorded one `ADMISSION_FULL` response. The matched admission-eight control completed 720,000 requests with zero errors or drops. The database connection budget remained unchanged.

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

Repeat 600 RPS with generator scheduling telemetry and Huawei RDS CPU, IOPS, WAL, checkpoint and storage-latency exports. Proceed to 750/1,000 only after a clean 600 RPS repeat. Full commands and failure gates are in `docs/rds-runbook.md`.
