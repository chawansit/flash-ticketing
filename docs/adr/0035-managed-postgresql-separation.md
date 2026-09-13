# ADR 0035: Separate PostgreSQL onto managed RDS

Status: Accepted for staged capacity validation; production qualification pending verified TLS and RDS service telemetry

## Context

The verified two-API topology passes 600 RPS for 30 minutes while API, Nginx, PostgreSQL, PgBouncer, Redis, Kafka and workers share one 4-vCPU backend ECS. PostgreSQL averaged 41.203% of one core and the two APIs together averaged 108.645% of one core. A separate database is required to distinguish shared-host CPU and disk interference from transaction design before raising the capacity gate.

The current application connects through a local PgBouncer in transaction mode. Application pools and hold admission are bounded to twelve in aggregate. Migrations connect directly to PostgreSQL and acquire a transaction advisory lock. The seat hold transaction, PostgreSQL uniqueness constraints and idempotency records remain correctness authorities; moving the server must not change those boundaries.

## Decision

Keep PgBouncer on the backend ECS and move only PostgreSQL to a Huawei RDS for PostgreSQL primary/standby instance in the same region and VPC. This preserves transaction pooling and the existing application connection topology for a matched comparison. Disable the Compose PostgreSQL service through an explicit profile when the RDS override is active. Point migrations directly at RDS and all runtime processes at local PgBouncer.

Use private networking and upstream TLS. Prefer `verify-full` with the Huawei CA bundle and the RDS hostname. Permit `require` only as a documented, temporary connectivity diagnostic; it is not the accepted production setting. Store RDS endpoints and credentials in an ignored environment file and never commit connection strings, passwords, CA private material or development bearer manifests.

Keep the aggregate application DB pool and hold-admission budgets at twelve during the comparison. PgBouncer backend pool size remains bounded. Do not add a read replica to the first experiment because seat holds and their supporting reads must use the primary and the current seat-map read path is served from Redis.

Before migration, run a read-only preflight that verifies DNS, TCP, TLS, PostgreSQL version, primary/read-write state, timezone, encoding, connection limit and basic round-trip/transaction timings without emitting credentials. Run migrations once through a direct RDS connection, then verify migration checksums, required tables and required indexes. Seed only the isolated development fixture.

Run matched 400 RPS control, then 500 and 600 RPS for 30 minutes. If all gates pass, continue to 750 and 1,000 RPS, stopping at the first failed gate. Keep the API topology, aggregate connection budget, generator, workload mix, keep-alive values and no-retry policy fixed. Compare network, pool acquisition, query, transaction body, commit, WAL/checkpoint and RDS CPU/storage metrics. After every stage require exact acknowledged persistence, zero overlapping held-seat intervals and drained queues.

## Alternatives considered

- Connect every application process directly to RDS. This removes PgBouncer and would confound the first matched comparison while multiplying RDS client connections as replicas scale.
- Run PostgreSQL on the generator ECS. That makes the generator and database compete for CPU, memory, disk and network and invalidates load-generation independence.
- Add a read replica immediately. The dominant seat-map path already uses Redis, and replica lag cannot safely authorize seat holds.
- Copy the existing local PostgreSQL volume. RDS does not consume the Docker volume format; logical migration is safer and the benchmark fixture can be recreated deterministically.
- Increase connection limits with the move. That would mix topology and concurrency changes and could hide queueing by moving it into RDS.

## Consequences

Database calls gain a private-network hop and TLS overhead while the backend ECS releases PostgreSQL CPU, memory and disk work. PgBouncer remains a single local component and a possible bottleneck; its queues must be measured. A primary/standby RDS can have different commit latency from the local single-node database, so lower host contention does not guarantee lower transaction latency.

The RDS application role needs ordinary DML access to the ticketing schema. The migration role may own schema objects or hold narrowly scoped DDL privileges. Using separate roles is preferred, but the initial isolated benchmark may use one temporary owner role if the limitation is recorded and the credential is rotated afterward.

## Failure and recovery behavior

If DNS, TCP, TLS, version, read-write state or schema verification fails, do not start API traffic. If migration fails, keep the RDS target isolated, preserve the migration error and continue using the local database. Migration files are immutable and checksum-verified; never edit an applied migration.

If RDS becomes unavailable during a hold, Redis shielding must be released and no idempotency/hold/order record may remain for the failed attempt. After RDS recovers, the same idempotency key may be retried. If PostgreSQL commits but the response is lost, replaying the same key must return the stored response. API readiness must fail while its database dependency is unavailable. No automatic dual-write or failover to local PostgreSQL is allowed because two writable authorities could double-book seats.

Rollback is a deployment rollback, not data replication: stop traffic, remove the RDS override and recreate the isolated local stack only when returning to its own test dataset. A production rollback after accepting RDS writes requires an explicit data migration plan and is outside this benchmark.

## Validation evidence

Cloud execution used Huawei RDS for PostgreSQL 17.11 with 4 vCPU, 16 GiB memory and 100 GB storage. The effective `max_connections` setting was 768. Infrastructure preflight passed primary/read-write state, UTF-8, TLS 1.3, connectivity and transaction probes. Schema verification passed five migration checksums, 17 required tables and nine required indexes. The idle collector retained all six representative query plans.

The matched admission-eight stages are retained in the [2026-09-13 report](../capacity/huawei-rds/2026-09-13-admission8-stages/README.md). At 400 RPS, all 720,000 requests completed with zero errors or drops. At 500 RPS, all 900,000 requests completed with zero errors or drops; worst-worker read p95 was 8.420 ms and hold p95 was 46.999 ms. All 45,000 acknowledged holds matched durable idempotency, hold and order records with zero overlapping intervals, and all queues drained.

The 600 RPS stage processed 1,079,999 of 1,080,000 scheduled requests. One generator worker recorded one late read drop, so the strict gate failed and escalation stopped before 750 RPS. There were no HTTP, transport or task errors, all 54,000 holds returned 201 and the durability/overlap audit passed. This is a generator-limited test boundary, not proof that RDS or the backend saturated.

The telemetry-instrumented [600 RPS repeat and 750 RPS boundary test](../capacity/huawei-rds/2026-09-13-600-repeat-750-stage/README.md) supersedes the capacity conclusion, while leaving this architectural decision unchanged. The 600 RPS repeat completed all 1,080,000 requests with zero drops or unexpected errors; worst-worker read p95 was 8.439 ms and hold p95 was 48.778 ms. All 54,000 acknowledged holds passed durability, expiry and overlap checks. At 750 RPS, three hold requests returned `ADMISSION_FULL` and three reads were dropped by the unchanged 50 ms generator lateness gate. The successful 67,497 holds remained durable with zero overlap and drained queues. Escalation stopped before 1,000 RPS, establishing 600 RPS as the highest clean measured point for this fixed topology and workload.

The follow-up [750 RPS sub-second diagnostic](../capacity/huawei-rds/2026-09-13-750-subsecond-diagnostic/README.md) corrected the premeasurement start-delay/TTL boundary and recorded zero seat-map warming. The strict gate still failed with three `ADMISSION_FULL` responses, one `DATABASE_UNAVAILABLE` response and five generator late drops. A 200 ms sample showed both replicas simultaneously at pool 6/6 with two acquirers; 12 ms later the database failure completed after 150.518 ms in `db_enter`, matching the configured 150 ms pool timeout. All 67,496 acknowledged holds passed durability and overlap checks. This evidence leaves the managed-PostgreSQL decision unchanged and keeps 600 RPS as the clean measured point.

The experiment used temporary `sslmode=require` without CA/hostname verification at the user's direction. `verify-full` remains required for production qualification. Huawei RDS CPU, IOPS, WAL/checkpoint and storage-latency metrics were not exported, so the accepted evidence establishes functional use of managed RDS and a 600 RPS operating point for this workload, but not the RDS saturation margin or maximum production capacity.
