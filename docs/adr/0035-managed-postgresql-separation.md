# ADR 0035: Separate PostgreSQL onto managed RDS

Status: Proposed; implementation prepared, cloud validation pending RDS access

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

RDS execution is pending the endpoint, credentials and CA bundle. Local preparation passed Compose structural validation, 75 unit tests, 57 integration tests and repository-wide Ruff checks. The preflight control against isolated PostgreSQL 17.6 passed five migration checksums, 17 required tables and nine required indexes. Its median TCP, connect and transaction times were 0.530 ms, 8.750 ms and 0.693 ms. The credential-free idle profiler completed all six representative plans. Cloud evidence will be stored without endpoints or credentials under `docs/capacity/huawei-rds`.
