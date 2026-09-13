# Huawei RDS 400 RPS control — 2026-09-13

Status: **performance and correctness targets met, workload gate failed due to one admission rejection**.

## Topology

- API ECS: Huawei `c6.xlarge.2`, 4 vCPU / 8 GiB.
- Generator ECS: Huawei `c6.2xlarge.2`, 8 vCPU / 16 GiB.
- RDS for PostgreSQL 17.11: 4 vCPU / 16 GiB / 100 GB.
- Two API replicas with six application connections each.
- PgBouncer transaction pool fixed at 12 server connections in aggregate.
- 800 shows, 300 seats per show and 8,000 viewers.
- 400 offered RPS for 1,800 seconds: 95% conditional seat-map reads and 5% unique-seat holds.
- Four generator workers at 100 RPS each; five-second client keep-alive expiry.
- The temporary validation connection used `sslmode=require`. CA and hostname verification remain required before production qualification.

The RDS instance reported `max_connections=768`. The provider's displayed recommended maximum of 1,600 was not the effective setting of this instance.

## Infrastructure and schema gates

The infrastructure preflight passed against PostgreSQL 17.11, a read/write primary, UTF-8 and TLS 1.3. Five connection samples had p95 30.77 ms. Ten empty transaction samples had p95 5.96 ms. TCP p95 from the API ECS was 1.60 ms.

All five migration checksums, 17 required tables and nine required indexes passed. The idle comparison retained the expected plans for all six representative queries. Candidate client timings include the private network round trip and were about 1.31–2.15 ms; server execution times were 0.023–0.041 ms.

## Load result

The generator scheduled all 720,000 requests with zero drops and zero transport errors. Worst-worker read p95 was 7.37 ms and hold p95 was 39.73 ms. Of 36,000 hold requests, 35,999 returned 201 and one returned `503 ADMISSION_FULL`.

The rejection occurred on one API replica at `2026-09-13T02:37:42Z`. Its hold occupancy was exactly 6/6 and the response completed in 0.15 ms before a database transaction began. That replica admitted 18,090 other holds. Its arrival histogram shows that almost all holds arrived with occupancy 0–2; only two observations reached the 5–6 bucket. Nginx already used least-connections balancing.

The run therefore fails the zero-unexpected-response gate. It must not be called an accepted 400 RPS operating point despite the latency results.

## Durability and queues

Post-expiry verification matched all 35,999 acknowledged holds to 35,999 idempotency records, distinct holds and distinct orders. Broken links, active overdue holds, pending orders and overlapping seat intervals were all zero. Unpublished outbox, pending seat refresh and dead-letter queues drained to zero.

Database snapshots taken during the run showed zero deadlocks, zero temporary files, one unchanged historical rollback and no abandoned, fatal or killed sessions. Application ECS samples showed each API using about 28–31% of one vCPU, PgBouncer about 13–15% and Redis about 13–16%. Huawei Cloud RDS CPU, storage latency and IOPS were not exported for this run, so these observations do not establish the RDS saturation margin.

## Next controlled experiment

Test an admission limit of eight per API replica while keeping each application pool at six and the aggregate PgBouncer pool at 12. This changes only the bounded in-process waiting allowance; it does not increase database connections. Run a fresh 400 RPS, 30-minute control and accept the candidate only if unexpected responses, transport errors and generator drops are zero, latency remains within target, correctness passes and every queue drains. If it passes, proceed to 500 RPS. If pool waits or latency rise materially, replace the tuning candidate with a bounded queue design and record that pattern in a new ADR before implementation.

## Evidence

- `rds-infrastructure-preflight-2.json`: connectivity, TLS and transaction samples.
- `rds-schema-preflight.json`: migration/schema verification.
- `rds-postgres-baseline-2.json`: PostgreSQL statistics and six execution plans.
- `rds-idle-comparison.json`: local-versus-RDS plan comparison.
- `rds-400-control/summary.json` and worker files: generator results.
- `rds-400-control-durability.json`: persistence, expiry, overlap and queue audit.