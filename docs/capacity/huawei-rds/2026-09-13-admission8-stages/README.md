# Huawei RDS admission-8 staged validation — 2026-09-13

Status: **500 RPS passed every measured gate for 30 minutes. The 600 RPS stage failed the strict zero-drop gate, so escalation stopped before 750 RPS.**

This result establishes a sustained operating point for the measured 95% conditional seat-map read / 5% unique-seat hold workload. It is not a maximum backend capacity, a payment/Kafka throughput result or a complete production qualification.

## Topology and controlled change

- API ECS: Huawei `c6.xlarge.2`, 4 vCPU / 8 GiB.
- Generator ECS: Huawei `c6.2xlarge.2`, 8 vCPU / 16 GiB.
- Huawei RDS for PostgreSQL 17.11: 4 vCPU / 16 GiB / 100 GB.
- Two API replicas behind Nginx `least_conn`.
- Six application database connections per API replica and 12 PgBouncer server connections in aggregate.
- Admission allowance increased from six to eight per API process. The database connection budget did not change.
- 800 shows, 300 seats per show and 8,000 viewers.
- Four generator workers, five-second client keep-alive expiry and no automatic retry.
- Validation connected with `sslmode=require`. CA and hostname verification remain required before production qualification.

The 400 RPS control immediately before this experiment used admission six and failed because one of 36,000 holds returned `503 ADMISSION_FULL`. See the [control report](../2026-09-13-control/README.md).

## Staged result

| Offered load | Duration | Scheduled | Completed | Holds returning 201 | Drops | HTTP/transport/task errors | Worst read p95 | Worst hold p95 | Gate |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 400 RPS | 30 min | 720,000 | 720,000 | 36,000 | 0 | 0 | 8.114 ms | 42.510 ms | Pass |
| 500 RPS | 30 min | 900,000 | 900,000 | 45,000 | 0 | 0 | 8.420 ms | 46.999 ms | Pass |
| 600 RPS | 30 min | 1,080,000 | 1,079,999 | 54,000 | 1 late drop | 0 | 8.324 ms | 48.635 ms | Fail |

All four workers exited zero at 400 and 500 RPS. At 600 RPS, worker 1 recorded one scheduler `late` drop between its 234,000 and 238,500 progress samples and exited one as required by the strict gate. Its scheduling-lag p95 was 1.065 ms and completed rate was 149.9997 RPS. There was no request, transport or task error. The single missing request was a read; all 54,000 scheduled holds completed with 201.

Exact-window container log scans found zero `ADMISSION_FULL` events on both API replicas in all three stages. Cumulative process metrics covered exactly 135,000 admitted holds, equal to the three stage totals. Each replica observed one hold arrival above occupancy six and at or below eight. This is evidence that the allowance of eight absorbed two microbursts without increasing the database pool.

The 600 RPS result does not demonstrate a backend saturation point because its only failed gate was generator scheduling. It also cannot be accepted as a clean operating point under the zero-drop criterion. Per the staged test rule, 750 and 1,000 RPS were not run.

## PostgreSQL and pool observations

The mid-run and post-run PostgreSQL collectors passed all six representative query plans at every stage. Samples showed 12–13 database backends. Post-stage counters remained at zero for deadlocks, temporary files, fatal sessions, abandoned sessions and killed sessions.

The mid-to-post observation windows, which cover only the latter part of each run, recorded 437,016 commits at 400 RPS, 439,605 at 500 RPS and 468,754 at 600 RPS. Each comparison also recorded one rollback, matching the collector cadence; no generator or application error accompanied it. These deltas are diagnostic context and are not full-stage transaction counts.

Across the lifetime of the two admission-8 API containers, all 137,570 recorded pool acquisitions had outcome `ok`. About 98.5% fell in the histogram bucket at or below 50 ms, and all fell at or below 500 ms. The final pool sample showed zero waiting requests, five available connections per replica and a configured maximum of six per replica. These metrics are cumulative across the three stages rather than isolated per-stage histograms.

Manual steady-state samples at 600 RPS showed the API replicas near 42–43% of one vCPU each, PgBouncer near 18% and Redis near 19%. An earlier 72% / 42% API sample returned to a balanced 47% / 45% on the next observation. Docker reports 100% as one vCPU. Huawei RDS CPU, IOPS, WAL/storage latency and checkpoint service metrics were not exported, so this evidence does not establish the RDS saturation margin.

## Durability, expiry and queues

Post-expiry verification passed for every stage:

| Offered load | Acknowledged holds | Idempotency records | Distinct holds | Distinct orders | Broken links | Overlapping intervals | Active/overdue holds | Pending orders |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 400 RPS | 36,000 | 36,000 | 36,000 | 36,000 | 0 | 0 | 0 | 0 |
| 500 RPS | 45,000 | 45,000 | 45,000 | 45,000 | 0 | 0 | 0 | 0 |
| 600 RPS | 54,000 | 54,000 | 54,000 | 54,000 | 0 | 0 | 0 | 0 |

After each expiry drain, unpublished outbox events, pending seat refresh work and dead letters were all zero. The overlap audit uses the selected load run IDs and order-created-to-hold-expiry intervals. This workload did not exercise payment callbacks or Kafka fulfillment.

## Decision and next experiment

Use **500 RPS** as the highest clean 30-minute operating point demonstrated by this topology and workload. Retain admission eight while keeping the aggregate database connection budget at 12. This is consistent with the existing admission decision; no architectural pattern changed in this experiment, so no new ADR is required.

Before raising the accepted point, repeat 600 RPS with generator event-loop/OS scheduling telemetry and enough generation margin to determine whether the one late drop is reproducible. A clean repeat may proceed to 750 RPS; any repeated drop, unexpected response, latency breach or correctness failure stops escalation. Export Huawei RDS CPU, IOPS, storage latency, WAL and checkpoint metrics for the repeat before making an RDS sizing claim.

## Evidence

- `rds-{400,500,600}-admission8/summary.json` and worker files: generator accounting, status, latency and transport results.
- `rds-{400,500,600}-admission8-durability.json`: persistence, expiry, overlap and queue audits.
- `rds-{400,500,600}-admission8-postgres-{mid,post}.json`: PostgreSQL counters and representative plans.
- `rds-admission8-runtime.json`: exact-window admission log counts, cumulative API pool/admission metrics and the final resource snapshot.

Private manifests, tokens and database credentials are excluded.
