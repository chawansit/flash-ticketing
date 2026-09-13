# Huawei RDS 600 RPS repeat and 750 RPS boundary — 2026-09-13

Status: **600 RPS passed every measured gate for 30 minutes. The 750 RPS stage failed the strict zero-error and zero-drop gate. Escalation stopped before 1,000 RPS.**

This follow-up resolves the single generator drop in the earlier 600 RPS run and establishes 600 RPS as the highest clean sustained point measured for this topology and workload. It is not a general maximum, a payment/Kafka throughput result or a production SLA guarantee.

## Fixed topology and workload

- API ECS: Huawei `c6.xlarge.2`, 4 vCPU / 8 GiB.
- Generator ECS: Huawei `c6.2xlarge.2`, 8 vCPU / 16 GiB.
- Huawei RDS for PostgreSQL 17.11: 4 vCPU / 16 GiB / 100 GB.
- Two API replicas behind Nginx `least_conn`.
- Six application DB connections per API replica, 12 PgBouncer server connections in aggregate and admission allowance eight per API process.
- 800 shows, 300 seats per show and 8,000 viewers.
- Four generator workers, 95% conditional seat-map reads, 5% unique-seat holds, five-second client keep-alive expiry and no automatic retry.
- Temporary `sslmode=require`; CA and hostname verification are still required before production qualification.

## Result

| Offered load | Duration | Scheduled | Completed | Holds 201 | Unexpected HTTP | Generator drops | Worst read p95 | Worst hold p95 | Gate |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 600 RPS repeat | 30 min | 1,080,000 | 1,080,000 | 54,000 | 0 | 0 | 8.439 ms | 48.778 ms | Pass |
| 750 RPS | 30 min | 1,350,000 | 1,349,997 | 67,497 | 3 `ADMISSION_FULL` | 3 late reads | 7.778 ms | 51.221 ms | Fail |

At 600 RPS, all worker exit codes were zero, start skew was 0.415 ms, transport/task errors were zero and every accounting gate passed. Worker scheduling-lag p95 was 1.059–1.065 ms; the largest observed lag was 39.840 ms, below the fixed 50 ms drop boundary.

At 750 RPS, one worker passed and three failed as required by the strict gate. Three hold requests returned `503 ADMISSION_FULL`; exact-window container logs found one event on API replica 2 and two on replica 3. Three reads were not scheduled because generator lag reached 51.139–54.919 ms in the last 25 seconds. Request accounting still balanced: completed responses plus drops equaled scheduled work. The latency targets passed, but the zero-error and zero-drop requirements did not.

Two earlier 750 launches aborted before measurement because their bootstrap reads received `SEATMAP_WARMING` after the maps expired between stages. They generated no measured traffic. The final run used the existing bounded warmup routine immediately before bootstrap; its retained evidence shows 224 missing/stale maps seeded and 800/800 post-ready.

## Resource and persistence evidence

During the exact 600 RPS window, generator host CPU busy was 14.304% at p95 and 28.141% maximum. The four load workers plus coordinator used 111.0% of one CPU at p95 and 124.001% maximum. CPU and memory PSI remained zero, steal was zero, iowait peaked at 0.879%, and network error/drop deltas were zero.

During the observed portion of the 750 RPS window, generator host CPU busy was 17.691% at p95 and 23.618% maximum. Load-process CPU was 137.997% of one CPU at p95 and 183.002% maximum. CPU, memory and IO PSI remained zero, steal was zero, iowait peaked at 0.376%, and network error/drop deltas were zero. The generator observer ended 35.034 seconds before the load window ended, so it did not capture the three late drops. Worker-local scheduling timestamps do capture those drops; the available OS telemetry cannot attribute their cause.

The backend observer covered both measurement windows without errors. At 600 RPS it observed at most 13 DB connections, 10 active connections, zero lock waiters, zero sampled API pool waiters and at least one free connection on the observed replica. At 750 RPS the corresponding values were 13, nine, zero, zero and one. Redis had zero missing maps and zero maps without TTL during both exact load windows. These two-second samples can miss sub-second admission and pool spikes; the three application and log `ADMISSION_FULL` records at 750 are authoritative evidence that admission reached its bound.

Post-expiry audits passed at both loads. The 600 RPS run had 54,000 acknowledged holds and the 750 RPS run had 67,497; each matched its idempotency, hold and order records exactly. Both runs had zero broken links, active/overdue holds after drain, pending orders and overlapping held-seat intervals. Unpublished outbox events, pending seat refresh work and dead letters were zero.

## Decision

Use **600 RPS** as the highest clean 30-minute operating point demonstrated for this exact topology and workload. Do not claim 750 RPS capacity: it breached both the application admission and generator scheduling gates. Do not run 1,000 RPS under the staged rule.

The next performance change should be evidence-led. Correlate admission occupancy and pool wait histograms at sub-second resolution around the three 750 failures, then create an ADR before changing the admission bound, connection budget or worker topology. Extend generator observation beyond the full coordinated start and drain interval before repeating 750 RPS.

## Evidence

- `rds-600/summary.json` and `worker-*.json`: load accounting, statuses, latency, transport and scheduling results.
- `rds-750/summary.json` and `worker-*.json`: failed-stage accounting and the three bounded late-drop examples.
- `durability.json`: post-expiry persistence, overlap and queue audit.
- `preflight.json` and the 750 `warmup.json`: fixture, credential-window and Redis projection readiness.
- `telemetry-summary.json`: exact-window generator/backend summaries and exact-window API log counts.

Raw observer samples remain on the test ECSs and are omitted from Git because the compact summaries retain the decision-relevant measurements. Private manifests, bearer tokens and database credentials are excluded.
