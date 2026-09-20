# Huawei RDS 750 RPS detached-job safety stage — 2026-09-20

Status: **Failed strict availability.** The detached-job execution, evidence,
post-expiry integrity and rollback gates passed. No 30-minute or 800 RPS stage
was started.

The API, workers and generator used clean worktrees at commit `12f0aa5`.
The operator verified matching full Git commit IDs before deployment. The
topology was four API replicas with DB pool three and hold admission five
per API, PgBouncer pool 24, a fresh 800-show fixture with 300 seats per show,
four no-retry generator workers, 95% conditional availability reads and 5%
unique-seat holds.

| Metric | Result |
|---|---:|
| Offered load | 750 RPS for 600 seconds |
| Completed responses | 450,000 |
| Availability reads | 127,115 HTTP 200; 300,385 HTTP 304 |
| Holds | 22,495 HTTP 201; **5 HTTP 503** |
| Generator drops / transport errors | 0 / 0 |
| Worst-worker read / hold p95 | 7.771 / 47.728 ms |
| Admission rejections | 0 |

The five failed holds occurred within roughly 150 ms at **01:42:22 UTC**.
Bounded API evidence identifies four `PoolTimeout` failures and one
`TooManyRequests`. An API readiness probe also returned one `PoolTimeout`
in that burst; it is separate from the 450,000 generated requests. The
public response remained `DATABASE_UNAVAILABLE`; no retries were used.

The API pool had three connections checked out on three replicas in the
half-second sample preceding the errors. In the 01:42:22.174–22.674 UTC
sample interval, **11 of 25 completed commits exceeded 250 ms**, including
three above 500 ms. In the adjacent half-second intervals, no completed
commit exceeded 100 ms. These are histogram-bucket differences across
four replicas, not individually timed transactions. PgBouncer briefly had
one waiting client, 15 active and zero idle server connections; observed
maximum wait then was 6.104 ms. Its stage-wide sampled wait peak was
27.675 ms. The correlated long commits and occupied API connections explain
the immediate pool exhaustion more directly than a sustained PgBouncer
queue, but do **not** identify why commits slowed.

A read-only PostgreSQL observer sampled zero lock waiters, advancing WAL and
no new checkpoint over the surrounding eight seconds. Two-second samples
can miss a short lock or I/O event. Huawei RDS provider CPU, IOPS and
storage latency were not captured for this window, so an RDS storage or
network root cause is not claimed. Do not increase pool timeouts, retry
holds or raise the load ceiling from this evidence alone.

The exact post-expiry audit matched all 22,495 acknowledged holds to
idempotency records, distinct holds and orders. Broken links, active or
overdue holds, pending orders, and overlapping seat intervals were zero.
Unpublished outbox, pending refresh and dead letters were zero. Admission
was restored to four per API; all APIs were healthy afterward and both
private manifest copies were removed.

The ADR 0043 detached runner received a definitive generator exit,
collected evidence, audited after expiry and rolled back without a long
SSH timeout. Its workflow mechanics passed this failure-path exercise.
**750 RPS is still not a passing capacity level.** The next step is to
obtain RDS CPU, IOPS and storage-latency metrics for this exact spike,
investigate commit latency, and rerun a fresh 750 RPS safety stage only
after an evidence-based correction. The highest repeatable clean
30-minute baseline remains 600 RPS.

Evidence: [stage result](stage-result.json),
[response counts](response-counts.json),
[bounded API causes](api-error-summary.json),
[spike counters](spike-summary.json),
[generator summary](load-summary.json), [preflight](preflight.json),
[durability audit](durability.json), [admission](admission.json) and
[rollback](rollback.json). Raw observer files remain private; no manifests,
bearer tokens, DSNs or request logs are committed.
