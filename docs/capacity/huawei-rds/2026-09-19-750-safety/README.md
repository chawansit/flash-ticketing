# Huawei RDS fresh-fixture 750 RPS safety stage - 2026-09-19

Status: **Failed strict availability and unattended-workflow gates.** No 30-minute or 800 RPS stage was started.

The stage used four API replicas, DB pool three and hold admission five per API, PgBouncer pool 24, a fresh 800-show/300-seat development fixture, four no-retry generator workers, 95% conditional seat-map reads and 5% unique-seat holds. Backup had completed before measurement. Preflight found all shows open through the deadline, valid credentials and empty queues.

| Metric | Result |
|---|---:|
| Offered load | 750 RPS for 600 seconds |
| Completed responses | 450,000 |
| Seat-map responses | 427,500 HTTP 304 |
| Hold responses | 22,499 HTTP 201; 1 HTTP 503 |
| Generator drops / transport errors | 0 / 0 |
| Worst-worker seat-map / hold p95 | 4.225 / 37.785 ms |
| Worker exits | 0, 1, 0, 0 |

The single unexpected response was `hold:503:DATABASE_UNAVAILABLE` on worker 1. In 1,320 half-second API samples, DB pool requests waiting peaked at zero and checked-out connections reached three. In 3,300 PgBouncer samples, client waiters and max wait time sampled at zero. These samples cannot exclude a brief pool-acquire timeout; the specific cause of the 503 is **unproven**. PostgreSQL active connections peaked at five, observed WAL increased about 276 MB, and no requested checkpoint occurred. Huawei RDS CPU, IOPS and storage latency were not captured, so no RDS bottleneck claim is made.

The generator wrote its summary at 16:13:05 UTC, but the operator SSH call timed out after 840 seconds. The finalizer restored admission from five to four and removed private manifests, but skipped automatic audit. ADR 0041 records the subsequent evidence-recovery change.

The first manual audit matched all 22,499 acknowledged holds, idempotency records and orders and found zero overlapping seat intervals. It also found 22,499 overdue active holds and 800 pending refresh entries. Maintenance and reconciliation logged `psycopg_pool.TooManyRequests` after earlier dependency downtime. Restarting only those workers reinitialized their pools, and all work drained.

The [final exact audit](durability-final.json) matched all 22,499 acknowledged holds to distinct holds, orders and idempotency records. Broken links, active/overdue holds, pending orders, overlapping seat intervals, unpublished outbox, pending refresh and dead letters were all zero. Integrity passed for this failed availability stage; 750 RPS is **not** a passing capacity level.

The API ECS checkout was based on `c85f440` with local changes and the generator checkout on `675043d` with local changes. Additive orchestration helpers came from the operator workspace. This is not a single-commit production sizing result; existing remote changes were preserved.

Evidence: [generator summary](load-summary.json), [preflight](preflight.json), [final audit](durability-final.json), [rollback](rollback.json). Private manifests, JWTs, credentials and raw per-request logs are excluded from Git.

The next 750 RPS safety attempt requires healthy maintenance/reconciliation pools, bounded candidate API error evidence before rollback, and a reproducible deployment revision. It cannot promote to 30 minutes or 800 RPS until all gates pass.
