# Huawei RDS fresh-fixture 750 RPS safety stage — 2026-09-20

Status: **Failed the unattended execution gate.** Traffic, availability,
latency and exact post-expiry integrity gates passed. No 30-minute or 800 RPS
stage was started.

The API, background workers and generator used clean worktrees at commit
`d80beb9`. The RDS environment file and TLS material remained private on
the backend ECS. The measured topology was four API replicas, DB pool three
per replica, PgBouncer pool 24, hold admission five per API, a fresh 800-show
fixture with 300 seats per show, four no-retry generator workers, 95% conditional
availability reads and 5% unique-seat holds. The prior local modifications on
both ECSs were preserved in separate checkouts.

| Metric | Result |
|---|---:|
| Offered load | 750 RPS for 600 seconds |
| Completed responses | 450,000 |
| Availability reads | 163,523 HTTP 200; 263,977 HTTP 304 |
| Seat holds | 22,500 HTTP 201 |
| Unexpected responses / generator drops / transport errors | 0 / 0 / 0 |
| Worst-worker read / hold p95 | 8.503 / 51.384 ms |
| Hold admission rejections | 0 |

All four generator workers exited zero and the coordinator wrote a passing
workload summary at 01:14:01 UTC. The long-lived operator SSH call stayed open
after that summary and timed out at 840 seconds. ADR 0043 records the decision
to replace that transport mechanism with a detached, bounded generator job and
short status polls. The new mechanism was implemented *after* this run, so this
run cannot validate it or establish 750 RPS as a fully passing unattended stage.

The exact post-expiry audit matched all 22,500 acknowledged holds to
idempotency records, distinct holds and orders. Broken links, active or overdue
holds, pending orders and overlapping seat intervals were all zero. Unpublished
outbox, pending refresh and dead-letter queues were zero. All four API error
excerpts recorded no error codes or database-unavailable causes. Admission was
restored to four per API and private manifests were removed.

Huawei RDS provider CPU, IOPS and storage-latency metrics were not captured
for this window; PostgreSQL/RDS bottleneck attribution is therefore not
claimed. The highest repeatable clean 30-minute baseline remains 600 RPS.
A new 750 RPS ten-minute safety stage using ADR 0043 must pass *all* gates
before a 30-minute confirmation or 800 RPS stage.

Evidence: [stage result](stage-result.json),
[generator summary](load-summary.json), [preflight](preflight.json),
[durability audit](durability.json), [admission](admission.json),
[API error summary](api-errors.json) and [rollback](rollback.json).
The retained files contain no manifests, bearer tokens, DSNs or raw request
logs.
