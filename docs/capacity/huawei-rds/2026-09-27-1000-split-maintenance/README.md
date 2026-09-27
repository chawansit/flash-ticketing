# 1,000 RPS split-maintenance safety diagnostic, 27 September 2026

**Verdict: the five-minute recovery-SLO diagnostic passed, while strict no-error capacity certification failed. Sustained 1,000 RPS remains unproven.**

Revision `62cb50d` ran four API replicas with admission five per replica, PgBouncer in front of Huawei RDS, two Kafka consumers, one dedicated refresh worker and one dedicated expiry worker. Each dedicated worker had a maximum PostgreSQL pool of two. The generator used four synchronized workers, no client retry, a 95% conditional seat-map read / 5% unique-seat hold mix and a fresh 800-show fixture.

| Measure | Result |
| --- | ---: |
| Scheduled and physically attempted requests | 300,000 |
| Generator drops / late deliveries / transport errors | 0 / 0 / 0 |
| Successful holds | 14,997 |
| Final `ADMISSION_FULL` responses | 3 (0.001%) |
| Worst-worker seat-map / hold p95 | 20.012 / 90.615 ms |
| Durable acknowledged holds | 14,997 |
| Broken links / overlapping hold intervals | 0 / 0 |
| Final outbox / refresh / dead-letter queues | 0 / 0 / 0 |
| Rollback | Passed |

The automation correctly reports `pass: false` because strict certification permits no unexpected response. The separate ADR 0051 diagnostic availability budget is below 0.01%; this run stayed within it and passed every load-fidelity, latency, durability, overlap, fixed-audit, queue and rollback check. A five-minute result is not sustained-capacity evidence.

The split lanes removed the background-processing failure seen in the earlier 30-minute combined-worker run. Refresh generation and completion both increased by 29,994. Pending refresh peaked at 557, its oldest age peaked at 14.975 seconds and the final value was zero. Overdue holds peaked at 19 with a maximum age of 0.319 seconds and ended at zero. Outbox depth peaked at 20 and ended at zero. Kafka lag peaked at 48 and ended at zero.

The remaining errors are admission saturation on the hold path. API logs recorded one rejection on each of three replicas. Successful commit phases occasionally exceeded 100 ms and peaked at 254.560 ms. The RDS observer sampled up to 14 simultaneous interesting waiters, mainly `WALWrite` and `WalSync`, with 463,314,864 WAL bytes, no `wal_buffers_full` increment and no checkpoint during the window. Sampling does not prove that a particular WAL wait caused every admission rejection.

Evidence: [stage verdict](stage-result.json), [load summary](load-summary.json), [admission count](admission.json), [deployment](deployment.json), [durability audit](durability.json), [drain trace](drain-trace-summary.json), [refresh pipeline](refresh-pipeline-summary.json), [RDS waits](rds-waits-summary.json) and [rollback](rollback.json). The four redacted API-error summaries and four slow-phase summaries are retained beside this report. Private manifests, credentials, fixture identifiers and raw high-volume telemetry are excluded.
