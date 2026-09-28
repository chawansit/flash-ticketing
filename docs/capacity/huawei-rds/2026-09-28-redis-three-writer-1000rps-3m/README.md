# Redis-first three-writer 1,000 RPS safety stage

This controlled Huawei ECS, DCS and RDS stage ran from **11:38 to 11:48 Bangkok time on 28 September 2026** using source revision 35a5a3e. The measured load was 1,000 aggregate RPS for 180 seconds with 94% seat-map reads, 6% reservation writes, eight generator processes, four API replicas, three reservation writers, a hard batch limit of four and no client retry.

## Result

Every gate passed. The generators sent all 180,000 scheduled requests with zero drops, late deliveries, transport errors, first-attempt failures or retries. Worst-worker read p95 was 88.374 ms and hold p95 was 188.229 ms.

All 10,800 HTTP 202 provisional reservation commands became durable PostgreSQL holds and orders with exact idempotency linkage. There were no terminal writer failures, broken links, overlapping seat-ownership intervals or admission rejections. Reservation streams, outbox, refresh and dead-letter queues were all zero at the fixed audit. Rollback restored PostgreSQL reservation mode, four APIs and zero reservation writers.

| Measurement | Result |
|---|---:|
| Scheduled / sent requests | 180,000 / 180,000 |
| Effective read / write rate | 940 / 60 RPS |
| Generator drops / transport errors | 0 / 0 |
| Read p95 / hold p95 | 88.374 / 188.229 ms |
| Provisional / durable commands | 10,800 / 10,800 |
| Average command age | 1.392 s |
| Command-age p95 upper bucket | 5 s |
| PostgreSQL persistence batches | 2,773 |
| Effective commands per PostgreSQL batch | 3.895 |
| PostgreSQL batch / commit average | 107.099 / 3.827 ms |
| Maximum sampled RDS interesting waiters | 7 |
| Maximum refresh backlog / final | 701 / 0 |
| Maximum Kafka lag / final | 3,593 / 0 |
| Overlapping hold intervals | 0 |

## Interpretation

ADR 0067 removed the idle discovery repair loop. Compared with the preceding two-writer stage, generator drops fell from 673 to zero and average command age fell from 23.741 to 1.392 seconds. Adding the third writer eliminated all 115 hold-expiry failures at the same 6% write mix.

This is a **three-minute safety and burst result**. It validates 1,000 RPS with 60 reservation writes per second for this isolated topology and window. It does not establish a sustained production capacity level; the existing 15-minute planning point remains 1,000 RPS with 30 reservation writes per second until a longer confirmation passes.

## Evidence

- [Stage result](stage-result.json)
- [Generator summary](generator/public/load/summary.json)
- [Durability audit](backend/public/durability.json)
- [Writer metrics](backend/public/reservation-writer-metrics.json)
- [Writer failures](backend/public/reservation-writer-summary.json)
- [RDS wait summary](backend/public/rds-waits-summary.json)
- [Refresh pipeline](backend/public/refresh-pipeline-summary.json)
- [Drain trace](backend/public/drain-trace-summary.json)
- [Deployment](backend/public/deployment.json) and [rollback](backend/public/rollback.json)
