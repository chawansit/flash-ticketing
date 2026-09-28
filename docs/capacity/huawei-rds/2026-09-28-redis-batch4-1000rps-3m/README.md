# Redis-first batch-size-4 diagnostic with contaminated backlog

This diagnostic ran on the isolated Huawei ECS, DCS and RDS environment from **09:39 to 09:48 Bangkok time on 28 September 2026** using source commit `c43a755`. It offered 1,000 RPS for 180 seconds with 6% reservation writes, four generator workers, two reservation writers, a hard total batch size of 4, admission 5 per API and no client retry.

## Result

The HTTP workload gate passed completely: 180,000 physical requests, zero generator drops, zero late deliveries, zero transport errors and no first-attempt failures. Worst-worker read p95 was 29.572 ms and hold p95 was 54.694 ms. No admission rejection or overlapping hold interval occurred.

The durability result is **invalid as a clean batch-size-4 comparison and failed the strict gate**. The preflight checker only covered PostgreSQL queues and did not reject an existing global DCS reservation backlog from the preceding failed stage. Writers encountered commands as old as 1,502 seconds while the 120-second hold TTL was in force. They spent part of the test processing expired prior commands.

| Measurement | Result |
|---|---:|
| Provisional acknowledgements for this fixture | 10,800 |
| Durable commands for this fixture | 7,350 |
| Writer deterministic `HOLD_EXPIRED` outcomes | 1,916 |
| Remaining global stream entries / pending | 2,133 / 1,108 |
| Observed batch average / p95 upper bound | 2.659 / 4 |
| PostgreSQL batch average | 87.042 ms |
| PostgreSQL commit average | 2.552 ms |
| Maximum RDS interesting waiters | 5 |
| Overlapping hold intervals | 0 |

The follow-up changes the read-only pre-load queue checker to count global `reservation-stream:*` entries and `reservation-writers` pending messages. A stage now stops before traffic when either value is nonzero. Focused tests for this addition and the hard batch bound passed four tests; Ruff passed.

## Evidence

- [Stage result](stage-result.json)
- [Generator summary](generator/public/load/summary.json)
- [Durability audit](backend/public/durability.json)
- [Writer metrics](backend/public/reservation-writer-metrics.json)
- [Writer failures](backend/public/reservation-writer-summary.json)
- [RDS wait summary](backend/public/rds-waits-summary.json)
- [Deployment](backend/public/deployment.json) and [rollback](backend/public/rollback.json)

No production capacity claim is made from this contaminated stage.