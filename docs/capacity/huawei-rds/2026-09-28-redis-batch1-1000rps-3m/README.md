# Redis-first 1,000 RPS / 6% write control

This diagnostic ran on the isolated Huawei ECS, DCS and RDS environment from **09:19 to 09:27 Bangkok time on 28 September 2026**. The API and generator used source commit `c859666`. The workload offered 1,000 RPS for 180 seconds with 6% reservation writes, four generator workers, two reservation-writer replicas, configured batch size 1, admission 5 per API and at most two client attempts. No HTTP retry was used because the run recorded no first-attempt failures.

## Result

The stage **failed** its workload and durability gates. Read and hold latency remained inside their targets and no double-booking was observed, but the reservation persistence lane did not keep pace with the offered writes.

| Measurement | Result |
|---|---:|
| Read p95 (worst worker) | 131.707 ms |
| Hold p95 (worst worker) | 174.686 ms |
| Generator drops | 5,535 |
| Late deliveries | 18,886 |
| Provisional acknowledgements | 10,455 |
| Durable commands | 9,566 |
| Deterministic `HOLD_EXPIRED` failures | 575 |
| Remaining stream entries / pending | 371 / 62 |
| Overlapping hold intervals | 0 |
| Admission rejections | 0 |

The writer observed average command age 41.276 seconds, with histogram p95 at or below 180 seconds. PostgreSQL batch work averaged 81.079 ms; database commit averaged 4.211 ms. The RDS observer sampled at most six interesting waiters, predominantly `WalSync` and `WALWrite`, and a maximum observer query time of 11.197 ms. Global queues other than the reservation stream drained.

## Batch-bound defect found

The configured batch size was not a hard total bound. Redis applies `COUNT` per stream for a multi-stream `XREADGROUP`; the reader polled up to eight event streams at once. A configured value of 1 therefore produced an observed average batch of 1.765 commands and a p95 bucket of 8. This invalidates the run as the canonical batch-size-1 comparison, while retaining it as evidence that the persistence lane and TTL gate need further work.

The follow-up correction reads each selected stream separately while carrying one total remaining budget. It prevents fetched-but-unreturned messages from being assigned to the consumer and adds a regression test across multiple streams. The focused unit/integration selection passed 15 tests. The complete isolated Compose confirmation passed 230 tests with two dependency deprecation warnings. An earlier full invocation in the same disposable project failed only its two HTTP end-to-end tests because the API services had not yet been started; the corrected canonical invocation started and seeded those services before the 230-test pass.

## Evidence

- [Stage result](stage-result.json)
- [Generator summary](generator/public/load/summary.json)
- [Durability audit](backend/public/durability.json)
- [Writer metrics](backend/public/reservation-writer-metrics.json)
- [Writer failures](backend/public/reservation-writer-summary.json)
- [RDS wait summary](backend/public/rds-waits-summary.json)
- [Deployment](backend/public/deployment.json) and [rollback](backend/public/rollback.json)

No credential-bearing URL, password, authorization header or private manifest is stored in this directory.