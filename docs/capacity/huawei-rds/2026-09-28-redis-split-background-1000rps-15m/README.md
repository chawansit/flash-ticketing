# Redis-first split-background 1,000 RPS sustained diagnostic

This 15-minute Huawei diagnostic used revision de30607 at 1,000 RPS with 6% reservation writes, eight generators, four APIs, three reservation writers, two Kafka consumers and dedicated refresh and expiry workers.

The stage failed request fidelity and latency: 899,933 of 900,000 requests were sent, with 67 generator drops. Worst-worker read/hold p95 were 161.799/343.946 ms. There were no HTTP, transport or admission errors and no retries.

Correctness and drain gates passed. All 53,998 delivered provisional commands became durable, double-booking was zero, and Kafka, refresh, expiry, outbox, dead-letter and reservation-stream queues ended at zero. RDS sampling saw at most nine interesting waiters, including up to eight concurrent WALWrite waits. Rollback passed.

The split lanes fix the downstream backlog seen with one combined maintenance worker, but the added concurrency misses the request SLO. This topology is rejected for sustained 60-write/s production capacity. The confirmed 15-minute planning point remains 1,000 total RPS with 30 reservation writes per second.

Evidence: [stage result](stage-result.json), [generator summary](generator/public/load/summary.json), [durability](backend/public/durability.json), [writer metrics](backend/public/reservation-writer-metrics.json), [pipeline](backend/public/refresh-pipeline-summary.json), [RDS waits](backend/public/rds-waits-summary.json), and [rollback](backend/public/rollback.json).

