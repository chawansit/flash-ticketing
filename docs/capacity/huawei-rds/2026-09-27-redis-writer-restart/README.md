# Redis reservation-writer restart recovery drill

Date: 27 September 2026  
Revision: `8c687c9`  
Run: `20260927T063157Z-e50dfe28`  
Recovery result: **passed**  
Strict capacity result: **failed because the generator dropped two schedules**

## Fault and workload

The established Redis-first topology ran at 1,000 scheduled requests per second for five minutes with eight generator processes, four API replicas, two reservation writers, two Kafka consumers, and split refresh/expiry workers. No client retry was enabled.

At `06:33:35Z`, one reservation writer was stopped while traffic was active. The peer writer remained running. After 20 seconds the stopped container was started again and returned to `running` at `06:34:01Z`. The fault was not overlapped with any other service interruption.

## Recovery evidence

All 15,000 acknowledged HTTP 202 holds became exactly 15,000 durable PostgreSQL reservation commands. Each had one idempotency record, hold and order. Broken links, pending orders, active or overdue holds, overlapping seat intervals, Redis stream entries, Redis consumer-group pending entries, unpublished outbox events, pending refresh rows and dead letters were all zero at the fixed post-TTL audit.

Admission rejection and first-attempt HTTP failure counts were zero. Worst-worker read p95 was 120.494 ms and hold p95 was 246.460 ms, within the 150/300 ms gates. Rollback restored PostgreSQL reservation mode, zero reservation writers, one combined maintenance worker, one Kafka consumer and four healthy API replicas.

This proves acknowledged provisional holds survived an intentional writer restart in the measured two-writer topology. It does not certify 1,000 RPS capacity for this run: one generator worker dropped two scheduled reads, so only 299,998 of 300,000 scheduled requests were sent and the strict workload gate correctly remained failed. The earlier no-fault five-minute stage remains the strict 1,000 RPS safety pass.

## Evidence

- [stage verdict](stage-result.json)
- [writer restart](writer-restart.json)
- [load summary](load-summary.json)
- [durability audit](durability.json)
- [deployment](deployment.json)
- [admission](admission.json)
- [rollback](rollback.json)

Raw telemetry, credentials and private manifests are excluded. DCS primary failover and the 30-minute confirmation remain outstanding before production activation.
