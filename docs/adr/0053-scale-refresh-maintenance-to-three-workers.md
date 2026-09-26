# ADR 0053: Scale refresh maintenance to three workers

Status: Accepted for a controlled three-worker recovery-SLO diagnostic; one worker remains the rollback configuration. Strict capacity promotion remains blocked.

This decision supersedes ADR 0050 only for the experimental maintenance replica limit. ADR 0050's database claim, lease, rollback and validation rules remain in force. ADR 0051 remains the authority for the separate recovery-SLO error budget.

## Context

The five-minute 1,000 RPS experiment at revision `d9d3cba` used two Kafka consumers and two maintenance workers. Two consumers removed the event-ingress bottleneck: maximum Kafka group lag fell from 4,000 messages with one consumer to 14, maximum partition lag fell from 1,171 to 7, and the group ended at zero lag.

The refresh stage remained slower than its input. The observer measured 30,000 generated refreshes at 58.524 per second and 29,310 completions at 57.178 per second. Pending refresh peaked at 782 and the fixed audit found 302 pending requests. The oldest refresh reached 184.4 seconds. PostgreSQL durability passed for all 15,000 acknowledged holds, overlapping held-seat intervals were zero, unpublished outbox and dead letters were zero, and overdue active holds returned to zero. This isolates the remaining queue failure to maintenance refresh throughput rather than Kafka delivery.

## Decision

Run a controlled experiment with three `maintenance` replicas, two Kafka consumers, four API replicas and admission five per API. Keep the six Kafka partitions, PgBouncer budget, workload, hold TTL, bounded retry settings and fixed post-TTL audit unchanged. Restore one maintenance replica, one consumer and admission four after every outcome.

The existing PostgreSQL work-claim design remains unchanged. Refresh workers claim leased requests with `FOR UPDATE SKIP LOCKED`; expiry workers claim orders with `FOR UPDATE SKIP LOCKED`. Start with a five-minute 1,000 RPS safety run. Strict capacity promotion requires zero final unexpected errors plus request accounting, latency, durability, zero overlap, zero queues and rollback. A separately labeled 15-minute recovery-SLO diagnostic may proceed under ADR 0051 only when final unexpected outcomes remain below 0.01% and every correctness, queue and cleanup gate passes.

## Alternatives considered

- Keep two maintenance workers and extend the drain allowance: rejected because it would hide a measured throughput deficit.
- Add more Kafka consumers: rejected because two consumers already reduced maximum total lag to 14 and ended at zero.
- Rewrite refresh processing as a bulk transaction now: deferred because it changes transaction and recovery scope before testing available safe parallelism.
- Split expiry and refresh into separate worker services: deferred until the three-worker trace shows whether shared scheduling still causes starvation.
- Increase admission or API replicas: rejected because sampled RDS sessions already wait on WAL synchronization and a higher permit count may increase contention.

## Consequences

A third maintenance process consumes another database connection and CPU share. PostgreSQL may become the next limit, so the experiment must retain pool-wait, lock-wait and transaction telemetry. The added process provides enough refresh throughput in the five-minute measurement, but the additional WAL contention can produce rare admission failures. No strict capacity claim follows from a recovery-SLO result.

## Failure and recovery behavior

If the third worker fails, its PostgreSQL transaction rolls back; an uncommitted expiry can be claimed by another worker and an uncompleted refresh becomes claimable after its lease expires. `SKIP LOCKED` prevents workers from processing the same committed claim concurrently. Deployment or readiness failure stops before load. Any missing acknowledged hold, overlap, dead letter, pending queue, unaccounted request, final unexpected fraction at or above 0.01%, or failed rollback blocks the longer recovery diagnostic and restores the original topology. Any final unexpected response continues to fail the strict verdict.

## Validation evidence

The two-consumer/two-maintenance baseline is retained privately at `tmp/capacity/cloudssd-consumer2-1000-safety-20260926`. It delivered all 300,000 scheduled requests with zero generator drops and transport errors. Six first-attempt admission rejections were recovered by retry. Read and hold p95 were 15.068 ms and 82.595 ms. Durability and overlap checks passed, Kafka ended at zero lag, but the audit found 302 pending refresh requests; therefore the stage failed.

The three-maintenance-worker five-minute stage at revision `75595a8` delivered all 300,000 scheduled requests with no generator drops, late deliveries or transport errors. Nineteen first-attempt `ADMISSION_FULL` responses produced 15 successful retries and four exhausted holds, a final unexpected fraction of 0.00133%. Read and hold p95 were 21.815 ms and 99.269 ms. All 14,996 acknowledged holds were durable and no held-seat intervals overlapped. Refresh generation and completion both reached 30,339 at 59.254 per second; pending refresh peaked at 666 and ended at zero, oldest refresh peaked at 30.7 seconds, overdue holds ended at zero, unpublished outbox and dead letters were zero, and rollback succeeded. Kafka maximum total lag was 41 and ended at zero.

The RDS observer sampled up to 16 simultaneous interesting waiters, dominated by `WALWrite` and `WalSync`. API logs recorded commit spikes up to 326.3 ms around the admission burst. The strict stage verdict remains failed because four final responses were unexpected. The result passes ADR 0051's sub-0.01% recovery-SLO request budget and all correctness, queue and cleanup gates, so it authorizes a separately labeled 15-minute diagnostic. A 30-minute diagnostic is still required before any sustained recovery-SLO claim; strict capacity still requires a no-retry, zero-unexpected-error run.
