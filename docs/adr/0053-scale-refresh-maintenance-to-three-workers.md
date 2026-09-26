# ADR 0053: Scale refresh maintenance to three workers

Status: Accepted for a controlled three-worker capacity experiment; one worker remains the rollback configuration.

This decision supersedes ADR 0050 only for the experimental maintenance replica limit. ADR 0050's database claim, lease, rollback and validation rules remain in force.

## Context

The five-minute 1,000 RPS experiment at revision `d9d3cba` used two Kafka consumers and two maintenance workers. Two consumers removed the event-ingress bottleneck: maximum Kafka group lag fell from 4,000 messages with one consumer to 14, maximum partition lag fell from 1,171 to 7, and the group ended at zero lag.

The refresh stage remained slower than its input. The observer measured 30,000 generated refreshes at 58.524 per second and 29,310 completions at 57.178 per second. Pending refresh peaked at 782 and the fixed audit found 302 pending requests. The oldest refresh reached 184.4 seconds. PostgreSQL durability passed for all 15,000 acknowledged holds, overlapping held-seat intervals were zero, unpublished outbox and dead letters were zero, and overdue active holds returned to zero. This isolates the remaining queue failure to maintenance refresh throughput rather than Kafka delivery.

## Decision

Run a controlled experiment with three `maintenance` replicas, two Kafka consumers, four API replicas and admission five per API. Keep the six Kafka partitions, PgBouncer budget, workload, hold TTL, bounded retry settings and fixed post-TTL audit unchanged. Restore one maintenance replica, one consumer and admission four after every outcome.

The existing PostgreSQL work-claim design remains unchanged. Refresh workers claim leased requests with `FOR UPDATE SKIP LOCKED`; expiry workers claim orders with `FOR UPDATE SKIP LOCKED`. Start with a five-minute 1,000 RPS safety run. Do not promote to a 15-minute run unless request accounting, final error, latency, durability, zero overlap, zero queues and rollback gates all pass.

## Alternatives considered

- Keep two maintenance workers and extend the drain allowance: rejected because it would hide a measured throughput deficit.
- Add more Kafka consumers: rejected because two consumers already reduced maximum total lag to 14 and ended at zero.
- Rewrite refresh processing as a bulk transaction now: deferred because it changes transaction and recovery scope before testing available safe parallelism.
- Split expiry and refresh into separate worker services: deferred until the three-worker trace shows whether shared scheduling still causes starvation.
- Increase admission or API replicas: rejected because it would create more accepted holds without addressing post-processing capacity.

## Consequences

A third maintenance process consumes another database connection and CPU share. PostgreSQL may become the next limit, so the experiment must retain pool-wait, lock-wait and transaction telemetry. The added process should provide margin above the measured 58.524 refreshes per second, but no capacity claim follows until the fixed queue gate passes.

## Failure and recovery behavior

If the third worker fails, its PostgreSQL transaction rolls back; an uncommitted expiry can be claimed by another worker and an uncompleted refresh becomes claimable after its lease expires. `SKIP LOCKED` prevents workers from processing the same committed claim concurrently. Deployment or readiness failure stops before load. Any missing acknowledged hold, overlap, dead letter, pending queue, exhausted retry, unaccounted request or failed rollback blocks promotion and restores the original topology.

## Validation evidence

The two-consumer/two-maintenance baseline is retained privately at `tmp/capacity/cloudssd-consumer2-1000-safety-20260926`. It delivered all 300,000 scheduled requests with zero generator drops and transport errors. Six first-attempt admission rejections were recovered by retry. Read and hold p95 were 15.068 ms and 82.595 ms. Durability and overlap checks passed, Kafka ended at zero lag, but the audit found 302 pending refresh requests; therefore the stage failed.

After implementation, record the three-worker five-minute result here, including refresh generation/completion rates, peak and final pending refresh, Kafka lag, admission outcomes, durability, overlap and rollback. A passing five-minute result permits a 15-minute confirmation; a 30-minute stage remains required for a sustained production-capacity claim.
