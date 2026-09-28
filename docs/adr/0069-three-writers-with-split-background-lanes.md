# ADR 0069: Pair three reservation writers with split background lanes

Date: 2026-09-28
Status: Rejected for sustained 60-write/s production capacity

## Context

ADR 0068 proved that three reservation writers can persist a 1,000 RPS, 6% write workload for three minutes. A 15-minute repeat at revision 560ec2b then sent all 900,000 requests with zero drops, HTTP failures, retries or admission rejections. Read and hold p95 were 43.211 ms and 77.823 ms.

Reservation persistence remained healthy for the full run: all 54,000 provisional commands became durable, no writer command expired, average command age was 1.412 seconds, PostgreSQL commit averaged 3.405 ms and ownership overlap was zero.

The strict stage failed in downstream background processing. One Kafka consumer left total lag at 34,011. The combined maintenance worker left 670 pending refresh rows at the fixed audit and several thousand expired holds and pending orders had not yet been cleaned. Event ingestion averaged 57.420 per second while outbox insertion averaged 83.424 per second across the complete observation window. The reservation-writer scale is no longer the measured bottleneck.

ADR 0052 already defines two idempotent Kafka consumers. ADR 0057 already defines isolated refresh and expiry worker roles. ADR 0059 previously validated that background topology with Redis-first intake at a lower sustained write mix.

## Decision

Run one matched 15-minute experiment with three reservation writers, two Kafka consumers, one dedicated refresh worker and one dedicated expiry worker. Disable the combined maintenance worker during the candidate. Keep four APIs, admission five per API, reservation batch limit four, 120-second hold TTL, DCS acknowledgement, no client retry and the aggregate 1,000 RPS with 6% writes unchanged.

This decision composes accepted ADR 0052 and ADR 0057 background lanes with the ADR 0068 three-writer reservation path. It supersedes ADR 0068 only where that experiment retained one Kafka consumer and one combined maintenance worker.

Do not add more refresh, expiry, consumer or database concurrency if this matched stage fails. Diagnose the resulting lane and WAL evidence before another topology change.

## Alternatives considered

- Add a fourth reservation writer. Reservation durability and command age already pass; it would not drain Kafka, refresh or expiry work.
- Extend the audit wait. The Kafka input rate exceeded observed consumption for the measured window, so extra drain time would hide a throughput deficit.
- Use three combined maintenance workers. ADR 0053 rejected that topology for sustained 1,000 RPS because WAL contention and admission failures increased.
- Batch or redesign refresh persistence now. Existing split roles and two-consumer idempotency are already implemented and provide the smaller controlled experiment.
- Accept nonzero read-model queues. Seat availability freshness and expiry cleanup are part of the capacity gate.

## Consequences

Two consumers can use more Kafka partitions concurrently. Dedicated refresh and expiry workers cannot starve each other in one loop. The topology adds background database concurrency and may expose WAL contention; the RDS wait observer and API latency gates remain mandatory.

Passing establishes only a 15-minute planning point for the measured 94% read and 6% write mix. It does not prove a 30-minute production capacity level.

## Failure and recovery behavior

Kafka consumer replay remains idempotent through consumer-inbox uniqueness. Refresh work uses generation fencing and leased claims. Expiry work uses PostgreSQL locking and rolls back atomically on failure. A stopped worker leaves durable work for another replica or a later restart.

The unattended runner restores one combined maintenance worker, one consumer, PostgreSQL reservation mode and zero reservation writers on every exit. Any request error, missing durable command, overlap, nonzero queue, observer failure or rollback failure rejects the candidate.

## Validation evidence

The sustained baseline at revision 560ec2b produced:

- 900,000 of 900,000 requests, zero drops, errors, retries and admission rejections.
- Read and hold p95 of 43.211 ms and 77.823 ms.
- Exact durability for 54,000 reservation commands, zero writer failures and zero overlap.
- Average command age 1.412 seconds and PostgreSQL commit average 3.405 ms.
- Final Kafka lag 34,011 and pending refresh 670; expiry cleanup was incomplete.
- Successful rollback.

The split-lane sustained stage failed the request gate:

- 899,933 of 900,000 requests were sent; generator drops were 67. There were no
  transport, HTTP or admission errors and no retry was used.
- Worst-worker read and hold p95 rose to 161.799 ms and 343.946 ms.
- All 53,998 delivered provisional commands became durable. Command age averaged
  1.622 seconds, writer failures and ownership overlap were zero.
- Two consumers processed 107,996 events and ended at zero Kafka lag. Refresh
  generation and completion were both 107,996; pending refresh, overdue holds,
  outbox and dead letters all ended at zero.
- RDS sampling observed at most nine interesting waiters, including up to eight
  concurrent WALWrite waits. PostgreSQL reservation batch and commit averages
  were 116.169 ms and 3.226 ms.
- Rollback and cleanup passed.

The split topology fixes the downstream queue deficit but consumes enough shared
database and host capacity to miss request fidelity and latency targets. It is
rejected as the sustained 60-write/s production topology. No additional worker
concurrency is authorized by this ADR. The confirmed 15-minute planning point
remains 1,000 total RPS with 30 reservation writes per second.

