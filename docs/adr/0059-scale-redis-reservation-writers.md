# ADR 0059: Scale Redis reservation persistence writers

Status: Accepted for a bounded two-replica cloud experiment

## Context

ADR 0058 introduced Redis-first provisional reservation intake and deliberately deferred
PostgreSQL writer scaling. The first distributed no-retry 1,000 RPS stage on 27 September 2026
sent 300,000 requests in five minutes, including 15,000 HTTP 202 provisional holds. Request
fidelity and latency passed: there were no drops, late deliveries, transport errors or retries;
worst-worker read and hold p95 were 19.583 ms and 33.712 ms.

One reservation writer made only 9,604 commands durable before their fixed 120-second hold
lease expired. Redis retained all 15,000 command outcomes: 9,604 were `DURABLE` and 5,396 were
terminal `FAILED` with `HOLD_EXPIRED`. PostgreSQL linkage for durable commands was exact, seat
ownership never overlapped, and every reservation stream entry and pending delivery drained.
The observed durable rate was about 32 commands per second while offered hold traffic was 50
per second. A single sequential writer is therefore the measured bottleneck.

## Decision

Add an explicit capacity-stage reservation-writer candidate count and run the next isolated
Redis-first stage with two writer replicas. Keep the API topology, 120-second hold TTL, DCS
replica acknowledgement, PgBouncer budget, PostgreSQL schema, no-retry workload and all strict
durability gates unchanged. Start the candidate writers before switching APIs to Redis-first
mode. Verify their replica count and source hash. Capture and restore the original reservation
mode and writer count on every exit.

Bound the experimental candidate to one through four replicas; two is the next measured step.
Do not promote two writers unless every acknowledged HTTP 202 becomes one PostgreSQL `DURABLE`
command with complete hold/order/idempotency linkage, zero ownership overlap, zero remaining
stream entries or pending deliveries, and successful rollback.

## Alternatives

- Increase the hold TTL: rejected because it changes the customer reservation contract and
  hides inadequate steady-state persistence throughput.
- Treat terminal `HOLD_EXPIRED` after HTTP 202 as an acceptable service error: rejected for the
  capacity gate because provisional acknowledgement promises a result that clients may poll;
  the strict test requires durability for every acknowledged command.
- Batch several commands into one PostgreSQL transaction: deferred. It may reduce commit cost
  but increases transaction scope and failure coupling and needs a separate persistence ADR.
- Add more PostgreSQL or PgBouncer connections without another writer: rejected because the
  measured writer processes commands sequentially and cannot use that added concurrency.
- Start with four writers: rejected because two should exceed the measured 50-command-per-second
  arrival rate if throughput scales near linearly, and a smaller step limits new WAL pressure.

## Consequences

Two writers can consume distinct stream entries concurrently and should raise durable command
throughput. PostgreSQL remains the final authority, and command/idempotency uniqueness makes
at-least-once replay safe. The second process adds one active transaction at a time and can
increase WAL synchronization, row-lock contention and refresh work. HTTP latency alone cannot
qualify the topology; command age and final durability remain mandatory gates.

The unattended runner gains an explicit candidate setting, records the deployed writer count,
and restores the pre-stage count. PostgreSQL mode always deploys zero reservation writers.

## Failure and recovery behavior

If either candidate writer fails, its pending delivery remains in the consumer group and another
writer reclaims it with `XAUTOCLAIM`. A database conflict still produces token-fenced Redis
compensation. Any missing durable command, terminal failure, overlap, undrained stream, worker
startup failure or observer failure fails the stage. Rollback first restores the captured
reservation mode and writer count, then recreates the API replicas and other workers.

## Validation evidence

The one-writer baseline is the 27 September 2026 five-minute 1,000 RPS stage at revision
`5be26f3`. It proved exact intake accounting (15,000 HTTP 202), 9,604 durable commands, 5,396
`HOLD_EXPIRED` terminal commands, zero overlaps, zero admission rejections and drained Redis
streams. The RDS wait observer failed independently because it removed the configured TLS CA;
the load and durability evidence remains valid, but observer collection must be fixed before
the two-writer result can pass the complete execution gate.

Implementation tests and the two-writer cloud result are pending at decision acceptance time.
No production activation or capacity claim follows until those results are appended.

## Implementation and cloud validation on 2026-09-27

The unattended stage now accepts one through four candidate reservation writers, starts and
source-verifies the candidate before the API cutover, records the deployed count and restores
the captured mode and replica count. The direct RDS wait observer retains its mounted TLS CA.
The focused unattended-stage suite passed 13 tests, the full unit suite passed 142 tests with
two dependency deprecation warnings, Ruff passed and both Huawei shell helpers passed Alpine
`sh -n`.

The first two-writer run made all 14,674 delivered provisional commands durable but exposed a
separate four-process generator limit: 6,208 schedules were dropped. Eight generator processes
then delivered all 300,000 requests and two writers made all 15,000 commands durable. The
combined maintenance topology left 80 refresh rows at the fixed audit, so that stage correctly
remained failed even though a later audit found zero.

The final matched five-minute stage used eight generators, two reservation writers, two Kafka
consumers and the ADR 0057 split refresh/expiry workers. It delivered all 300,000 no-retry
requests with zero drops, late deliveries, transport errors or HTTP failures. Worst-worker
read/hold p95 were 103.682/235.254 ms. All 15,000 provisional acknowledgements reached one
PostgreSQL `DURABLE` command with exact hold/order/idempotency linkage, zero overlapping seat
intervals and zero queues or reservation stream work at the fixed 180-second audit. Rollback
and cleanup passed.

This validates two writers for the isolated safety topology. It does not supersede the remaining
DCS primary-failover and pending-command writer-restart gates, and five minutes is insufficient
for a sustained production capacity claim. Compact evidence is published in the
[Redis-first 1,000 RPS report](../capacity/huawei-rds/2026-09-27-redis-first-1000-rps/README.md).
