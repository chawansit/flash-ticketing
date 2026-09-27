# 0062: Treat managed-failover latency as non-gating evidence

## Status

Accepted on 27 September 2026. This decision supersedes only the requirement in
[ADR 0061](0061-bounded-same-key-redis-replay.md) to define and pass a
failover-recovery latency SLO before Redis-first activation. Its correctness,
idempotency and bounded-replay decisions remain accepted.

## Context

Normal operation and dependency failover have different purposes in capacity
validation. Normal stages determine customer-facing latency and sustainable
throughput. A managed DCS switchover intentionally removes the write dependency
for a short interval and tests whether the system recovers without corrupting
ownership or losing acknowledged work.

The second same-key replay drill recovered every logical request and persisted
all 45 holds without overlap or queue backlog. DCS connectivity recovered in
approximately 500 ms, but two logical holds took 6.238 and 7.192 seconds because
their measured duration included conservative bounded backoff. Mixing those
fault-recovery samples into a small hold population made the aggregate hold p95
7.192 seconds even though ordinary HTTP 202 hold p95 was 13.472 ms.

## Decision

Declared managed-failover windows have no release-gating latency SLO. Their
latency, outage interval, retry count and recovery duration remain measured and
reported as operational evidence, but they do not fail normal capacity
certification or block promotion by themselves.

Normal-operation stages continue to enforce the existing read, hold, error-rate
and generator-delivery gates. Failover stages continue to enforce:

- zero double-booking and ownership overlap;
- exact PostgreSQL durability for every accepted logical hold;
- immutable same-key replay with no replacement idempotency key;
- no broken hold, order or actor relationships;
- bounded attempts with exhausted and unresolved outcomes reported explicitly;
- complete outbox, refresh, dead-letter and Redis-stream drain;
- successful rollback and readiness verification.

A representative-load failover correctness stage is still required before
Redis-first production activation. Its fault-window latency is recorded but is
not compared with the normal-operation percentile gate.

## Alternatives considered

- **Use one latency SLO for normal and failover traffic.** Rejected because a
  planned dependency outage measures recovery behavior and can dominate a small
  percentile sample without describing steady-state capacity.
- **Define a separate failover-recovery latency SLO.** Rejected for the current
  release at the operator's direction. There is not yet a customer or business
  requirement that justifies a specific recovery percentile.
- **Stop measuring failover latency.** Rejected because outage duration and
  retry delay are still necessary for incident analysis and future tuning.
- **Skip failover testing entirely.** Rejected because latency tolerance does
  not permit lost acknowledgements, duplicate ownership or undrained queues.

## Consequences

Capacity claims use normal-operation samples only. A successful correctness
drill may contain slow recovered requests and must state that no failover
latency commitment exists. Product and SLA documents must not promise a
response-time objective during a declared managed-failover window.

This policy can increase visible wait time during failover. Bounded attempts,
client timeouts and explicit unresolved outcomes remain necessary so clients do
not wait forever or create a new idempotency key.

## Failure and recovery behavior

During DCS unavailability, the API returns the explicit retryable unknown-outcome
contract. The client replays the identical actor, payload and key within the
bounded attempt budget. HTTP 202 pending and HTTP 201 durable replay are both
valid terminal success states. Exhausted attempts remain visible and are not
converted into success.

After connectivity returns, reservation writers persist commands to PostgreSQL,
expiry and refresh workers drain their lanes, and the post-TTL audit proves
durability and ownership integrity. Any durability mismatch, overlap, broken
link, unresolved accepted command or non-draining queue fails the stage
regardless of latency policy.

## Validation evidence

The [27 September managed-DCS same-key replay drill](../capacity/huawei-rds/2026-09-27-dcs-same-key-replay/README.md)
completed 900 logical requests with zero final unexpected errors or generator
drops. Both ambiguous holds and all three transient read failures recovered.
The audit verified 45 of 45 holds and reservation commands as durable, zero
overlap and zero queue backlog. The observed 6.238 and 7.192 second replay
durations are retained as evidence but are non-gating under this decision.
