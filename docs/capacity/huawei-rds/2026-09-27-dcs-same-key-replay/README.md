# Managed DCS same-key replay drill

**Run date:** 27 September 2026
**Window:** 22:21:22-22:24:21 Asia/Bangkok
**Outcome:** recovery correctness passed; fault-stage latency was recorded and is non-gating under ADR 0062.

This low-rate drill tested the bounded same-idempotency-key recovery chosen in
[ADR 0061](../../../adr/0061-bounded-same-key-redis-replay.md) during a real Huawei
DCS switchover. It validates correctness during the fault. It does not establish
high-rate capacity or a production failover latency SLO.

## Candidate

The stage offered 5 RPS for 180 seconds across two generator workers. The backend
used four API replicas, admission limit four, two reservation writers, two Kafka
consumers and separate refresh and expiry workers. Retries were opt-in, limited
to three physical attempts per logical request, and always reused the original
actor, payload and idempotency key.

## Switchover and recovery

The 250 ms DCS probe recorded 1 timeout in 1,920 samples. The interval from the
last successful probe before the timeout to the first success after it was
500.159 ms. Successful probe latency was 0.937 ms average, 1.239 ms p95 and
9.460 ms maximum.

The generator scheduled 900 logical requests and made 907 physical HTTP
attempts. It dropped none. Two holds initially returned
'RESERVATION_DURABILITY_UNKNOWN'; both resolved as HTTP 201 'DURABLE' after two
same-key replays. Three seat-map reads initially returned
'SEATMAP_UNAVAILABLE'; all three recovered. No retry was exhausted and no
logical request ended with an unexpected error.

The final workload contained 45 accepted holds: 43 HTTP 202 provisional
acceptances and two HTTP 201 durable same-key replay results. The remaining 855
logical requests were successful seat-map responses.

## Latency

Normal provisional holds remained fast: the worse worker's HTTP 202 hold p95 was
13.472 ms, and the worse read p95 was 8.208 ms. The two recovery responses took
6.238 and 7.192 seconds because they included bounded backoff across the managed
switchover. That raises the worse worker's aggregate hold p95 to 7.192 seconds,
which exceeded the former strict latency gate. ADR 0062 now treats this declared-failover latency as non-gating evidence.

This fault latency is not a capacity percentile. It shows the current recovery
contract favors a resolved, durable outcome over a sub-second response while
DCS availability is ambiguous.

## Post-TTL audit

The corrected Redis-first audit counted HTTP 201 durable replay results as
commands that must exist in PostgreSQL. It verified:

- 45 acknowledged logical holds, 45 idempotency records, 45 distinct holds and
  45 distinct orders;
- 45 durable reservation commands;
- zero broken links, active holds, overdue holds or pending orders;
- zero overlapping seat ownership intervals;
- zero unpublished outbox events, pending refresh requests, dead letters,
  Redis stream entries or pending stream deliveries.

The audit correction retains the previous behavior for direct PostgreSQL runs,
where HTTP 201 does not imply a Redis reservation command. The focused verifier
suite passed five tests.

## Rollback

Rollback restored PostgreSQL reservation mode, removed both reservation writers,
restored the original maintenance and consumer topology, and passed five
consecutive readiness checks.

## Decision

ADR 0061's same-key recovery behavior is validated at low rate: every ambiguous
hold resolved, PostgreSQL durability was exact, no seat was double-booked and all
queues drained. Redis-first production activation remains blocked pending a
managed-failover correctness stage at representative load. No failover latency
SLO is required. The 7.192-second fault percentile remains operational evidence
and must not be presented as normal-operation production capacity.

## Evidence

- [Load and retry summary](load-summary.json)
- [Post-TTL durability audit](durability.json)
- [DCS probe summary](dcs-probe-summary.json)
- [Candidate and rollback topology](topology.json)
