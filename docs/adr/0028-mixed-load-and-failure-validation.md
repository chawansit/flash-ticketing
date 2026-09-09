# ADR 0028: Mixed workload and bounded failure validation

Status: Accepted; measurement and recovery execution completed.

## Context

Uniform 95% read / 5% distinct-seat load does not exercise hot-seat conflicts. Contention bursts test one seat but include client queueing and cannot establish sustained mixed-load capacity. The authorized next steps require mixed load, increasing RPS, a soak and recovery drills.

## Decision

Add an opt-in mixed mode preserving 95% availability reads, 4% distinct-seat holds and 1% attempts on one shared hot seat. Keep the existing default workload unchanged. Partition ordinary shows/viewers across workers; explicitly share only the hot event/seat. Treat only 201 and known SEAT_BUSY/SEAT_UNAVAILABLE 409 outcomes as expected hot outcomes; unexpected HTTP/transport errors and drops remain failures. Record hot failed-response p95 separately with a 200ms gate, ordinary hold p95 <=300ms and read p95 <=150ms. Include successful hot holds in durable acknowledgement verification. Never retry measured requests. After expiry, compare acknowledged holds/orders and check that selected load runs have no overlapping order-created-to-hold-expiry intervals for the same event/seat; this interval audit assumes the harness never explicitly releases holds early.

Run bounded stages 400/500/600/800 RPS, stopping upward escalation at the first failed gate. If 400 fails, validate lower fallback rates 200 then 100. Use distinct seat allocations and a fresh development manifest for a 30-minute soak at a validated passing rate; stop if no safe passing rate exists. If a soak fails, preserve its complete client accounting and run one lower previously passed rate for a new full 30-minute soak with fresh credentials and expired/disjoint seats. Do not rerun the same failed rate until it happens to pass. Do not extrapolate a maximum production ceiling.

## Alternatives

Counting all 409 as success would hide invalid requests and unrelated failures. Mixing hot and successful hold latency would hide the failed-response target. Resetting data or rerunning failed stages until they pass would bias results.

## Consequences

Hot success count may exceed one over a long run as the 120s hold TTL expires; PostgreSQL must still have at most one current owner. Different reads can return 200/304 as projection versions change. One shared seat is a deliberately concentrated scenario, not a model of every venue. Synthetic fixture capacity is checked before measurement.

## Failure and recovery

Preflight the exact fixture IDs against PostgreSQL: all sale windows must remain open through the planned run and drain, and credentials must outlast the run. The first uniform control started just after all 800 synthetic sale windows expired; retain its SALE_CLOSED outcomes as an invalid capacity sample. Extend only these fixture windows, record before/after state, and run a separately named control. Reject ordinary seat allocation overlapping the dedicated hot seat.

Retain all failed stages and source hashes. Run isolated Redis, PostgreSQL and Kafka service interruptions only after load and expiry drain. Require three currently unheld/unbooked fixture seats before stopping any service; an explicit seat offset allows later drills to avoid an already booked test seat. Restore each stopped service in finally paths; never overlap faults. During outages require no double booking, controlled failure or authoritative DB behavior, idempotent payment and one eventual ticket after duplicate callbacks. Keep DB/Redis volumes and ECSs. Clean credentials, stop test services and remove temporary network access after validation. No ownership, delivery or idempotency ADR is superseded.

## Validation evidence

400/500 RPS stages passed five minutes; 600 failed with 95 admission rejections and 800 was not run. The 500 RPS 30-minute soak failed with 13 admission rejections and one read reset; the separate 400 RPS 30-minute fallback failed with three read resets and no admission rejection. Both had zero generator drops and passed latency gates. No clean sustained capacity is established by these mixed soaks.

The final serial read-only audit covered all 94,748 acknowledged holds across 60 worker results with zero overlapping seat intervals, broken links, active/overdue holds or pending orders. Queues were zero. Redis/PostgreSQL interruption and idempotent recovery passed. With Kafka stopped, one payment stayed PAID with one unpublished outbox event; after restart it became FULFILLED with one ticket despite five deliveries of the same callback. Final expiry/booking/queue checks passed. Full evidence is in docs/capacity/overnight. Retain the failed diagnostic shared-memory query alongside its bounded serial recovery (ADR 0027).
