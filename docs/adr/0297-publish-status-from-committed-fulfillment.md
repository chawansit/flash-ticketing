# ADR0297: Publish status from the committed fulfillment transaction

## Status

Accepted for implementation and one controlled candidate under standing authorization. Supersedes ADR0149's separate refresh read only for a newly handled OrderPaid event when the new default-off option is explicitly enabled. Retains ADR0280 replay/freshness safeguards. Adoption and capacity qualification are pending.

## Context

The latest hour dispatched and eventually fulfilled 300,235 journeys, with 2,165 generator drops, 380 initially errored journeys and zero final customer failures. Ticket wait p95 was 9.993 seconds. The pooler reached 24 active server connections, 16 waiting clients and 384.954 ms maximum wait. Diagnostic coverage was incomplete, so full failure-time causality remains unconfirmed. Each new OrderPaid event currently performs fulfillment, then separately acquires a database connection and reads the same order and tickets for advisory status publication. Simultaneous cache-fill races are less than 0.2% of cache misses; coalescing is not the first performance candidate.

## Decision

Add default-off ORDER_STATUS_COMMITTED_SNAPSHOT, requiring the existing bounded cache and committed-event refresh. Only a consumer handling a newly inserted OrderPaid inbox event may capture the complete actor-owned order and actual ticket identities inside its existing financial transaction. Obtain the Redis freshness timestamp before acquiring the database connection. Derive tickets from the existing booking read and ticket insertion results; do not add a new database statement or transaction. Publish only after successful commit and connection release.

Existing ticket IDs must be retained on conflicts; never invent an ID for a ticket not inserted. Replayed events and unavailable/invalid captures retain the existing fresh committed-read path. Best-effort Redis publication failures do not undo or acknowledge uncommitted financial work. Keep the existing one-second candidate freshness bound, publication timestamp fencing, actor-scoped keys and bounded ADR0280 hints. No expiry, ordering, idempotency, payment, refund or connection-budget changes.

## Alternatives

Miss coalescing alone: limited measured overlap. Longer cache TTL: changes freshness and is excluded. Pipeline status reads: ADR0255 already failed to improve the measured workload; leave disabled. More workers/connections before measuring work reduction: defer. Derive status from uncommitted events or optimistic IDs: rejected.

## Consequences

Eligible fulfillments avoid one additional read transaction and pool acquisition, and publish their committed tickets promptly. The booking query includes seat and existing-ticket columns; inserts return actual IDs. This can reduce serial consumer work and database competition, but a 20% throughput gain is not established. The option is disabled by default and rollback is the same image with the option disabled.

## Failure and recovery behavior

Financial, inbox or commit failures propagate without publication or offset acknowledgement. Replay reads current committed state and repairs advisory cache loss. Slow capture cannot extend freshness; Redis publication retains its original deadline. Invalid/incomplete captured rows fall back to an authoritative committed read. Redis failure remains advisory; authorized API reads fall back to PostgreSQL. Preserve all failed evidence, check payment durability and zero double-booking, drain queues and restore runtime.

## Validation evidence

Executed source inspection confirms that the fulfillment transaction already has the order, bookings and ticket insertion results needed for a snapshot. Executed 99 unit regressions and 24 real PostgreSQL/Redis projection regressions against the image-derived candidate source; all passed. These cover successful commit and connection release, failed commit, actual ticket IDs on insertion/conflict, duplicate delivery, advisory cache outage, actor authorization, timestamp fencing and bounded freshness. The integration regression observes one financial transaction instead of two for eligible fulfillment. Another 70 reservation/Redis-first integration cases and 18 parent-compatible payment recovery cases passed on the candidate source. Current HEAD payment recovery tests require an unrelated later payment-pipeline capability absent from this deliberately frozen parent (32 setup errors and two failures); used the retained 9b0abb7 tests instead, without adding that capability or changing financial gates.

Built and published an immutable image derived from the measured ADR0267 image, retaining all 22 modules except the four reviewed configuration, fulfillment, projector and instrumentation changes. Verified both package copies and imported module hashes after pulling by registry digest; dependency inputs and eight role settings passed verification. Repeated source preparation yields the same source identity. The instrumentation wrapper only forwards the new optional argument; its measurements remain unchanged. See [publication receipt](../capacity/cce/committed-fulfillment-status-image-2026-10-13.json).

Controlled cloud comparison and hourly qualification are pending. No performance improvement or 100-ticket/s capacity is claimed.
