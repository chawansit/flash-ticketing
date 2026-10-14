# ADR0325: Transfer ticketing patterns to the customer stack

## Status

Proposed customer integration contract. Authorized scope is preparing a language-neutral developer handoff, not implementing a customer port, changing cloud resources or selecting their final broker. No existing accepted decision is superseded.

## Context

The customer has APIs on CCE, NATS, PGpool on ECS and an RDS cluster described as having five replicas. Their language, schema, NATS durability configuration, PGpool routing and real payment-gateway contract have not been inspected. The user requested transfer of five patterns: atomic holds and expiry, payment idempotency and recovery, payment plus outbox atomicity, asynchronous issuance with duplicate protection, and cached reads with controlled connections.

Our Python implementation supplies examples and executed evidence. ADR0324 qualifies one five-minute point at 100.90 conservative paid-and-issued tickets/s; it does not qualify a customer port, NATS adapter, real gateway or this configuration for an hour.

## Decision

Prepare contracts, language-neutral pseudocode, source references and an acceptance checklist. Preserve the customer's API language and durable database authority. Retain their PGpool initially, subject to verified primary/read routing and a connection budget calculated for PGpool rather than copied from PgBouncer.

Describe PostgreSQL-first booking as the initial pilot boundary and Redis-first provisional intake as a separate optional phase. Neither permits independent old and new booking writers for the same inventory. Redis supports the reference atomic reservation scripts; NATS KV is not treated as a compatible replacement.

Expose a durable event transport interface. Kafka is the implemented reference. Reusing NATS JetStream is a proposed adapter subject to persistence, replay, concurrency, retention and failure verification; Core NATS alone does not satisfy the durable financial-event contract. Do not introduce Kafka solely to reproduce the reference topology.

Preserve original seat expiry, authenticated ownership, request identities, financial uniqueness, outbox/inbox transactions and bounded retry. External gateway calls, broker acknowledgements and cache publication must occur outside database transactions. Retain batch one as the portable initial default; ADR0324 batch four remains a separately qualified experiment.

## Alternatives

- Rewrite the entire application: unnecessary before the customer's language and bottleneck are known; requires full correctness and performance qualification.
- Deploy the Python service behind the existing API: possible alternative when the customer prefers a service boundary; still needs schema, authentication and inventory ownership contracts.
- Require Redis and Kafka immediately: closest to tested code, but may duplicate the customer's durable messaging platform.
- Use NATS KV for all reservation logic: requires a distinct locking and multi-seat atomicity design, not a translation of the reference Redis scripts.

## Consequences

The customer can implement the invariants in its own framework. Adapters, migrations and gateway inquiry/refund behavior remain customer implementation work. Benchmark capacity cannot be inherited from a different language, schema, broker, pooling model or workload. Read replicas can offload suitable reads; they do not add independent booking write authorities.

## Failure and recovery behavior

Ambiguous payment outcomes are resolved by authenticated primary-backed operation inquiry before identical-key retry. Unavailable inquiry means processing, not a new payment identity. Late successful payment cannot reclaim a seat allocated after expiry; it creates a durable refund or review obligation. Callback acknowledgement follows committed financial effects, or a separately designed durable receipt. Unpublished outbox rows and unacknowledged events remain recoverable. Inbox and ticket uniqueness commit together. Cache failures do not undo financial commits. Rollback drains or transfers outstanding commands, callbacks and events before inventory ownership changes.

## Validation evidence

Source references and benchmark assertions are grounded in repository commit bc71b713e5dbaf3d4f9129766064910692fb783e and the ADR0324 image receipt. The handoff distinguishes proposed customer contracts from implemented reference behavior. Customer acceptance scenarios start as NOT RUN. Executed documentation checks verified 35 local links, referenced source symbols, all five contract sections, the event JSON example, 35 unique acceptance scenario IDs and benchmark numbers. All customer scenarios remain NOT RUN. Whitespace and public-content checks are completed before publication; these checks do not execute a customer implementation.

[Customer transfer pack](../customer-integration/pattern-transfer/README.md). [Reference comparison](../capacity/cce/fulfillment-transaction-batching-comparison-2026-10-14.json).
