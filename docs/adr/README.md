# Engineering decision records

Create or update an ADR whenever selecting or changing an architectural pattern. Record context, decision, alternatives, consequences, failure handling and validation. Accepted decisions are superseded by a linked new ADR rather than silently rewritten. These initial records document existing implementation, not new scope approval.

- [PostgreSQL authority and Redis admission](0001-postgresql-authority.md)
- [Transactional outbox](0002-transactional-outbox.md)
- [Kafka at-least-once delivery](0003-kafka-delivery.md)
- [Payment and callback idempotency](0004-payment-idempotency.md)
- [Seat hold expiry](0005-seat-hold-ttl.md)
- [Horizontal scaling with shared database authority](0006-horizontal-scaling.md)

- [Offered-load capacity validation](0007-capacity-validation.md)
- [Bounded background work and durable cache refresh](0008-bounded-background-processing.md)

- [Measured database work and incremental seat projections](0009-incremental-seat-projection.md)

- [Separate layout and conditional availability](0010-conditional-seatmap-reads.md)

- [Bounded reconciliation scheduler](0011-bounded-reconciliation-scheduler.md)

- [Separate fixture preparation and HTTP load generation](0012-separated-load-generation.md)
