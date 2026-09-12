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

- [0013: Single-ECS capacity validation](0013-single-ecs-capacity-validation.md)

- [0014: Private-network cloud capacity benchmark](0014-private-cloud-capacity-benchmark.md)

- [0015: Hold-path diagnostics](0015-hold-path-diagnostics.md)

- [0016: Isolated reconciliation worker](0016-isolated-reconciliation-worker.md)

- [0017: Redis-validated bounded browse bodies](0017-validated-browse-body-cache.md)

- [0018: Concentrated traffic validation](0018-concentrated-traffic-validation.md)

- [0019: Contention latency attribution](0019-contention-latency-attribution.md)

- [0020: Protocol ingress timing and bounded admission experiment](0020-ingress-and-admission-experiment.md)

- [0021: Opt-in worker dispatch diagnostics](0021-thread-dispatch-diagnostics.md)

- [0022: Inline in-memory service dependency](0022-inline-service-dependency.md)

- [0023: Bounded read transport diagnostics](0023-read-transport-diagnostics.md)

- [0024: Read-only idle-boundary experiment](0024-idle-boundary-experiment.md)

- [0025: Client expiry load comparison](0025-client-expiry-load-comparison.md)

- [0026: Direct ASGI instrumentation and admission](0026-direct-asgi-instrumentation.md)

- [0027: Transaction-scoped diagnostic settings](0027-transaction-scoped-diagnostics.md)

- [0028: Mixed load and bounded failure validation](0028-mixed-load-and-failure-validation.md)

- [0029: Long-run stability and slow-commit isolation workflow](0029-long-run-stability-validation.md)

- [0030: Keep-seatmap-hot-path reads alive by TTL touch](0030-read-path-ttl-touch.md)

- [0031: Partial index for active seat ownership](0031-active-seat-owner-index.md)

- [0032: Server keep-alive margin experiment](0032-server-keepalive-margin-experiment.md)
