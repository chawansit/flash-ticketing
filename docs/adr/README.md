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

- [0033: Re-evaluate hold admission after owner-index optimization](0033-hold-admission-after-owner-index.md)

- [0034: Fixed-budget horizontal API scaling experiment](0034-fixed-budget-horizontal-api.md)

- [0035: Separate PostgreSQL onto managed RDS](0035-managed-postgresql-separation.md)
- [0036: Bounded PgBouncer backend-pool headroom](0036-pgbouncer-backend-pool-headroom.md)
- [0037: Fixed DB-pool admission headroom for four API replicas](0037-fixed-db-pool-admission-headroom.md)
- [0038: Nginx upstream keep-alive margin](0038-nginx-upstream-keepalive-margin.md)

- [0039: Admission headroom for repeatable four-API bursts](0039-admission-headroom-repeatability.md)

- [0040: Unattended distributed capacity-stage orchestration](0040-unattended-distributed-capacity-stages.md)

- [0041: Recover evidence after a timed-out load SSH call](0041-timeout-evidence-recovery.md)
- [0042: Classify database-unavailable responses during capacity stages](0042-classify-database-unavailability.md)
- [0043: Detach and poll long generator jobs](0043-detached-generator-job.md)
- [0044: Restore the intended API keep-alive margin](0044-restore-server-keepalive-margin.md)
- [0045: Bounded API pool wait through intermittent RDS commit stalls](0045-bounded-api-pool-wait.md)
- [0046: Build the measured API image from the matched source revision](0046-build-measured-api-image.md)
- [0047: Observe RDS waits for the full measured stage](0047-full-window-rds-wait-observation.md)
- [0048: Separate service-SLO diagnostics from capacity certification](0048-separate-service-slo-diagnostics-from-capacity-certification.md)
- [0049: Bounded idempotent retry and late-delivery diagnostic](0049-bounded-idempotent-retry-diagnostic.md)
- [0050: Scale the maintenance worker for expiry-drain capacity](0050-scale-maintenance-expiry-drain.md)
- [0051: Assess 1,000 RPS recovery with a bounded service-error budget](0051-1000-rps-recovery-slo-diagnostic.md)
- [0052: Scale the SeatsChanged Kafka consumer for refresh ingress](0052-scale-seatschanged-kafka-consumer.md)
- [0053: Scale refresh maintenance to three workers](0053-scale-refresh-maintenance-to-three-workers.md)
- [0054: Batch refresh leases and acknowledgements](0054-batch-refresh-persistence.md)
- [0055: Build and verify measured worker images](0055-build-and-verify-worker-images.md)
- [0056: Promote verified batching under the recovery-SLO diagnostic](0056-promote-verified-batching-diagnostic.md)
- [0057: Isolate refresh and expiry maintenance lanes](0057-isolate-refresh-expiry-maintenance.md)
- [0058: Redis-first durable reservation intake](0058-redis-first-durable-reservation-intake.md)
- [0059: Scale Redis reservation persistence writers](0059-scale-redis-reservation-writers.md)

- [0060: Bound Redis reservation-writer failover recovery](0060-bounded-redis-writer-failover-recovery.md)

- [0061: Resolve ambiguous Redis intake by bounded same-key replay](0061-bounded-same-key-redis-replay.md)

- [0062: Treat managed-failover latency as non-gating evidence](0062-non-gating-failover-latency.md)
- [0063: Bound reservation persistence batches and reject stale intake](0063-bounded-reservation-persistence-batching.md)
- [0064: Rotate bounded reservation-stream discovery fairly](0064-fair-bounded-reservation-stream-discovery.md)
- [0065: Filter empty reservation streams during bounded discovery](0065-filter-empty-reservation-streams-during-discovery.md)
- [0066: Compact empty reservation-stream registry members](0066-compact-empty-reservation-stream-registry-members.md)
- [0067: Throttle empty reservation discovery and repair scans](0067-throttle-empty-reservation-discovery-repair.md)
- [0068: Scale Redis reservation persistence to three writers](0068-scale-reservation-persistence-to-three-writers.md)
- [0069: Pair three reservation writers with split background lanes](0069-three-writers-with-split-background-lanes.md)
- [0070: Batch event consumption and coalesce seat-map refresh work](0070-batch-event-consumption-and-coalesce-seat-map-refresh.md)
- [0071: Serve bounded versioned seat-map deltas after an initial snapshot](0071-bounded-versioned-seat-map-deltas.md)
- [0072: Align the Nginx file-descriptor limit with its bounded connection budget](0072-align-nginx-file-descriptor-budget.md)
- [0073: Encode seat-map delta responses in the synchronous read worker](0073-encode-seat-deltas-in-sync-worker.md)
- [0074: Extend active seat-map retention beyond transient refresh gaps](0074-extend-active-seatmap-retention.md)
- [0075: Bind seat-map delta cursors to cache incarnation](0075-bind-seat-delta-cursors-to-cache-incarnation.md)
- [0076: Track the actual generator session process](0076-track-actual-generator-session-process.md)
- [0077: Atomically publish the private cloud load manifest](0077-atomically-publish-private-load-manifest.md)
- [0078: Keep the reconciler on the same seat-map writer version as the API](0078-coherent-reconciler-image.md)
- [0079: Omit zero-length bootstrap seat-map delta entries](0079-omit-zero-length-bootstrap-deltas.md)
- [0080: Batch read-only durability audit by load run](0080-batch-read-only-durability-audit.md)
- [0081: Bound development payment-simulator concurrency during paid-ticket validation](0081-bound-development-payment-simulator-concurrency.md)
- [0082: Test two Kafka consumers for paid-ticket fulfillment](0082-two-consumer-paid-fulfillment-diagnostic.md)
- [0083: Decouple bounded API DB-pool waiters from connection count](0083-bounded-api-db-pool-waiters.md)
- [0084: Bound OrderPaid fulfillment batches](0084-bound-orderpaid-consumer-batches.md)
- [0085: Test four paid-event Kafka consumers](0085-test-four-paid-event-consumers.md)
- [0086: Isolate synthetic generator HTTP connection-pool headroom](0086-isolate-generator-http-pool-headroom.md)
- [0087: Shard the synthetic paid-journey generator](0087-shard-paid-load-generator-for-capacity-validation.md)
- [0088: Bound synthetic checkout-status polling](0088-bound-synthetic-checkout-status-polling.md)
- [0089: Use a single-statement PostgreSQL order-status read](0089-single-statement-order-status-read.md)
- [0090: Isolate the paid generator with a controlled loopback responder](0090-isolate-paid-generator-with-loopback-responder.md)
- [0091: Profile the paid generator without changing pool behavior](0091-profile-paid-generator-without-changing-pool-behavior.md)
- [0092: Partition generator HTTP pools with a fixed total budget](0092-partition-generator-http-pools-with-fixed-total-budget.md)
- [0093: Re-evaluate bounded polling with the validated generator](0093-reevaluate-bounded-polling-with-validated-generator.md)
- [0094: Re-evaluate single-snapshot order reads with bounded timeouts](0094-reevaluate-single-snapshot-order-reads-with-bounded-timeouts.md)

- [0095 - Generator lifecycle and active-journey pressure](0095-measure-generator-lifecycle-and-active-journey-pressure.md)

- [0096 - Sixteen generator pools with a fixed budget](0096-compare-sixteen-generator-client-pools-with-fixed-budget.md)

- [0097 - Bounded order-status read cache](0097-proposed-bounded-order-status-read-cache.md)

- [0098 - Index unfinished payment deliveries](0098-index-unfinished-payment-deliveries.md)

- [0099 - Booking order lookup index](0099-proposed-booking-order-lookup-index.md)

- [0100 - Bounded paid-rate validation after lookup indexes](0100-bounded-paid-rate-validation-after-indexes.md)

- [0101 - Consumer phase and paid-generator diagnostics](0101-consumer-phase-paid-generator-diagnostics.md)

- [0102 - Six consumers with a controlled connection budget](0102-six-consumers-fixed-connection-budget.md)

- [0103 - Four API pool connections with a fixed server budget](0103-api-pool-four-fixed-server-budget.md)

- [0104 - Five-minute validation of the paid profile](0104-five-minute-paid-profile-validation.md)

- [0105 - Require live paid observers before dispatch](0105-paid-observer-startup-contract.md)

- [0106 - Recheck event streams after advisory registry pruning](0106-recheck-streams-after-registry-pruning.md)

- [0107 - Recheck pending order state when claiming expiry](0107-recheck-pending-order-on-expiry-claim.md)

- [0108 - Include payment simulator in deployment source consistency](0108-simulator-source-consistency-gate.md)

- [0109 - Compare two paid generator processes at fixed total budgets](0109-fixed-budget-two-process-paid-control.md)

- [0110 - Measure development payment dispatch phases](0110-payment-dispatch-phase-diagnostics.md)

- [0111 - Continuously refill bounded development callback slots](0111-continuously-refill-simulator-slots.md)

- [0112 - Twelve development delivery slots within the same DB budget](0112-twelve-delivery-slots-fixed-db-budget.md)
