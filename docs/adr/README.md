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

## Imported CCE experiment decisions

These entries record experiments implemented on the pinned experimental branch; this documentation PR does not integrate their runner, backend or migrations into main. See the [hourly milestone](../capacity/flash-sale-opening/cce-hourly-milestone-2026-10-09.md) for source provenance and qualification limits.

- [0232: Bounded hourly paid ticket qualification](0232-bounded-hourly-paid-ticket-qualification.md)
- [0234: Bounded CCE acquisition headroom with unchanged connections](0234-bounded-cce-acquisition-headroom.md)
- [0235: Hourly terminal audit and independent recovery](0235-hourly-terminal-audit-and-recovery.md)

- [ADR0236: Stage the paid backend schema before runtime promotion](0236-staged-paid-backend-schema-promotion.md)

- [ADR0237: Promote source-pinned backend and worker behavior](0237-source-pinned-backend-and-worker-promotion.md)

- [ADR0238: Promote pinned CCE deployment and the paid-load runner](0238-promote-pinned-cce-and-paid-runner.md)

- [0090: isolate paid generator with loopback responder](0090-isolate-paid-generator-with-loopback-responder.md)
- [0147: fixed budget two host api comparison](0147-fixed-budget-two-host-api-comparison.md)
- [0151: identical cache age committed status refresh comparison](0151-identical-cache-age-committed-status-refresh-comparison.md)
- [0153: owned cloud parent image retrieval](0153-owned-cloud-parent-image-retrieval.md)
- [0161: isolated asynchronous confirmation comparison](0161-isolated-asynchronous-confirmation-comparison.md)
- [0165: exact admission observer compatibility](0165-exact-admission-observer-compatibility.md)
- [0171: bounded slow database phase diagnostics](0171-bounded-slow-database-phase-diagnostics.md)
- [0172: standing work envelope and exception escalation](0172-standing-work-envelope-and-exception-escalation.md)
- [0173: bounded database wait and wal diagnostics](0173-bounded-database-wait-and-wal-diagnostics.md)
- [0174: fixed budget api placement rebalance](0174-fixed-budget-api-placement-rebalance.md)
- [0177: bounded application role rebalance comparison](0177-bounded-application-role-rebalance-comparison.md)
- [0181: bounded diagnostic placement comparison](0181-bounded-diagnostic-placement-comparison.md)
- [0182: verified pre dispatch abort recovery](0182-verified-pre-dispatch-abort-recovery.md)
- [0190: historical recovery exception and test isolation](0190-historical-recovery-exception-and-test-isolation.md)
- [0191: pinned fixture format and zero dispatch abort](0191-pinned-fixture-format-and-zero-dispatch-abort.md)
- [0193: bounded atomic payment claim comparison](0193-bounded-atomic-payment-claim-comparison.md)
- [0216: shared callback routing comparison](0216-shared-callback-routing-comparison.md)
- [0217: shared callback api placement comparison](0217-shared-callback-api-placement-comparison.md)
- [0218: safety only diagnostic abort recovery](0218-safety-only-diagnostic-abort-recovery.md)
- [0219: shared callback 84 ticket rate probe](0219-shared-callback-84-ticket-rate-probe.md)
- [0221: dispatched cohort recovery classification](0221-dispatched-cohort-recovery-classification.md)
- [0222: coalesce interleaved seat refresh intents](0222-coalesce-interleaved-seat-refresh-intents.md)
- [0223: attribute paid observer sampling delay](0223-attribute-paid-observer-sampling-delay.md)
- [0224: index orders by event for paid cohort queries](0224-index-orders-by-event-for-paid-cohort-queries.md)
- [0225: pipeline reservation command writes](0225-pipeline-reservation-command-writes.md)
- [0226: reap completed generator tasks before admission](0226-reap-completed-generator-tasks-before-admission.md)
- [0228: bounded cce api isolation comparison](0228-bounded-cce-api-isolation-comparison.md)
- [0230: remove cce database bridge cpu bottleneck](0230-remove-cce-database-bridge-cpu-bottleneck.md)
- [0231: refresh owned api ports after restart](0231-refresh-owned-api-ports-after-restart.md)
- [0233: retain cce admission failure evidence](0233-retain-cce-admission-failure-evidence.md)

- [0146: freeze and measure observation overhead](0146-freeze-and-measure-observation-overhead.md)

- [0149: committed event order status refresh](0149-committed-event-order-status-refresh.md)

- [0170: single control payment stall diagnostics](0170-single-control-payment-stall-diagnostics.md)

- [ADR0239: Remove redundant transaction BEGIN](0239-remove-redundant-transaction-begin.md)

- [ADR0240: Bounded compressed diagnostic traces](0240-bounded-compressed-diagnostic-traces.md)

- [ADR0241: Bounded failure-time slot ownership](0241-bounded-failure-time-slot-ownership.md)

- [ADR0242: Bind matched CCE transaction comparison](0242-bind-matched-cce-transaction-comparison.md)

- [ADR0243: Use managed CCE SWR pull credentials](0243-use-managed-cce-swr-pull-credentials.md)

- [ADR0244: Reconcile failed matched transaction candidate](0244-reconcile-failed-matched-transaction-candidate.md)

- [ADR0245: Rebalance API payment connections within a fixed budget](0245-rebalance-api-payment-connections-within-fixed-budget.md)

- [ADR0246: Reconcile the failed fixed-budget payment control](0246-reconcile-failed-fixed-budget-payment-control.md)

- [ADR0247: Batch paid-cohort observer lookups](0247-batch-paid-cohort-observer-lookups.md)

- [ADR0248: Batch payment context after locking the order](0248-batch-payment-context-after-order-lock.md)

- [ADR0249: Compare post-lock payment context with fixed budgets](0249-compare-post-lock-payment-context-with-fixed-budgets.md)

- [ADR0250: Reconcile the failed payment-context control](0250-reconcile-failed-payment-context-control.md)

- [ADR0251: Decouple callback dispatch concurrency from the database pool](0251-decouple-callback-dispatch-concurrency-from-db-pool.md)

- [ADR0252: Qualify the accepted simulator correction for one hour](0252-qualify-simulator-correction-for-one-hour.md)

- [ADR0253: Independently reconcile the failed simulator hourly run](0253-independently-reconcile-failed-simulator-hourly-run.md)

- [ADR0254: Recover customer payment and confirmation failures](0254-recover-customer-payment-and-confirmation.md)

- [ADR0255: Pipeline status-read transaction setup](0255-pipeline-status-read-transaction-setup.md) â€” [paired cloud evidence](../capacity/cce/customer-recovery-comparison-2026-10-10.json); recovery verified, pipeline remains off.

- [ADR0256: Qualify customer recovery for one hour](0256-qualify-customer-recovery-for-one-hour.md)

- [ADR0257: Close hourly recovery without qualifying incomplete diagnostics](0257-close-hourly-recovery-and-preserve-diagnostic-failures.md)
