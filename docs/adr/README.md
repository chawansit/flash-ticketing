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

- [ADR0258: One application image, separate service containers](0258-shared-application-image-and-separate-cce-services.md)

- [ADR0259: Compare shared-image worker placement](0259-compare-shared-image-worker-placement.md)

- [ADR0260: Close failed shared-image control without qualifying capacity](0260-close-shared-image-control-without-qualifying-capacity.md)

- [ADR0261: Preserve accepted consumer batching in the shared image](0261-preserve-consumer-batching-in-shared-image.md)

- [ADR0262: Retain startup proof with live runtime verification](0262-retain-startup-proof-with-live-runtime-verification.md)

- [ADR0263: Bound payment dispatch slots independently](0263-bound-payment-dispatch-slots-independently.md)

- [ADR0264: Close failed dispatch correction without qualifying capacity](0264-close-failed-dispatch-correction-without-qualifying-capacity.md)

- [ADR0265: Isolate fulfillment from seat-refresh consumption](0265-isolate-fulfillment-from-seat-refresh-consumption.md)

- [ADR0266: Bounded event-lane capacity correction](0266-bounded-event-lane-capacity-correction.md)

- [ADR0267: Bound multipartition Kafka fetch frames](0267-bound-multipartition-kafka-fetch-frames.md)

- [ADR0268: Validate explicit API flags before legacy projection](0268-validate-explicit-api-flags-before-legacy-projection.md)

- [ADR0269: Reconcile the failed event-lane paid cohort](0269-reconcile-failed-event-lane-paid-cohort.md)

- [ADR0270: Preserve event-lane CPU observation binding](0270-preserve-event-lane-cpu-observation-binding.md)

- [ADR0271: Rebalance event-lane workers to CCE](0271-rebalance-event-lane-workers-to-cce.md)
- [ADR0272: Capture bounded failure logs during paid traffic](0272-capture-bounded-failure-logs-during-paid-traffic.md)

- [ADR0273: Probe Kafka route before worker migration](0273-probe-kafka-route-before-worker-migration.md)

- [ADR0274: Close refunded worker-migration safety scope](0274-close-refunded-worker-migration-safety-scope.md)

- [ADR0275: Preserve recovery in worker-placement comparisons](0275-preserve-recovery-in-worker-placement-comparisons.md)
- [ADR0276: Reconcile the mixed terminal worker-placement cohort](0276-reconcile-mixed-terminal-worker-placement-cohort.md)

- [ADR0277: Five-minute 50,000-ticket capacity test](0277-five-minute-fifty-thousand-ticket-capacity-test.md)

- [ADR0278 Retain paid timeout evidence before restoration](0278-retain-paid-timeout-evidence-before-restoration.md)

- [ADR0279: Native database TCP bridge](0279-native-database-tcp-bridge.md)

- [0280: Reuse fresh committed order status in fulfillment](0280-reuse-fresh-committed-order-status.md)

- [ADR0281: Qualify committed-status reuse for one hour](0281-qualify-status-reuse-for-one-hour.md)

- [ADR0282: Bound hourly terminal relationship audit work](0282-bound-hourly-terminal-relationship-audit.md)

- [ADR0283: Bound hourly observer cost and preserve diagnostics](0283-bound-hourly-observer-cost-and-preserve-diagnostics.md) — Hourly ticket target measured; qualification retains audit timeout and diagnostic coverage failures.

- [ADR0284: Pipeline payment initiation writes](0284-pipeline-payment-initiation-writes.md) — Opt-in implementation compared; mixed results; default off retained.

- [ADR0285: Complete audits and compare payment pipelining](0285-complete-audits-and-compare-payment-pipelining.md) — Complete audits and fresh off/on comparisons executed; both short arms passed; no pipeline adoption.
- [ADR0286: Hourly qualification with complete verification](0286-hourly-qualification-with-complete-verification.md) — Hourly ticket target demonstrated; full qualification failed on log capture; independent integrity/restoration passed.

- [ADR0287: Capture complete slot history from existing API metrics](0287-capture-slot-history-from-existing-metrics.md) — Explicit new-scope diagnostic transport correction; historical hourly failure preserved.

- [ADR0288: Collect statistics before variable cohort work](0288-collect-statistics-before-cohort-work.md) — Preserve the existing connection budget and two-second gate.

- [ADR0289: Prepare paid fixtures before fresh transition admission](0289-prepare-fixtures-before-fresh-admission.md) — Fresh admission passed; subsequent hour failed throughput/customer and slot-history gates, with independent recovery verified.

- [ADR0290: Compare CCE-managed ingress with the ECS proxy](0290-compare-cce-managed-ingress.md) — Proposed private ELB route comparison; no deployment or capacity gain claimed.

- [ADR0291: Compare the existing CCE NGINX ingress](0291-compare-existing-cce-nginx-ingress.md) — Accepted bounded customer-entry comparison; no capacity gain measured.

- [ADR0292: Prepare short fixtures after expiry safety](0292-prepare-short-fixtures-after-safety.md) — Accepted short staging correction; preserves freshness limits and hourly ordering.

- [ADR0293: Qualify the existing CCE ingress for one paid hour](0293-qualify-existing-cce-ingress-hourly.md)

- [ADR0294: Bootstrap hourly observers before native expiry qualification](0294-bootstrap-hourly-observers-before-expiry.md)

- [ADR0295: Bind the hourly generator to the existing CCE ingress](0295-bind-hourly-generator-to-cce-ingress.md)

- [ADR0296: Align hourly observer and supervisor lifetimes](0296-align-hourly-observer-supervisor-lifetime.md)

- [ADR0297: Publish status from the committed fulfillment transaction](0297-publish-status-from-committed-fulfillment.md)

- [ADR0298: Bind the committed-status load comparison](0298-bind-committed-status-load-comparison.md)

- [ADR0299: Expose preloaded query statistics](0299-expose-preloaded-query-statistics.md)

- [ADR0300: Compare cohort observer query cost](0300-compare-cohort-observer-query-cost.md)

- [ADR0301: Attribute unchanged paid-stage database cost](0301-attribute-unchanged-paid-stage-database-cost.md)

- [ADR0302: Compare actual runtime cohort query](0302-compare-actual-runtime-cohort-query.md)

- [ADR0303: Guard batched runtime cohort replacement](0303-guard-batched-runtime-cohort-replacement.md)

- [ADR0304: Bind the runtime cohort comparison](0304-bind-runtime-cohort-comparison.md)

- [ADR0305: Bound final customer failures after recovery](0305-bound-final-customer-failures-after-recovery.md)

- [ADR0306: Rebalance payment slots on the committed-status runtime](0306-rebalance-payment-slots-on-committed-status.md)

- [ADR0307: Renew CCE admission from live identities](0307-renew-cce-admission-from-live-identities.md)

- [ADR0308: Qualify the committed runtime at the approved customer target](0308-qualify-committed-runtime-at-approved-customer-target.md)

- [ADR0309: Admit terminal audits after paid work stops](0309-admit-terminal-audits-after-paid-work-stops.md)

- [ADR0310: Classify gateway read failures within the approved SLO](0310-classify-gateway-read-failures-within-approved-slo.md)

- [ADR0311: Activate bounded gateway-read recovery](0311-activate-bounded-gateway-read-recovery.md)

- [ADR0312: Bound headroom on the qualified recovery runtime](0312-bound-headroom-on-qualified-recovery-runtime.md)

- [ADR0313: Reconcile the failed headroom cohort](0313-reconcile-failed-headroom-cohort.md)

- [ADR0314: Rebalance general slots at headroom](0314-rebalance-general-slots-at-headroom.md)

- [ADR0315: Scale reservation-writer concurrency](0315-scale-reservation-writer-concurrency.md)

- [ADR0316: Test larger reservation batches at fixed four-writer capacity](0316-test-reservation-batch-eight-at-fixed-budget.md)

- [ADR0317: Isolate foreground backend slots at fixed budget](0317-isolate-foreground-backend-slots-at-fixed-budget.md)

- [ADR0318: Rebalance isolated backend slots toward fulfillment](0318-rebalance-isolated-backends-toward-fulfillment.md)

- [ADR0319: Apply post-lock payment context to the measured shared image](0319-apply-post-lock-payment-context-to-measured-image.md) - Controlled102/s candidate rejected; failed experiment independently reconciled and restored.

- [ADR0320: Pipeline reservation transaction boundaries](0320-pipeline-reservation-transaction-boundaries.md) - Local pipeline rejected before cloud load; application changes restored.

- [ADR0321: Read writer sale context after seat locks](0321-read-writer-sale-context-after-seat-locks.md) - Clean102/s customer journeys; conservative100-ticket/s gate missed19 tickets, all services restored.

- [ADR0322: Bound redundant ticket-notification reads](0322-bound-redundant-ticket-notification-reads.md) - Measured short comparison improved ticket latency but failed zero-drop gate; independently reconciled and restored.

- [ADR0323: Remove duplicate driver transaction starts](0323-remove-duplicate-driver-transaction-starts.md) - Default-off experiment: latency improved, but 13 generator drops failed the short comparison; actual 30,587 payments reconciled and runtime restored. Late commit stalls remain.

- [ADR0324: Bound fulfillment transaction batches](0324-bound-fulfillment-transaction-batches.md) - Default-one experiment: five-minute comparison passed at 100.90 paid-issued tickets/s, no drops/final failures; hourly qualification pending and runtime restored.

- [ADR0325: Transfer ticketing patterns to the customer stack](0325-transfer-ticketing-patterns-to-customer-stack.md) - Proposed language-neutral customer contracts and acceptance checklist; no customer port or deployment implemented.

- [ADR0326: Compare the upgraded RDS at unchanged ticketing load](0326-compare-upgraded-rds-at-fixed-load.md) - Five-minute upgraded-RDS repeat passed at 100.89 paid-issued tickets/s with zero customer errors; runtime restored; 102/s hourly profile pending.

- [ADR0327: Push five-minute throughput at the existing cloud budget](0327-push-fixed-budget-five-minute-throughput.md) - Clean120/s repeat passed all12 gates at118.63 paid-issued tickets/s with zero customer errors or drops; original failure and recovery preserved; hourly qualification pending.

- [ADR0328: Test 140 offered journeys per second at the existing budget](0328-push-140-journeys-fixed-budget.md) - Passed all12 gates:138.49 conservative paid-issued tickets/s,42000 fulfilled,zero customer errors or drops; financial audit/drain/restoration passed; higher-rate hourly qualification pending.

- [ADR0329: Test 160 offered journeys per second at the existing budget](0329-push-160-journeys-fixed-budget.md) - 160/s test failed arrival/zero-drop gates;47875 dispatched payments/tickets reconciled and runtime restored. Best clean short result remains138.49/s; generator calibration required.

- [ADR0330: Retire the secondary ECS from the CCE workflow](0330-retire-secondary-ecs-from-cce-workflow.md) - Primary-only bootstrap qualified live with post-TTL audit, drain and exact restoration; final subset 111 tests passed. Secondary VM empty; deletion pending console access.

- [ADR0331: Add customer and engine demonstration views](0331-add-customer-and-engine-demonstration-ui.md) - Customer and engine views privately published in sample mode; 20 adapter/bridge tests and browser journeys passed. Actual engine financial integration remains unverified.

- [ADR0332: Calibrate generator process scaling](0332-calibrate-generator-process-scaling.md) - Sealed synthetic160/s two/four-process and200/s four-process controls passed; no new paid capacity qualified.

- [ADR0333: Repeat160/s with four generator processes](0333-compare-four-generator-processes-at-160.md) - Live160/s comparison failed with7905 active-capacity drops;40095 paid/issued reconciled; best clean138.49/s unchanged.

- [ADR0334: Use a bounded creator-clock reference for fixture admission](0334-validate-fixture-freshness-against-bounded-creator-clock.md) - Preserve strict freshness and retain raw receipts across small measured clock offsets; no application TTL change.

- [ADR0335: Separate active journeys from HTTP connections](0335-separate-active-journeys-from-http-connections.md) - 800-slot160/s test failed:8848 drops and five final failures;39151 payments/tickets independently reconciled. Source/resource budgets unchanged; no capacity improvement qualified.

- [ADR0336: Remove the extra authentication thread handoff](0336-remove-authentication-thread-handoff.md) - Live160/s comparison failed arrival quality:10458 drops,37542 paid/issued,349 initial errors recovered and zero final failures. Conservative121.77/s; candidate not adopted. Independent financial audit and restoration passed; best qualified138.49/s unchanged.

- [ADR0337: Test payment delivery headroom with a fixed database budget](0337-test-payment-delivery-headroom-with-fixed-database-budget.md) - 32-slot cloud comparison failed:9361 drops,448 final failures,124.18/s conservative.38,260 paid/tickets and284 expired unpaid independently reconciled; original runtime restored. Retain sixteen slots; no capacity improvement.

- [ADR0338: Test API client pool headroom with a fixed server cap](0338-test-api-client-pool-headroom-with-fixed-server-cap.md) - Five-minute cloud comparison passed: 48,000 dispatched/confirmed, zero drops or customer errors; 158.0633/s conservative, all financial/drain/restoration gates passed. APIs 3+3 clients, 24 physical backends unchanged; hourly and maximum capacity unqualified.

- [ADR0339: Test 180 journeys/s with qualified API pools](0339-test-180-journeys-with-qualified-api-pools.md) - 180/s test failed:11,145 drops;42,855 completed,892 initial-error journeys recovered,zero final failures.139.43/s conservative. Independent financial/queue/restoration audit passed; original failure retained. Best passing158.0633/s unchanged.

- [ADR0340: Use compact local run monitoring and handoffs](0340-use-compact-local-run-monitoring-and-handoffs.md) - Local summary/watcher/evidence checker implemented;15 unit checks and both retained-run evidence audits passed. No cloud calls or capacity change.

- [ADR0341: Bound payment eligibility at claim statement start](0341-bound-payment-claim-eligibility.md) - Local SQL correction; 20 focused checks and immutable-parent local build passed. No cloud run or capacity gain claimed.

- [ADR0342: Bind the local payment claim comparison through existing builders](0342-bind-local-payment-claim-comparison.md) - Local configuration checks and explicitly approved immutable registry publication/pull verified; live registration and paid admission remain incomplete.

- [ADR0343: Qualify the payment claim cutoff at 180 offered journeys/s](0343-qualify-payment-claim-cutoff-at-180.md) - User approved180/s control then conditional180/s candidate; real control admission and535-file reproduction checks passed. Fresh control failed and independently restored; conditional candidate not run.

- [ADR0344: Reduce simulator transaction round trips](0344-reduce-simulator-transaction-round-trips.md) - Atomic claim and simulator transaction pipeline locally qualified:53 checks,22-module image verification and22.18% lower local DB operation time; cloud unmeasured.

- [ADR0345: Bind the simulator round-trip comparison](0345-bind-simulator-roundtrip-comparison.md) - Guarded160/s pair locally qualified:233 unique checks,535-file reproduction and8-role/22-module offline validation; unpublished, no paid allowance.

- [ADR0346: Reduce successful callback write waits](0346-reduce-successful-callback-write-waits.md) -300/s occupancy budget and default-off successful callback write pipeline;29 checks,22-module local build and22.364% lower local callback time; cloud unmeasured.

- [ADR0347: Qualify the combined confirmation path](0347-qualify-combined-confirmation-path.md) - Combined image published after explicit approval; control stopped before paid dispatch on inventory-role integration and fully restored; candidate not run.

- [ADR0348: Bind combined worker inventory renewal](0348-bind-combined-worker-inventory-renewal.md) - Corrected final pre-dispatch role integration/evidence retention;212 checks and actual preparation/admission passed locally; no automatic cloud retry.

- [ADR0349: Seal transferred combined worker verification](0349-seal-transferred-combined-worker-verification.md) - Portable observer verification qualified with both actual images,22 transferred helpers and267 regressions; fresh control stopped before traffic and independently restored.

- [ADR0350: Bind combined four-writer observation](0350-bind-combined-four-writer-observation.md) - Correct the combined label's historical three-writer fallback across startup, resource summary and pipeline validation; no automatic cloud retry.

- [ADR0351: Switch to the restored RDS endpoint](0351-switch-to-restored-rds-endpoint.md) - User-restored database switch with verified TLS and unchanged application role/pool limits; load tests remain paused.

- [ADR0352: Bind replacement-RDS capacity preparation](0352-bind-replacement-rds-capacity-preparation.md) - Approved new-RDS160/s control passed/restored; candidate observer passed but shared-time reserve blocked dispatch. Both restored; no automatic retry.

- [ADR0353: Bound replacement-RDS pair preparation time](0353-bound-replacement-rds-pair-preparation-time.md) - Preparation only: fixed7200s fresh-pair ceiling and3600s control ceiling; no paid authority.

- [ADR0354: Retain bounded creator-clock observations](0354-retain-bounded-creator-clock-observations.md) - Preparation only: retain raw timings and at most three strict read-only samples; no paid retry.

- [ADR0355: Qualify five-minute rates through200/s](0355-qualify-five-minute-rates-through-200.md) - Accepted: measured160/s candidate selected; preapproved180/s then200/s qualification with unchanged resources and149 focused cases.

- [ADR0356: Reconcile dispatched qualification cohort](0356-reconcile-dispatched-qualification-cohort.md) - Exact failed180/s recovery only; preserve original quality failure and reuse full database audit.

- [ADR0357: Redistribute fixed worker budget](0357-redistribute-fixed-worker-budget.md) - Explicit five-writer/five-consumer allocation; unchanged15-worker3750m/7680Mi aggregate budget and application image.

- [ADR0358: Reconcile failed allocated dispatched cohort](0358-reconcile-allocated-dispatched-cohort.md).

- [ADR0359: Preserve six consumers and redistribute simulator CPU](0359-preserve-six-consumers-and-raise-simulator-cpu.md).

- [ADR0360: Cleanup and complete pre-fixture200 preparation](0360-cleanup-pre-fixture-200-stop.md).

- [ADR0361: Reconcile failed200 dispatched cohort](0361-reconcile-dispatched-200-cohort.md).

- [ADR0362: Coalesce reservation replay lookup and claim](0362-coalesce-reservation-replay-lookup-and-claim.md).
