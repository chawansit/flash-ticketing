# ADR0339: Test 180 journeys/s with qualified API pools

## Status

Completed: the paid180/s300s stage failed; independent financial/restoration reconciliation passed and operational status is FAILED_RESTORED. This workload is not qualified. The single paid allowance is consumed; no further stage is authorized. ADR0338 remains the best passing short reference.

## Context

ADR0338 passed 160 offered journeys/s: 48,000 journeys completed without generator drops or customer errors, 47,419 conservative in-window tickets (158.0633/s), journey p95 3.71s, all payment-integrity and restoration gates passed. Four API pods used 3 general and 3 payment clients each while PgBouncer remained capped at 24 physical backends. API CPU averaged 2.40 cores out of four; pool acquisition mean was 11ms. Reservation durability p95 was 3.04s and writer command age mean 1.56s. These measurements justify a modest workload step, not unlimited scaling or another code change.

## Decision

Offer exactly 180 journeys/s for 300 seconds (54,000 journeys) over 180 distributed fixture shows. Require at least 53,400 conservative in-window unique paid-and-issued tickets (178/s), all unchanged quality gates, zero generator drops, and the approved final customer-error ceiling of 0.01%. Keep four generator processes, 800 aggregate active journeys, 500 HTTP connections, sixteen clients, four 1-vCPU/2Gi API pods and fifteen unchanged background pods. Keep API pools 3+3, shared admission 20, acquisition deadline 500ms, simulator 16 delivery jobs/10 clients, physical backends 24 with no reserve. Pin the same ADR0324 application image and all existing financial, polling, recovery, expiry and messaging behavior. Only offered rate and proportional fixture/expected counts change from ADR0338.

## Alternatives

Repeat 160/s first for reproducibility: useful for qualification, but this request prioritizes improving five-minute throughput. Jump directly to 300/s: unsupported. Add compute or database connections immediately: changes multiple factors without proving the next bottleneck. Increase retries, relax latency, or hide undispatched journeys: rejected.

## Consequences

Higher load may saturate reservation writers, API payment pools, PgBouncer or RDS and increase active journey occupancy. Report all scheduled/dispatched counts, first errors, recovery, final failures, latency, conservative issuance and CPU/database waits. A passing sample demonstrates only this short workload; it does not establish hourly, maximum or production capacity.

## Failure and recovery behavior

Preserve atomic seat holds, customer authorization, TTL, payment idempotency, payment/outbox atomic commit and duplicate-safe issuance. Stop escalation on any failed gate. Always reconcile paid tickets, drain queues and restore the exact original runtime within the 60-minute experiment boundary; cleanup may continue if needed for safety. Preserve original failed evidence. No automatic retry stage.

## Validation evidence

Pending focused workload/binding checks, historical profile regression, generated manifest/admission verification and the actual cloud stage. The completed ADR0338 report remains the immutable passing baseline. No higher capacity has yet been measured.

Local validation executed: 153 focused workload/profile tests and 228 isolated historical resource/lifecycle checks passed. Ruff F/I passed. Historical/current reproduction verified 535 files. Full generated four-API/fifteen-worker manifests, exact source/image binding and reservation passed in isolated files using actual existing publication, with no publication mock or live accounting changes. Original broader regression failures came from archived receipts inheriting a consumed live goal; isolated test context fixed collection without relaxing runtime guards. The proposed goal is PREPARED_NOT_AUTHORIZED. Automatic approval review rejected the combined push/launch because the user had not explicitly approved the exact180/s paid stage. No launch, reservation or paid load occurred; the provisional envelope authorization was withdrawn and the prepared goal retained. Explicit180/s approval is required. Cloud load and higher capacity remain pending.

Fresh authorization: the user replied Approved to the exact180/s300s cloud test request. Registered one paid-stage allowance at ledger index152, preserving all quality, financial and restoration gates. Higher capacity remains unmeasured until the stage completes.

### Pre-dispatch correction

Run `adr0151-b8a36980ef01` stopped before paid dispatch: the fixture-identity validator retained a fixed show-count allowlist without 180. The financial-audit validator also lacked 180 and would have blocked the next step. This is a missing workload integration, not an application capacity result. The run restored all services with integrity and cleanup verified; zero paid stages started. Extend only these explicit allowlists, preserve all identity/ownership/freshness/financial checks, and test complete local 180-show fixture preparation with simulated transports before retrying the unused paid allowance. Retain the failed report and bind fresh source hashes; no load gate is weakened.

Correction validation executed: 34 fixture/180-workload checks passed, including complete PaidStage.prepare with 54,000 tokens and simulated remote transfers, retained identity, compiled financial audit and duplicate-ID rejection. Revalidated the actual failed180-show fixture with its creator time and pinned legacy producer. Full generated admission/reservation and535-file reproduction passed again. The failed run used zero paid stages and restored all resources; fresh source contracts retain the same single unused180/s approval. See qualified-pools-180-predispatch-abort-2026-10-17.json.

### Executed paid result

Run `adr0151-fa8e65219b30` offered 180 journeys/s from **12:53:56 to 12:58:56 Bangkok** on 2026-10-17. Of 54,000 scheduled journeys, 42,855 dispatched and confirmed tickets; **11,145 were undispatched (20.6389%)**. All 892 initial-error journeys recovered using 940 retry attempts; zero final customer failures. Initial operation errors were 793 order-status503,114 payment503 and five payment-operation503; operation counts are distinct from error-journey counts. Arrival/completeness and observation requirements failed; journey latency worsened. The normal aggregate measurement-gate evaluation was not reached after the observer error. Worst-shard journey p95 rose from 3.71s to 6.92s; durability p95 was 5.06s, payment HTTP p95 266.97ms and ticket-wait p95 4.84s.

Observer-lifetime API process CPU rose from 2.40 to 3.55 cores of four allocated; acquisition mean rose from 11.00 to 21.34ms. Shared acquisition reached20 and991 API admission/pool errors were observed, including callbacks and safety/drain. Payment pickup mean rose from104.83ms to3146.14ms, while callback delivery mean rose60.92to76.74ms. Reservation-writer command-age mean was approximately stable (1.56to1.51s), as was successful PostgreSQL batch wall time (77.91to76.91ms). Cache misses rose48,759to138,178 and hits49,725to168,170 despite fewer completed journeys. These observations support investigating confirmation backlog and amplified status work; they do not establish a single causal database holder or prove additional compute alone fixes the problem.

Slot-history observation was incomplete and raised before the normal terminal financial/window audit. The original run therefore reported RECOVERY_REQUIRED with integrity unqualified. A separate read-only repeatable-read audit verified42,855 actual orders/payments/bookings/unique tickets and both safety cohorts, no double-booking/payment loss/invalid relationships, complete callbacks, elapsed hold TTLs and empty queues. Independent database timestamps counted42,117 tickets inside300s and41,829 conservatively, or **139.43/s**, below the passing158.0633/s reference and the predeclared53,400-ticket target. This independent evidence does not rewrite the failed load or observation result.

Fresh read-only verification proved exact original runtime stability, absent owned CCE namespace/helpers, idle generator and empty queues. Local operational recovery was closed as FAILED_RESTORED; the original report remains unchanged. No further paid stage started. Retain the passing160/s3+3 reference and isolate confirmation pickup/status-read amplification before another180/s test. More generator occupancy alone is not a service-capacity improvement.

[Measured failed result](../capacity/cce/qualified-pools-180-result-2026-10-17.json) and [independent reconciliation](../capacity/cce/qualified-pools-180-recovery-2026-10-17.json).

Original paid result SHA256: `666ca45c36f2b1a1394b5d9a5471f0755905e8a6a033da70e1ef7fb0433555b1`. Recovery SHA256: `521621f46c4dad8faf2b9a474accc536b2ffb505e3828199f03858c0e93e653c`.
