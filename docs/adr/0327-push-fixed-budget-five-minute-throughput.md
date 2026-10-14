# ADR0327: Push five-minute throughput at the existing cloud budget

## Status

Measured higher ticket throughput with independent financial reconciliation. The automated run failed its audit coverage check; operational recovery is complete. This is not full automatic or hourly qualification. The user requested more throughput for five minutes on 15 October 2026. Supersedes no application, financial, locking or durability decision. ADR0312 remains the102/s historical workload; this opt-in profile adds exactly120/s.

## Context

ADR0326 passed102 offered journeys/s on the user-upgraded8vCPU/16GB RDS, with30600 completed journeys, zero customer errors or generator drops,1.70s worst-shard journey p95, complete financial checks and restoration. Fixed offered load does not measure maximum capacity.

## Decision

Run one fresh300-second stage at120 offered journeys/s,36000 distinct journeys across120 isolated300-seat shows. Require at least35400 conservative unique paid-and-issued tickets inside the offered window (118/s using the full300-second denominator). Keep the exact ADR0324 image, four API pods, fifteen background pods, four reservation writers at batch4, six fulfillment consumers at batch4, physical database budget24, admission20, per-API general/payment slots2+2, generator concurrency500, gateway timing and recovery unchanged. No infrastructure resize or waiting-room change.

Extend only the runner's explicit rate/fixture/issuance bounds and coordinator ceiling hash. Reject an unmarked rate increase, a modified reference, arbitrary rates, resource/budget expansion or a partial profile. Bind source changes to the fresh experiment. The prerequisite is the fully passed and restored ADR0326 raw report and ledger, independently of the older software-development references. Never relabel the original102/s evidence or its consumed identities.

## Alternatives

Leaving102/s unchanged cannot measure more throughput. Jumping directly to168/s adds unnecessary saturation risk. Increasing pods/connections or changing application code simultaneously prevents attribution. Weakening latency, financial or generator-drop gates is rejected.

## Consequences

The higher offered rate can expose queue or database saturation. The500-journey concurrency ceiling stays visible and may cause dispatch drops if journeys take too long. A passing short sample is not hourly or actual flash-sale-opening qualification.

## Failure and recovery behavior

Preserve first-attempt errors, recovery attempts, final failures, latency and generator drops separately. Keep the approved0.01% final-failure limit, zero drops, unchanged latency thresholds and zero payment loss/double-booking. On any failed gate, stop increasing load, retain evidence, perform mandatory post-TTL financial/queue checks and restore the captured runtime. No automatic retry of an ambiguous fixture or paid run.

## Validation evidence

Implemented the opt-in rate profile without application source or image changes. Executed294 relevant unit tests with isolated operator settings; all passed. Lint passed. Separate real-goal probes verified the exact prerequisite/source closure, unchanged runtime and connection budget,120-show ownership, generated fixture program,120/s generator arguments and concurrency500. [Local verification](../capacity/cce/fixed-budget-throughput-push-local-2026-10-15.json). Cloud preflight passed; paid-stage outcome is still pending. The prior result is [ADR0326](../capacity/cce/upgraded-rds-fixed-load-comparison-2026-10-15.json). Record executed checks and fresh outcome here after completion.


### Dispatched-stage audit defect and recovery

Run `adr0151-cde8461821c5` dispatched and fulfilled36000 journeys with zero customer errors or generator drops. Its original outcome is `RECOVERY_REQUIRED` because `cce_bounded_financial_audit.expectations` rejected120 show IDs before opening a database connection. The terminal-audit allowlist was omitted from the rate integration. The original raw report and its failed integrity/qualification result must remain unchanged.

Correct that exact allowlist, exercise the full120-show audit program locally and add pre-dispatch construction of the terminal audit. Independently repeat all financial predicates against the retained owned cohort, verify both safety cohorts and normal runtime/namespace/helper/queue cleanup, then append recovery evidence. Recovery may clear the operational block; it cannot retroactively pass the original benchmark or manufacture the omitted final pod/configuration observations. Do not dispatch more customers during recovery.


### Retained outcome

[Measured cloud result](../capacity/cce/fixed-budget-throughput-push-result-2026-10-15.json) and [independent recovery](../capacity/cce/fixed-budget-throughput-push-recovery-2026-10-15.json) retain the original failure. All36000 journeys dispatched and completed with zero initial errors, retries, final failures and generator drops. PostgreSQL timestamps place35850 unique paid-and-issued tickets inside the300-second window; the two-second inner guard retains35595, or118.65/s using the full300-second denominator,17.61% above ADR0326. Worst-shard journey p95 increased from1704.57 to2549.11ms; durable-reservation p95 increased from1042.39 to2024.57ms. Payment HTTP p95 was93.22ms. This is increased load, not an application optimization.

After restoration, an independent repeatable-read read-only audit verified36000 payments, orders, bookings and tickets, zero duplicates or broken relationships, and all seven financial predicates. Both safety cohorts were re-audited; runtime stability, namespace/helper removal, secondary-host emptiness, generator idleness and zero queues were checked again. Recovery started no additional paid stage. The ledger is `FAILED_RESTORED`; raw hash `d6135abcc896a3f3de6c467e7fcfda0060f28f1417b62ebe341b53a73f226f94` is unchanged. The omitted final pod/background/configuration checks were not retroactively marked passed.

The audit allowlist now recognizes exactly120 shows. Stage preparation constructs the complete terminal-audit program before uploading customer manifests. Executed55 correction regression tests passed, including120-show audit construction and rejection before dispatch when the audit cannot be built. The294 earlier tests passed but did not cover this missing terminal-audit case; their original receipt remains retained. A clean repeat at the same120/s is needed before treating this as a fully passed automated qualification. Do not claim an hourly maximum or increase load on this failed qualification.
