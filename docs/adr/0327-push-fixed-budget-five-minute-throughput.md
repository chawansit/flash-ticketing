# ADR0327: Push five-minute throughput at the existing cloud budget

## Status

Clean same-rate repeat passed all twelve automatic measurement gates and restored the normal runtime. This qualifies the120/s five-minute profile only; hourly and maximum capacity remain unmeasured. The prior failed run and independent recovery remain preserved. The user requested more throughput for five minutes on 15 October 2026. Supersedes no application, financial, locking or durability decision. ADR0312 remains the102/s historical workload; this opt-in profile adds exactly120/s.

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


### Clean same-rate repeat

The user instructed continuation. A fresh one-stage goal authorizes the same120/s300-second workload after the audit correction, without changing the application image, resources or connection budgets. The full120-show terminal audit was constructed locally before cloud access;55 focused regression tests passed again. The original failed run and its recovery remain immutable. New cloud outcome pending.


### Clean repeat result

[Fresh fully passed result](../capacity/cce/fixed-budget-throughput-clean-repeat-2026-10-15.json): run `adr0151-ac8debc44a23`, ledger `bounded_cce_ticket_target__ecd5af0cff30`, `PASSED_RESTORED`. The offered window was15 October2026,07:20:17-07:25:17 Bangkok time. All36000 journeys dispatched and completed with zero initial customer errors, retries, final failures or generator drops. Journey p95 was1761.06ms (worst-shard methodology); payment HTTP p95 was106.16ms and reservation durability p95 was1094.53ms.

The same-run mandatory post-TTL financial audit completed successfully and verified36000 paid orders, payments, bookings and tickets, with zero double-booking, payment loss or broken relationships. PostgreSQL timestamps count35849 unique paid-and-issued tickets inside the five-minute window; the two-second inner guard counts35589, or118.63/s using the full300-second denominator. This is17.59% above the passed102/s offered-load reference; the prior120/s observation was118.65/s. No application or resource change caused that rate increase: only offered load increased from the reference. Latency variation between the two120/s samples is not attributed to the audit-only correction.

All twelve measurement gates passed, including the final unchanged-pod/background/ingress checks omitted by the earlier failed audit. The new pre-dispatch terminal-audit marker was true before traffic. Both pre- and post-restoration queues were zero; owned namespace/resources and temporary credentials were removed; the generator was idle and normal runtime restored. No recovery exception or supplemental audit was required for this repeat. Hourly qualification, production concert-opening behavior and maximum capacity remain outside this five-minute result.
