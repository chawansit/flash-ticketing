# ADR0275: Preserve recovery in worker-placement comparisons

## Status
Accepted correction to ADR0271 execution; its three-attempt customer recovery decision and all existing correctness/SLO gates remain unchanged. No new paid-stage allowance is granted.

## Context
Run adr0151-df0b0a270c9d passed CCE safety and ran84 offered journeys/s for300seconds. The worker-placement selector was accepted by the transaction profile, but customer_recovery_bundle.enabled omitted ADR0271. Consequently the stage transferred the historical no-retry generator and recorded recovery_max_attempts1. Baseline ADR0266 used3. The run is failed and cannot establish a fair placement improvement:3982 drops,389 status503 outcomes,57 payment503 outcomes,20772 customer-confirmed tickets. Financial totals report21161 paid tickets and57 unpaid expired orders. Independent recovery is separate ADR0276.

## Decision
Reject any mismatch between the selected recovery workload and the bound attempt count during PaidStage construction, before resource creation or dispatch. Enable the already sealed ADR0254 recovery workload for ADR0271, preserving parent bytes, machine sizes, roles, delays, connection budgets, workload and image. Test the actual PaidStage construction and coordinator/CLI selection with a validated ADR0271 goal, including the80-file sealed generator bundle, explicit3-attempt arguments and recovery binding. Unsupported profiles retain the historical no-retry workload. Record source overlays without changing historical hashes. Preserve the failed original report and consumed paid-stage allowance.

## Alternatives
Compare the unmodified run with a recovery-enabled baseline: invalid. Add retries only in the summary: dishonest. Change pool sizes or pods at the same time: confounds diagnosis. Reset consumed allowance: violates accounting.

## Consequences
Corrects a benchmark implementation error; it does not remove measured pool pressure or prove higher ticket throughput. Replay safety, same idempotency keys, bounded jitter and customer authorization use the previously reviewed recovery implementation.

## Failure and recovery behavior
Reject source/manifest/contract drift before paid dispatch. Retain failure evidence and independently reconcile paid outcomes. Do not change quality gates or restart paid load without remaining authorization. Recovery cannot conceal sustained overload.

## Validation evidence
Implementation and targeted regression execution pending. Cloud run failed and is not comparable; no capacity improvement claimed.

Executed local verification:48 worker/recovery/paid-stage tests passed, including actual constructor selection of the sealed80-file bundle, missing/incorrect binding rejection and a forced selector omission rejected before dispatch.107 transaction/shared-comparison/reproduction tests and53 envelope tests also passed. Ruff passed;535 retained source identities verified. Application image and historical generator bytes unchanged. Corrected workload has not been cloud-retested; consumed paid-stage allowance is preserved.
