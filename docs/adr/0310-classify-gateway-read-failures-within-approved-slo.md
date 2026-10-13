# ADR0310: Classify gateway read failures within the approved customer SLO

## Status

Accepted for correction and explicit reevaluation after restoration. Supersedes only ADR0305's incomplete transient outcome enumeration. The0.01% final-failure target, original latency gates, zero drops and financial guarantees remain unchanged.

## Context

The completed ADR0309 repeat dispatched302,400 journeys, recovered24 of25 first-attempt errors and retained one final order-status HTTP502 failure. Its final-failure rate is0.0003306878%, below the user-approved0.01% limit. The generator reports the concrete customer-response outcome order_http_502, but ADR0305's helper omits that label and rejects it as an unknown failure. Every other original hourly measurement gate passed, including302,278 unique paid-issued tickets inside the hour,302,066 after conservative guards, complete financial audit and queue drain. The failed customer's payment and ticket persisted; customer confirmation failed.

The existing client retries429 and503 reads, but returns502 immediately. That recovery gap is a separate implementation improvement; old evidence must retain the observed unrecovered customer.

## Decision

Recognize order_http_502 as a known transient customer read outcome in the approved outcome evaluator. Keep unknown generator/coordinator/authentication outcomes blocked. Do not change any numeric customer, latency, accounting, financial, expiry, double-booking or drain threshold.

After complete restoration, retain the original raw run and its failed classifier result. Publish a separately versioned reevaluation of the same immutable evidence. It may qualify only when the original failure is exactly the missing order_http_502 classification, its old outcome checks differ only at known_customer_failures, every other original measurement gate passed, all original source/binding/result hashes authenticate, and restoration, financial integrity and credentials are verified. Do not claim a fresh load run or rewrite the old benchmark into PASSED_RESTORED.

Extend the customer read-recovery client to bounded502/504 read retries under its existing deadline, jitter and three-attempt maximum. Preserve original authorization, idempotency and payment-operation checks. Keep old sealed generator bundles immutable; activation of a new cloud recovery bundle is a separate tested workload change. No claim that the old failed customer recovered.

## Alternatives

Force a third hour solely because a known customer status label was omitted: does not add evidence about the approved numerical target. Ignore unknown outcomes generally: rejected. Hide the one final failure or add retrospective retries to its counters: rejected. Relax latency or financial checks: rejected.

## Consequences

Business acceptance and the original runner's result remain distinguishable. The same measured one-hour evidence can be evaluated correctly against the already approved SLO with a declared classifier version. Customer read recovery can improve in future comparisons without altering historical counts.

## Failure and recovery behavior

Any additional failed gate, changed raw receipt, missing restoration, unsupported failure, drop or payment violation blocks reevaluation and scaling. The raw run remains failed; only its separate approved-SLO assessment may pass. Future read retries stop at the original deadline or attempt cap, and retrieve existing results without writing or charging again.

## Validation evidence

Implementation and focused cases pending. Original customer counts and the completed financial audit are preserved; cloud recovery behavior for502/504 has not been retested.

Implemented classifier correction and separate immutable-evidence assessment.75focused customer accounting, recovery and reevaluation tests passed; Ruff passed. Actual evidence assessment is QUALIFIED_UNDER_APPROVED_SLO:302,278unique tickets inside the hour,302,066conservative, one final customer failure (0.0003306878%), zero drops or financial violations, queues drained and runtime restored. Original raw FAILED_RESTORED report and digest remain unchanged. Optional gateway read client is tested locally and not active in old sealed cloud bundles. [Evidence](../capacity/cce/approved-slo-hourly-qualification-2026-10-13.json).
