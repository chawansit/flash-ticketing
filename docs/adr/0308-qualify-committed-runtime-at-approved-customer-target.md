# ADR0308: Qualify the committed runtime at the approved customer target

## Status

Accepted conditionally on a fully passed and restored fresh ADR0306 short run. Not started or production-qualified.

## Context

The user requested hourly qualification after the short comparison passes: at least 300,000 unique paid-and-issued tickets inside the measured hour, at most 0.01% final customer failures, no undispatched journeys and zero payment-integrity violations. The proven hourly generator and its 90-minute experiment boundary already exist, but the committed-status runtime and new outcome gate need an explicit hourly binding.

## Decision

Reuse the existing hourly generator, fixture, early-observer adapters, complete audits and exact cleanup. Bind a freshly passed/restored short receipt, immutable ADR0297 application image, committed status publication enabled, chosen two/two or one/three API partition, ADR0305 outcome evaluator and ADR0307 live admission renewal. Keep four 1-vCPU/2-GiB API pods, 14 background pods, physical database budget24 and admission budget20, gateway timing and bounded recovery unchanged.

Offer 84 journeys/s for 3,600 seconds; 302,400 scheduled journeys. Require zero generator drops, all customer/latency gates, at least300,000 unique paid-and-issued tickets inside the conservative one-hour window and complete post-TTL payment/ticket relationships. At most30 final customer failures fit the approved target; this never permits payment loss, duplicate charges, double-booking or unfinished queues. Use a fresh reservation with at most5,400 seconds including setup, verification and restoration. Do not reopen a consumed short or hourly scope.

## Alternatives

Extrapolate five-minute counts: insufficient for qualification. Increase resources simultaneously: invalidates the fixed-budget result. Change gateway delays, retry counts or financial expectations: rejected. Run hourly before a short pass: rejected.

## Consequences

One continuous hour is needed to confirm the actual ticket target. A short pair may not establish a meaningful performance gain; choose the measured configuration and state its uncertainty. The hourly binding changes test duration and fixture size, not persistence, locking, messaging, idempotency or TTL behavior.

## Failure and recovery behavior

Any failed short prerequisite blocks hourly dispatch. Failed hourly customer, issuance or correctness gates block headroom escalation. Preserve raw failures and perform mandatory audit, drain, retirement and exact restoration even when the offered load fails.

## Validation evidence

Hourly reuse hooks implemented; authenticated fresh candidate binding dry-run passed. New prerequisite/runtime tests and observer-bootstrap regressions:35passed; prior broader checks produced100passed and4historical fixture errors, corrected by applying existing scope isolation to lifecycle/hourly fixture modules. The application, sealed hourly generator, latency thresholds and financial audit implementation remain unchanged. No hourly load or new capacity qualification yet.

The first hourly run measured302,292 unique paid-issued tickets inside the hour (302,096conservative), zero errors/drops, and all302,400payments reconciled in cleanup and fresh recovery. Original result remains FAILED_RESTORED because the terminal audit action budget blocked final gate assembly. ADR0309 amends that admission; a clean fresh repeat is required. [Evidence](../capacity/cce/committed-runtime-hourly-audit-recovery-2026-10-13.json).

The ADR0309 repeat completed the full financial, native identity and restoration checks. Its sole failed measurement gate was the omitted order_http_502 classifier label: one final customer error is below the already approved0.01% target. ADR0310 publishes a separately versioned QUALIFIED_UNDER_APPROVED_SLO assessment; the original runner remains FAILED_RESTORED. [Qualification evidence](../capacity/cce/approved-slo-hourly-qualification-2026-10-13.json).
