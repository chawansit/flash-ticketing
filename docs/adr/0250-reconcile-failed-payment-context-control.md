# ADR0250: Reconcile the failed payment-context control

## Status
Accepted and independently verified. This consumed control is FAILED_RESTORED. No candidate progression.

## Context
ADR0249 control adr0151-135b02478459 scheduled 25200 journeys, dispatched 20911 and confirmed 20817. There were 4289 generator capacity drops, ten payment HTTP 503s and 84 order-status HTTP 503s. Terminal aggregate counts show 20901 succeeded payments and tickets plus ten expired unpaid orders. The original target failed. The observer stopped on incomplete slot evidence: bounded headers now demonstrate ring overwrites rather than losing basic API metrics.

## Decision
Reuse ADR0246 independent read-only recovery for this exact consumed control. Pin the original report hash, ledger, binding, immutable image-pair receipt and 84-show fixture. Require 20911 orders, 20901 unique valid paid-and-issued tickets, ten expired unpaid orders, both safety payments, zero duplicates, post-TTL relationships, complete queues, stable normal runtime and exact cleanup. Preserve the original failed customer and observation results. Financial recovery cannot qualify capacity or justify candidate progression.

Use the existing set-based recovery SQL, read-only repeatable-read snapshots, 60-second paid query bound, 20-second safety bounds and 600-second recovery ceiling. Keep earlier validators immutable. No customer dispatch, payment retry, financial writes or resource changes.

## Alternatives
Clear the guard using aggregate counts alone: incomplete durability proof. Reopen the failed reservation or run its candidate: violates control gates. Treat the 84 confirmation errors as proven durable orders individually: aggregate agreement does not establish identity mapping.

## Consequences
The consumed experiment may close as FAILED_RESTORED only with complete independent evidence. Customer errors, generator drops and incomplete diagnostics remain failures. Read-only recovery time is added to cumulative accounting.

## Failure and recovery behavior
Any cohort, original hash, binding, payment/ticket relationship, queue, runtime or ownership mismatch leaves recovery required. Preserve timed attempts and original reports. No further load while restoration or integrity is unresolved.

## Validation evidence
Pending local exact-binding rejection tests and independent cloud verification. No capacity improvement or hourly qualification is claimed.

Executed 51 affected recovery tests passed, including three additional exact-pair/decision rejection cases; Ruff and repository naming checks passed. Independent verification completed in 45.610 seconds: all 20901 successful payments have unique valid tickets, ten unpaid orders expired, both safety payments and callback deliveries remained durable, every relationship check passed, queues are zero, normal runtime is stable and owned resources/private inputs are absent. Original failed report and one consumed stage are unchanged. [Failed-control and recovery evidence](../capacity/cce/payment-context-control-2026-10-09.json).
