# ADR0305: Bound final customer failures after recovery

## Status

Accepted on 13 October 2026 for an explicitly selected runner gate. Historical profiles and results remain immutable. This supersedes the zero-final-failure acceptance requirement only for fresh profiles explicitly bound to this decision; all financial correctness, dispatch, latency, diagnostics and restoration gates remain mandatory.

## Context

The user approved at most 0.01% final customer failures after bounded recovery. First-attempt errors are reported separately; recovery is not evidence that sustained overload is acceptable. At 25,200 dispatched journeys, two final failures fit the target; three do not. Existing sealed generators require every journey to succeed, and their raw outcomes must remain visible.

## Decision

Keep the proven sealed generator and its original pass result. Add a source-bound, opt-in local evaluator over its complete report: failure_count * 10000 <= dispatched_count, without rounding. Count every dispatched journey, include recovery and failures in existing total latency reporting, and require zero generator drops. Incomplete accounting, duplicate ticket/order identities, transport/coordinator faults and late successful completions still fail.

Separate customer confirmation from financial durability. The existing read-only, repeatable-read, post-TTL audit still executes every relationship predicate. For the new gate, permit missing/expired unpaid orders only within the observed final-customer failure count. Require every persisted payment attempt to have succeeded and have exactly one valid fulfilled order, booking and ticket, all callback deliveries complete, no duplicate seats and no unfinished queues. A customer who failed to retrieve a committed ticket still counts as a final customer failure; a paid transaction missing a ticket always fails financial integrity.

No new retry policy, authorization, persistence, locking, messaging, TTL, deployment or spending change. Recovery uses the existing idempotency key, operation lookup and bounded attempts. Both comparison arms use the same target. Report the original strict results alongside the approved outcome gate.

## Alternatives

Keep zero final failures: stricter than the approved business target. Relax generator drops or derive expected payments from customer successes alone: rejected because either hides unserved traffic or can conceal a committed payment. Change sealed historical generator files: rejected because it breaks reproduction.

## Consequences

A bounded customer failure can be accepted while any financial mismatch still rejects the experiment. Short runs cannot establish a production failure rate or an hourly capacity claim. No rounding or retrospective reclassification is permitted.

## Failure and recovery behavior

Failed gates stop escalation; mandatory post-TTL financial audit, full drain and exact restoration still run. Unknown or incomplete evidence fails closed. Disable the explicit goal selector to restore the historical strict gate.

## Validation evidence

Implemented the explicit gate; 78 focused tests passed, including the two/three failure boundary and financial-loss rejection. Legacy stage/lifecycle checks passed with their default contract isolated from consumed live bindings. Ruff and source compilation passed. [Local validation](../capacity/cce/payment-slot-outcome-local-validation-2026-10-13.json). No new cloud load or capacity claim.
