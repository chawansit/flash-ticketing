# ADR0311: Activate bounded gateway-read recovery on the qualified runtime

Status: Accepted; implemented and short cloud verification completed.

## Context
ADR0310 qualified the immutable hourly evidence under the approved 0.01% final customer-error target. One order-status 502 remained after a durable payment and ticket. The optional gateway-read client has local tests but is absent from the sealed workload. The adopted runtime uses four API pods, fourteen background pods, 24 physical database connections, admission 20, and three payment plus one general connection per API.

## Decision
Create a sealed child of the existing ADR0254/ADR0278 workload. Replace only the delivered recovery client: add 502/504 recovery to authenticated GETs for existing operations, retain the same three attempts, jitter, authorization, deadlines and inherited write/idempotency behavior. Preserve historical source and manifests.

Authenticate the exact ADR0310 hourly result and its separate approved-SLO assessment as the adopted application/runtime reference. Run a new 84 offered journeys/s, 300-second experiment at unchanged resources and connection budgets. This is a recovery verification, not a matched duration comparison or proof of increased capacity. Record the child contract in the plan, binding and transferred source evidence. Unselected historical profiles remain unchanged.

After all short gates pass, prepare a separately bounded offered-rate experiment toward 100 successful tickets/s. Offered rate is not successful issuance rate; count unique paid-and-issued tickets inside the offered window, and retain conservative window counts.

## Alternatives
- Rerun the old 2+2 payment-slot comparison: unnecessary for a client-only correction; the qualified runtime already adopted 1+3.
- Modify the historical recovery module in place: loses workload provenance.
- Retry all POSTs on gateway failures: risks ambiguous payment effects and changes a different factor.
- Treat a durable ticket as a successful customer response: hides retrieval failures.

## Consequences
The child can add read attempts during gateway faults, within existing bounds. First-attempt errors remain visible; recovered journeys and final failures remain distinct. A short pass cannot establish permanent error elimination, flash-opening capacity, or a new hourly qualification.

## Failure and recovery
Stop escalation on any quality, dispatch, integrity or drain failure. Preserve failed evidence and restore the normal runtime, remove private manifests and temporary credentials. Rollback selects the unchanged parent bundle. Zero duplicate charge, double-booking and payment loss remain absolute gates. Seat expiry and messaging behavior remain unchanged; no new persistence, locking, TTL or scaling pattern is introduced.

## Validation evidence
Pending: sealed-child mutation/replay/write tests, adopted-reference authentication and budget-drift tests, existing runner regression tests, and the fresh 84/s cloud run. No cloud load for this decision has started.

Executed203focused recovery/runner tests and Ruff checks passed. Fresh84/s300s run adr0151-19ac8d32b4df finished PASSED_RESTORED:25,200dispatched and fulfilled; zero drops, initial errors or final failures;25,099unique tickets inside the offered window; all original quality/financial/drain/restoration gates passed. No natural502/504 fault occurred, so bounded fault recovery is verified locally, not observed in this cloud run. [Evidence](../capacity/cce/gateway-read-recovery-result-2026-10-13.json).
