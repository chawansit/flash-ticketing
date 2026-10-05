# ADR0154: Separate CPU observer placement from experiment factor

- Status: Accepted; corrected dry pair passed and restored; paid capacity comparison not authorized
- Date: 2026-10-05

## Context

The approved ADR0151 dry control arm passed loaded-source identity, 100-way safety, duplicate-payment replay, post-TTL durability and full queue drain. Its CPU jobs then exited with `Placement differs from arm`. The inherited ADR0147 CPU validator interprets control as four primary APIs and no secondary APIs. ADR0151 instead keeps two APIs per host for both off/on logical arms. This is a harness mapping defect; no backend financial failure was observed. The control failure stopped the pair and original runtime/queues were restored. One safety protocol and zero capacity stages were consumed; the qualification-pair allowance is consumed.

## Decision

Represent physical placement explicitly in CPU specs, independently of the logical control/candidate factor. Permit only `four-primary` and `two-plus-two`; preserve the historical inferred placement when the field is absent. Explicit four-primary remains control-only. ADR0151 supplies two-plus-two for both factors. Construct and validate each spec locally before uploading or launching the observer, retain its logical arm, carry placement into raw data and summaries, and reject differing physical placements when comparing hosts. Keep container-ID, host identity, API-only secondary, counts, time windows, restart/counter and completeness gates unchanged.

## Alternatives

Rename control to candidate only for CPU jobs: obscures which factor was measured. Ignore failed CPU collectors: loses required evidence. Allow arbitrary API counts: weakens topology gates. Re-run the failed customer protocol automatically: exceeds the approved single pair and hides a failed result.

## Consequences

The same validator tests the exact specs emitted by the runner. Historical ADR0147 inputs continue to mean the same topology. An adapter hash changes, invalidating old preparation and qualification bindings. No image rebuild or production configuration change is needed. All original failed evidence remains immutable.

## Failure and recovery

Unknown placement, wrong counts, mismatched host placements or logical arm prevents observer launch/comparison. Missing output remains failed. Existing financial audits, full queue drain and exact original restoration continue on any failure. A corrected replacement dry pair needs fresh explicit approval and new binding; do not reset or replay the consumed ledger. No paid stage is authorized.

## Persistence, messaging, idempotency, TTL and scaling

Observation contract only. No production pattern, connection budget, hold/payment transaction, Kafka semantics, cache freshness or physical scaling decision changes; none is superseded.

## Validation evidence

The [staging and dry report](../capacity/flash-sale-opening/status-refresh-staging-and-dry-qualification-2026-10-05.json) preserves the failed cloud control. Both CPU logs show the same placement exception. Local regression checks and result counts will be added after execution. Candidate live arm and performance benefit remain unmeasured.

Executed202 focused harness tests and Ruff passed. The exact retained live2+2 inventory produces valid corrected control CPU specs for both hosts. No corrected cloud protocol was run; the failed ledger remains consumed.

The user subsequently requested Continue for the identified corrected dry replacement. A fresh exact-bound scope now authorizes one pair, max2 simulated safety tickets and0 capacity stages. The earlier failed scope remains archived with its consumption/evidence; no further replacement or paid launch is authorized. Outcome remains pending.

The [corrected qualification report](../capacity/flash-sale-opening/status-refresh-corrected-dry-qualification-2026-10-05.json) records the executed drycb2e26816335: both logical arms passed CPU observation with explicit two-plus-two placement, full loaded-source/financial/replay/post-TTL/zero-double-booking/queue/restoration gates. Two simulated safety tickets were confirmed and zero capacity stages ran. Original primaryAPI4 restored, secondary resources/credential snapshots removed, generator idle, Kafka lag/all queues0. Old failed evidence and scope preserved. Performance improvement remains unmeasured; any paid comparison needs separate approval and matching dry age<=3600s.
