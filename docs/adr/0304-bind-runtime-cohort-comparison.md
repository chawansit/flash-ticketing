# ADR0304: Bind the runtime cohort comparison to the existing paid runner

## Status

Accepted for implementation and a bounded control/candidate comparison. Runner integration is implemented and locally validated; paid-load results remain pending.

## Context

ADR0302 measured the actual indexed observer and ADR0303 implemented a guarded alternative. Idle query savings do not establish customer benefit. The existing paid runner already verifies sources, immutable application images, financial correctness, independent diagnostics and restoration.

## Decision

Add an explicit short-only observer query discriminator, with exact SQL and helper fingerprints in the experiment binding, transferred inventory and every cohort sample. Keep the default ADR0283 query. Activate ADR0303 only for the candidate after verifying the exact measured baseline.

Use fresh separate 84 journeys/s, 300-second control and candidate reservations. Keep the ADR0298 committed-status flag enabled in both arms, four 1-vCPU/2-GiB APIs, fourteen 250m/512-MiB workers, physical database budget 24 and admission budget 20, payment timing, ingress, retries and original gates unchanged. A candidate requires a passing restored control from this decision, the same source/configuration identities, complete original measurement gates and zero final customer failures/drops. Independent waits, one-second sampling, financial audits, post-TTL checks and full queue drain remain mandatory.

This explicitly supersedes ADR0303's pending runner wiring for this opt-in comparison. It does not replace accepted application behavior or historical ADR0298 flag comparison prerequisites outside this new observer-only scope.

## Alternatives

Replace defaults or alter cadence: rejected because that changes the control. Rebuild the application image: unnecessary. Start the candidate using an older control with different monitoring sources: rejected. Expand the new runner: deferred; reuse the proven runner.

## Consequences

The observer receipt distinguishes the application flag arm from the query arm. Updated helper hashes require fresh source-bound diagnostic contracts in both reservations. No schema, payment transaction, persistence, locks, messaging, TTL, customer authorization, connection or resource budget changes. Hourly activation remains out of scope.

## Failure and recovery behavior

Reject orphan, altered, hourly or mismatched query bindings before dispatch. A failed control stops the comparison. SQL errors, missing query receipts, source drift or failed quality gates retain failed evidence and trigger existing cleanup/restoration; never fall back silently. Candidate activation must not bypass passing-control or source checks.

## Validation evidence

Executed 77 unit tests across the new comparison guard, existing replacement, committed-status comparison, native observer and hourly sampling suites; all passed. Ruff and compilation passed. Real cloud preflight verified the restored runtime and immutable image. [Validation receipt](../capacity/cce/runtime-cohort-runner-validation-2026-10-13.json). ADR0302 supplies query measurements; ADR0303 supplies three executed PostgreSQL query-equivalence cases; SQL text is unchanged. A fresh control/candidate run is being prepared; no new paid result or capacity claim.
