# ADR0304: Bind the runtime cohort comparison to the existing paid runner

## Status

Accepted for implementation and a bounded control/candidate comparison. Implemented and tested with a completed, restored control/candidate pair. The candidate remains opt-in; no capacity increase or hourly qualification established.

## Context

ADR0302 measured the actual indexed observer and ADR0303 implemented a guarded alternative. Idle query savings do not establish customer benefit. The existing paid runner already verifies sources, immutable application images, financial correctness, independent diagnostics and restoration.

## Decision

Add an explicit short-only observer query discriminator, with exact SQL and helper fingerprints in the experiment binding, verified transfer receipt and every cohort sample. Keep the default ADR0283 query. Activate ADR0303 only for the candidate after verifying the exact measured baseline.

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


## Executed paid comparison

Both fresh 84/s, 300-second runs passed original gates and restoration. Each dispatched and fulfilled 25,200 journeys with zero drops and zero final customer failures. Control had zero initial errors; candidate had 13 initial payment 503 journeys (0.0516%), all recovered.

Control/candidate journey p95 was 2,670.94/2,678.64 ms; exact-window unique paid-and-issued tickets were 25,098/25,100. API CPU was 1.293/1.202 cores, worker CPU 1.309/1.345 cores. Candidate observer mean SQL execution fell 11.16%, and buffer hits per sample fell 21.86%. These query savings did not produce a meaningful customer-throughput or journey-latency gain.

All post-TTL financial checks passed, zero double-booking/payment loss was verified, global queues drained, owned CCE resources were removed and original services restored. Candidate errors occurred 133.20–172.25 seconds into offered traffic with API identities unchanged; pre-dispatch readiness passed. The captures show occupied payment slots, including commit and query phases. Nearest one-second database samples are correlation; they do not identify an exact per-request backend cause.

Keep ADR0283 as the default query. Do not increase load based on this pair alone. Next investigate payment-slot occupancy and the remaining write/I/O cost; further observer tuning is not the primary performance priority. [Comparison](../capacity/cce/runtime-cohort-paid-comparison-2026-10-13.json), [failure-time analysis](../capacity/cce/runtime-cohort-candidate-failure-analysis-2026-10-13.json).
