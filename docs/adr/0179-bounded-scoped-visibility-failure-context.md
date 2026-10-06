# ADR0179: Bounded scoped visibility failure context

## Status
Accepted and locally qualified under the standing work envelope; the new context has not been collected in cloud.

## Context
After ADR0178, both ADR0177 dry layouts passed, including three-API log collection. Measured control 61ba705c24bc completed 18,000 unique paid-and-issued tickets with zero customer errors, loss or duplicates and full drain/restoration. It failed only application_database_wait_evidence_complete: three application_visibility_missing validation samples about one minute apart. Each collected in 13.8–19.4 ms, below the overhead ceiling. The candidate was skipped. The collector discarded role-partition and backend-category counts on failure, so unknown ownership versus hidden application state cannot be determined from retained evidence. A periodic background session is a hypothesis, not an identified cause.

## Decision
Retain bounded sanitized category counts from the same materialized activity snapshot when scoped collection fails. Include nonnegative integer partition counts and at most 16 grouped backend categories, with explicit unknown/application/foreign role classification and hidden-state flags. Export no usernames, database names, PIDs, SQL, client addresses, connection strings or exception text. Map unrecognized backend labels to other rather than exporting arbitrary strings.

Expose fixed collector error categories and bounded failure-context peaks in the compact summary. Preserve the original MissingOrInvalidScope count, rejected sample status, broken continuity and null counter deltas. Do not interpolate failed samples, retry queries, ignore unknown ownership or claim visibility completeness. Keep the existing connection, read-only 100 ms queries, sampling cadence and 250 ms overhead ceiling. The added grouping uses the existing materialized snapshot and adds no round trip.

This supplements ADR0176/ADR0177 diagnostics and supersedes no ownership, financial, customer or restoration decision. No application image, transaction, connection budget, load, latency/error threshold or infrastructure change is selected.

## Alternatives
- Ignore periodic invalid samples: rejected; their identity and effect are unknown.
- Reclassify null-role sessions as harmless background work: rejected without evidence.
- Retrieve session identities and SQL: unnecessary and more sensitive than bounded category counts.
- Repeat the same long control without improving attribution: rejected; likely produces the same ambiguous failure.

## Consequences
The next fresh diagnostic comparison can explain failed ownership validation without weakening its gate. Category evidence can narrow hypotheses but cannot establish a WAL cause, prove ownership of unknown sessions or demonstrate increased capacity. Query grouping overhead still requires verification under load.

## Failure and recovery behavior
Keep failed controls and consumed scopes unchanged. Stop progression on incomplete visibility and preserve financial audits, drain and exact restoration. Missing or malformed context never changes a failed sample into a pass. Source changes require fresh bindings and scopes. Locally qualify privacy, bounds, strict rejection and real PostgreSQL query execution before future cloud work.

## Validation evidence
Executed 247 targeted collector/runner/observer/envelope tests in 89.71 seconds, plus one real PostgreSQL integration test in 0.24 seconds. The native helper completed in 4.656 seconds and removed its owned resources. Ruff and repository naming passed. Offline analysis of the original trace classified the same three failures and retained failed visibility, broken continuity and null counter deltas; it found no historical category context and did not rewrite the original report. See [local qualification](../capacity/flash-sale-opening/scoped-visibility-failure-context-validation-2026-10-06.json). Cloud workflow 4a39498e4eff used 1553.25 seconds, passed dry qualification a27b10cd3e24 and stopped measured comparison after its failed control, with exact restoration. No candidate performance or capacity improvement was measured.
