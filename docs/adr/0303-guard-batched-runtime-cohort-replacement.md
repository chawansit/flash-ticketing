# ADR0303: Guard batched runtime cohort replacement

## Status

Accepted for isolated opt-in implementation and correctness tests. Paid-runner wiring and runtime activation remain pending.

## Context

ADR0302 compared the actual ADR0283 runtime observer against ADR0247 on the verified completed cohort. All nine counters matched; the alternative reduced buffer hits 29.97% and mean EXPLAIN execution 10.41%. The observer accounts for substantial measured database buffer work. These idle results do not prove customer capacity improvement.

## Decision

Add an explicit opt-in installation function to the existing paid_cohort_query module. Accept only the exact measured ADR0283 query fingerprint before replacement; reject base SQL, an already replaced query or any drift without mutating the caller. Return baseline/candidate SQL fingerprints for subsequent identity-bound runner receipts.

Preserve the existing nine counters, payment delivery predicates, ticket existence checks and global queue/lock counters. Test against both original and actual indexed runtime SQL using isolated migrated PostgreSQL data, including mixed states, partial callbacks, missing payment/ticket rows, unrelated shows and duplicate cohort IDs.

Leave current observer defaults and deployed services unchanged. Later paid-runner integration must explicitly bind this helper, select control/candidate, preserve sampling cadence and financial audits, and use a fresh same-resource control before candidate load. Do not bypass source or profile validation.

## Alternatives

Unconditionally replace the observer default: rejected because it would alter the control and invalidate existing bindings. Accept any caller-supplied baseline fingerprint: rejected because it permits accidental drift. Rebuild the application image: unnecessary for this observer-only implementation. Reduce diagnostic frequency: rejected.

## Consequences

The correction is available as guarded code but is not active in the existing paid runner. Any future change to baseline SQL requires explicit remeasurement and fingerprint review. No application transaction, schema, locking, messaging, TTL, authorization or connection budget changes.

## Failure and recovery behavior

Unexpected or repeated installation fails before mutation; preserve the original query. A future candidate's SQL error or count mismatch must fail diagnostics and retain original evidence, with mandatory financial checks, queue drain and restoration. Rollback uses the qualified baseline query, not a retry that hides a diagnostic failure.

## Validation evidence

Executed six guard tests and three real PostgreSQL 17.6 integration tests on an owned local database. Original, actual ADR0283 runtime and guarded candidate returned identical results across empty/mixed cohorts, multiple seats, partial callbacks, missing payment/ticket rows, unrelated shows and duplicate show IDs. JSON receipt roundtrip and rejection without mutation passed. The owned database was removed; Ruff passed. [Validation receipt](../capacity/cce/guarded-runtime-cohort-correction-validation-2026-10-13.json).

ADR0302 supplies executed query-level measurements on the actual Huawei RDS. The candidate SQL fingerprint remains identical to that measured alternative. No current observer default or deployed service changed. Paid-runner wiring, fresh control/candidate load and customer-capacity validation remain pending.
