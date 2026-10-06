# ADR0177: Bounded application-role rebalance comparison

## Status
Accepted, implemented and locally qualified under the standing work envelope. Registered locally; cloud execution is stopped at the user boundary. No cloud deployment or load was started.

## Context
ADR0176 provides application-role diagnostics without statistics grants. Its collector is locally qualified but not connected to the bounded runner. ADR0174's paid control completed 18,000 tickets but failed diagnostic coverage; its candidate was skipped. Hidden foreign sessions remain unidentified and WAL causality is unproven.

## Decision
Register a separate application_role_rebalance profile, using ADR0176 diagnostics identically in both arms. Preserve ADR0174 and all historical full-database profiles. Reuse its exact frozen application images, settings, aggregate connection budgets, 60 journeys/second and 300-second windows; compare only API placement 2+2 against 1+3. A five-minute window preserves the identified baseline and intermittency coverage; the 3,600-second experiment deadline is a ceiling including safety, audit and restoration, not a target. No new infrastructure or permission grants.

Before safety or paid fixtures, prove from actual private inspected configuration that all four APIs and fourteen database workers share the observer's role/database. Attach only a fingerprint and exact replica coverage to the existing source-qualified inventory receipt. Verify the observer's effective identity with a bounded read-only preflight before creating a paid fixture. Pin the profile, parent policy, collector and transferred observer bytes. Reject wrong or missing diagnostic modes instead of silently falling back.

Use the distinct application_database_wait_evidence_complete gate and retain full_database_visibility_complete as a separate, possibly false, observation. Never emit the old full-database gate as a scoped success. Keep admission and slow-phase diagnostics, customer latency/errors, authorization, post-TTL durability, zero double-booking, complete global queue/Kafka drain and exact restoration. A failed control stops the candidate. Reserve only fresh exact-bound scopes through run_work_envelope.py; registration and local checks do not reserve or start cloud work.

This does not supersede ADR0173, ADR0174 or ADR0175. It implements the separately scoped alternative accepted in ADR0176. Application persistence, locking, messaging, idempotency and TTL are unchanged; runtime placement is still experimental, not adopted.

## Alternatives
- Grant full statistics access: requires an administrator/provider action and broader privileges.
- Replace ADR0174's visibility gate in place: rejected because it changes historical comparison semantics.
- Ignore missing diagnostics: rejected because incomplete evidence cannot support attribution or adoption.

## Consequences
The comparison can attribute application-role waits while exposing foreign invisibility. Aggregate WAL/checkpoint observations cannot establish foreign-session identity or prove a WAL cause. Configuration drift, missing replicas or mixed database roles block execution. Registration adds no capacity claim.

## Failure and recovery behavior
Fail closed on source/role/mode drift, malformed or hidden application activity, observer preflight failure, sampling gaps, resets or overhead. Preserve failure evidence and mandatory financial audit, drain and owned restoration. Stop progression on a failed control; never reopen consumed identities. Uncertain restoration blocks further load. The current user instruction stops this task before any cloud deployment or load.

## Validation evidence
Executed 284 focused regressions in 138.88 seconds and one real PostgreSQL integration test in 0.24 seconds. Ruff passed; the isolated 21-module source contract was verified. The native helper completed in 3.906 seconds and removed its owned container/volume. Tests cover both placement layouts, exact role/source/budget checks, diagnostic-mode rejection, real transferred-file hash validation, effective preflight, the bound scoped gate, failure cleanup and registry dispatch before reservation. Local inspected-replica evidence is synthetic; database visibility/preflight are native. A candidate compatibility defect and test-driver assumptions were corrected before the final passing suite. Cloud accounting, application code and schema remained unchanged. See [local qualification](../capacity/flash-sale-opening/application-role-rebalance-validation-2026-10-06.json). No capacity improvement or production qualification is claimed.

## Cloud validation follow-up
Fresh dry a27b10cd3e24 passed both placements after ADR0178. Measured control 61ba705c24bc completed 18,000 paid-and-issued customer journeys with zero errors, payment loss or double-booking, post-TTL full drain and exact restoration. Three scoped visibility-validation samples failed, so the control failed overall and the measured candidate was skipped. No placement benefit or higher capacity is qualified. See [sanitized control evidence](../capacity/flash-sale-opening/application-role-rebalance-control-2026-10-06.json).
