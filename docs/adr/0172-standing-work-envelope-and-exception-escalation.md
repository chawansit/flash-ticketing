# ADR0172: Standing work envelope and exception escalation

- Status: Accepted; implemented and locally qualified, cloud controller unqualified
- Date: 2026-10-06

## Context

Per-experiment approvals have slowed routine work. Scope exhaustion and human pause share a flag. The user approved one standing envelope: existing resources only, time as required, current profile gates, reviewed code/evidence to a codex branch, main merges separately approved. The target remains 300,000 unique paid-and-issued tickets per hour, not a measured capacity claim.

## Decision

Record and enforce boundaries in docs/capacity/work-envelope.json above the existing bounded runner. New infrastructure spending is zero: no instances, resize, replicas, upgrades or paid service changes. Existing service charges continue; no overall bill cap was specified. Record cumulative time without an overall cap, explicitly authorized by the user, while bounding every experiment.

ADRs document engineering decisions and do not automatically require another approval. Diagnose, fix, profile, verify and report independently. Escalate only new spending/infrastructure, requirements or correctness changes, publication outside the approved branch, or unresolved recovery requiring user involvement.

Every experiment receives a fresh append-only reservation, exact artifact/configuration/source binding and ledger. Retain old scopes and evidence. One exclusive shared run lock serializes reservation and execution. A safety qualification and measured stage share one reservation. Preserve fresh qualification and all existing customer, financial, post-TTL, queue and restoration gates. Initially register only the unchanged ADR0171 control: extending the registry requires implementation and local qualification, not another human approval within the envelope. No new load levels or database sampling are implemented by this decision.

Reserve a conservative 3,600-second experimental window before cloud access and never refund an ambiguous or failed reservation. Track actual elapsed time including cleanup separately. Check deadline and human pause before remote experimental actions; mandatory evidence/financial checks, owned-job shutdown and restoration remain available after stopping. Existing bounded remote jobs and cleanup paths are retained. This is not an unattended scheduler or a hard billing cutoff.

Human pause is independent of scope exhaustion. Legacy scopes remain closed. Only a validated standing reservation can bypass the legacy exhausted-scope flag; explicit pause is never bypassed. Drift and uncertain ownership fail closed. Push reviewed code and sanitized evidence only to codex/ branches; main merges and other destinations require approval. Publication policy does not replace review or secret checks.

## Alternatives

Repeated approvals retain the bottleneck. Removing counters or reopening consumed scopes weakens replay safety. Unlimited untracked cloud execution hides resource exposure. Scheduling unattended work exceeds this request. None selected.

## Consequences

Failed-but-restored experiments may be followed by a corrected fresh experiment inside the same envelope. New profiles still need an ADR and executed local tests. Existing-resource costs can accumulate during long work. Gates are inherited from the exact profile and never relaxed: the current paid profile requires every scheduled customer fulfilled within its existing completion deadline, no drops or customer retries. Reported p95 timings are measurements unless the profile supplies a threshold.

## Failure and recovery behavior

Persist before cloud access; never refund or replay. A crash retains active ownership and blocks another reservation. Failure with verified restoration closes as failed-restored. Missing restoration, integrity or duplicate-booking evidence blocks more load pending recovery. Atomic file replacement plus the exclusive run lock serialize state. Keep ownership locks on uncertainty.

This supersedes ADR0170 and ADR0171 only for fresh human-approval/replacement restrictions within the standing envelope. Historical identities, consumed scopes, frozen controls and gates stay unchanged. ADR0171 application/persistence/messaging decisions remain in force.

## Validation evidence

Implemented standing policy, fresh reservation/ledger integration, exact binding, legacy-scope rejection, pause/deadline guards with cleanup exemption, controlled qualification-to-measured progression, time accounting and publication guards. Executed 368 broader regression tests (3 publication-hook cases initially skipped), then 65 final boundary/admission checks with no skips, including the actual Windows Git-shell hook cases. Runs overlap and are not summed. Repository-wide Ruff and canonical-name checks passed. Corrected three pre-existing import-order lint errors only; no application transaction logic changed.

Evidence: docs/capacity/work-envelope-validation-2026-10-06.json; raw local outputs tmp/adr0172-qualified-tests.txt and tmp/adr0172-final-boundary-tests.txt. Initial default status made zero cloud calls and created no reservation. The initial qualified registry contains only the unchanged ADR0171 control. No cloud load, live controller qualification, capacity improvement or hourly production qualification was performed.

## User boundary amendment: shortest sufficient experiments

On 2026-10-06 the user retained no cumulative time cap and required every experiment to remain bounded, tracked and as effectively short as possible. Select and record the shortest predeclared window sufficient for the hypothesis and required gates. Prefer offline/local checks and short diagnostics; extend only for necessary evidence, intermittent behavior, stability or hourly qualification. Equal baseline/candidate windows and complete post-TTL financial, double-booking, queue-drain and restoration checks remain mandatory. Avoid cherry-picked early-success termination. The 3,600-second ceiling is not a target and the existing qualified 300-second control is unchanged.

This amends the time boundary and engineering selection policy; no new orchestration pattern, runtime deadline, paid load profile, application code or cloud deployment is introduced. The earlier validation evidence remains historical evidence for the implementation described above; it is not a claim of newly executed tests for this amendment.
