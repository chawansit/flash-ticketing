# ADR0162: Failure-time acquisition evidence and conservative partial timeout reclamation

- Status: Accepted for local reproduction and conditional opt-in implementation; cloud unqualified
- Date: 2026-10-05

## Context

ADR0161 control completed 17,996 of 18,000 customer journeys: four payment-initiation503s and one callback503 recovered by simulator redelivery. Three PoolTimeout and two TooManyRequests failures occurred in adjacent one-second samples on one replica. These cannot prove occupancy at rejection or the exact admission ceiling responsible.

ADR0133 retains timed-out checkout slots until the entire native queue is empty. Installed psycopg_pool3.3.1 keeps expired WaitingClient entries in its FIFO, pruning them during connection handoff. Public requests_waiting is the current FIFO length. An expired prefix can disappear while live waiters remain, leaving the guard counting more retained positions than the whole remaining queue. This is a local hypothesis, not the proven cloud trigger.

User continue authorizes diagnosis, one evidence-based fix and local validation. No replacement cloud stage or deployment follows this decision.

## Decision

First reproduce partial FIFO pruning with the real dependency/PostgreSQL. Capture shared-guard rejection snapshots under its admission lock and native failures immediately before slot release. Record fixed role/reason counters and failure-only structured fields: numeric native-pool state, live/retained guard occupancy and acquisition elapsed time. Attach evidence to the original exception for request-log correlation. Exclude SQL, DSNs, driver messages, payloads, customer/payment IDs and idempotency keys. Preserve HTTP503/Retry-After; no automatic retry.

After confirming the mechanism, add default-off API_PARTIAL_TIMEOUT_RECLAIM requiring the existing shared acquisition guard. For each role, retain min(previous_retained, current_native_requests_waiting), releasing only excess positions. Do not subtract live acquisitions or guess which waiters are active. Native max_waiting, per-role and shared ceilings, callback reservations, connection limits, deadlines and financial transaction bodies remain unchanged.

## Safety argument and limits

A role's remaining expired entries are bounded by both its retained count and the whole physical FIFO length. Their minimum is conservative. An in-flight timeout not yet released remains counted as a live acquisition. The guard lock prevents concurrent role-count changes; newly enqueued live calls already hold slots. Native pruning only lowers the actual expired count. Callback/payment aliases each reconcile conservatively against the same queue; never assign queue positions between roles. This may still retain extra slots and reject overload. It does not guarantee zero errors or higher sustained capacity.

## Alternatives

Empty-queue-only reclamation is safe but can retain already-pruned positions. Immediate timeout release can expand native backlog. Subtracting live acquisitions is unsafe because admitted calls may not yet be queued. Private waiter inspection/mutation adds driver-version/locking risk. Increasing pool/waiter/deadline budgets or request retry confounds the comparison; none selected.

## Consequences

The candidate recovers only demonstrably excess conservative slots. Failure evidence makes rejection diagnosable without high-frequency polling. Fixed-cardinality error metrics/logging add overhead during a prolonged failure storm. Defaults and frozen cloud images remain unchanged. Source/profile export and a fresh bounded same-load comparison need qualification before deployment.

## Failure and recovery

Counters stay nonnegative and mixed admissions never exceed existing ceilings. Diagnostics must not replace a driver failure or prevent slot release. Native errors, cancellation and pool closure preserve recovery. Disabling the flag restores empty-queue-only reconciliation; no migration is needed. HoldTTL, ownership, payment idempotency, booking uniqueness, outbox/Kafka and late-refund rules remain unchanged.

## Decision relationships

If locally qualified and enabled, partially supersede ADR0133 only at partial native queue timeout reclamation. Preserve its budgets, ADR0145 callback reservations and ADR0160 receipt/confirmation semantics. Do not rewrite earlier failed gates.

## Validation evidence

Before implementation: psycopg_pool3.3.1 getconn, _getconn_unchecked, _add_to_pool and public requests_waiting source inspected. The baseline was reproduced before implementation; results follow below. [Cloud control evidence](../capacity/flash-sale-opening/async-payment-confirmation-measured-control-2026-10-05.json).

Required: real expired-prefix/live-tail FIFO pruning; baseline rejection versus candidate progress; mixed/alias budget conservation; exact failure snapshots and sensitive-field exclusion; original deadlines; same-key payment/callback replay, durability,100-way seat contention, expiry/refunds and test queue drain. Append only executed results. Cloud cause/capacity benefit remain unproven.

Executed baseline reproduction: one real PostgreSQL17.6 / psycopg_pool3.3.1 test passed. Four expired positions were physically pruned while one live waiter remained; baseline still retained four timeout slots and rejected another checkout at the unchanged role ceiling. The owned test resources were removed. This establishes the local mechanism only; it does not attribute the cloud failures. Evidence: tmp/adr0162-checks/baseline-result.json and baseline-tests.txt.

Failure evidence distinguishes an exact guard decision from adjacent native pool measurements: the guard count/reason is captured under its lock; public native metrics are not an atomic combined snapshot with that lock. Diagnostic exceptions never replace driver failures or prevent slot release.

First broader native attempt stopped during collection because the new unit/integration files shared a basename. Both were read-only inputs and owned resources were removed. Rename the unit module before qualification; this setup failure is not a behavioral result.


Local qualification executed:99 focused checks and the full supported host suite of1,180 tests passed. Native application integration plus focused unit modules produced355 passes,1 skip and1 failure. Real partial FIFO baseline/candidate, candidate-enabled mixed HTTP financial replay, callback reserve,100-way atomic holds, ambiguous payment commit/replay, expiry/refunds and local queue recovery cases passed. Financial transaction AST and seat/payment authority files remain unchanged; changed-file Ruff and diff checks passed.

The sole core failure was18,000-seat full projection replay exceeding the unchanged Redis100ms deadline. The exact case passed alone with the same code/deadline. Preserve the failed broader run: this is an unresolved intermittent stability limit, not a clean full-suite result or demonstrated cause. Earlier broad native harness setup failures are retained in the summary; host tests ran on their supported Git/frozen-resource environment. All owned native test resources were removed and copied inputs remained unchanged.

[Local validation summary](../capacity/flash-sale-opening/partial-timeout-reclamation-local-validation-2026-10-05.json) records attempts, source hashes, limitations and raw local evidence paths. No cloud call, deployment or load occurred. The flag remains off. Before capacity measurement, qualify a pinned admission-only profile: identical source/diagnostics/budgets and async confirmation off in both arms, changing only reclamation0->1. Async confirmation requires a separate comparison after the control is stable. Prior consumed cloud scopes stay closed.
