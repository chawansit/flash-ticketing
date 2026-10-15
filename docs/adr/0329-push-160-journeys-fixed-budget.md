# ADR0329: Test 160 offered journeys per second at the existing budget

## Status
Accepted; one higher-rate experiment completed but failed workload/qualification gates. Operational recovery verified; no higher capacity qualified.

## Context
ADR0328 passed all twelve gates at140 offered journeys/s for300seconds:41,547 conservative unique paid-and-issued tickets,138.49/s, with42,000 fulfilled journeys and zero initial customer errors, retries, final failures or generator drops. Journey p95 was2566.36ms. The user requests another increase.

## Decision
Run one fresh160 offered journeys/s300-second stage:48,000 journeys across160 isolated shows. Keep the exact ADR0324 image and application configuration, four API/fifteen worker pods,24 physical database backends, admission20 and generator concurrency500. Require at least47,400 conservative in-window unique paid-and-issued tickets (158/s), all existing latency gates, final customer errors at most0.01%, zero drops and zero financial violations. Keep gateway timing and customer recovery unchanged. Require the fully passed/restored140/s result. Construct the complete160-show terminal financial audit before manifest upload or paid dispatch. Retain exact historical102/120/140 profiles. Bound the experiment, including mandatory checks and restoration, to3600seconds. One paid stage only; no infrastructure resize.

## Alternatives
Repeat140/s to measure variance, jump to a much larger offered rate, or add resources. Choose the smaller workload-only increase to test existing headroom without conflating application or scaling changes.

## Consequences
Only offered rate and isolated fixture cardinality increase. Report first-attempt errors, recovery, final failures, generator drops, total journey latency, timestamp-window and conservative ticket throughput separately. A five-minute result cannot establish hourly production capacity, opening-time contention or a maximum. No accepted persistence, locking, messaging, TTL or scaling pattern is superseded.

## Failure and recovery behavior
Stop escalation if any quality, latency, financial, observer or cleanup gate fails. Preserve failed raw evidence and consumed experiment identity. Complete post-TTL financial checks, payment durability, zero double-booking, queue drain and restoration. Do not bypass source/image/budget guards, weaken correctness, or automatically dispatch another stage.

## Validation evidence
Execute exact-profile, full-cardinality audit, pre-dispatch rejection and lifecycle/source-binding checks before traffic. Verify live image/source/configuration, readiness and zero queues. Record only executed tests and measured outcomes below. No new cloud traffic has started when this decision is recorded.


### Executed preflight

366 focused regression tests passed (101 exact-profile/audit/budget checks and265 deployment/lifecycle checks). The import/undefined-name check passed. The full160-show/48000-ticket terminal audit compiled locally, and source/configuration binding matched the passed140/s reference under declared workload-only differences. Live read-only preflight verified image/source identity, normal runtime and empty queues. Fresh run `adr0151-edcf49a8b495` is active; no performance outcome is claimed yet.


### Failed workload and terminal reconciliation

The160/s stage delivered47,875 of48,000 scheduled journeys;125 were dropped at the configured active-journey limit. Sixteen initial payment503 errors all recovered; final customer failures were zero. Dispatch lag p95 was40.114seconds, so the intended arrival schedule was not maintained. Both generator processes used approximately99% of one core and reached250 active journeys each. This invalidates application-ceiling inference from this offered-rate comparison. No additional paid stage is authorized or started in this scope.

The original mandatory audit completed all seven relationship predicates, with47,875 unique tickets and no relationship errors, but correctly failed scheduled-count completeness against48,000. Do not change that result. After restoration, perform a separate read-only audit scoped to the47,875 actually dispatched journeys, repeat safety/runtime/queue/helper/namespace checks, and retain the original failed result hash. Only if those checks pass may the operational recovery block be cleared toFAILED_RESTORED. This does not pass the workload, remove drops, weaken the zero-drop gate or qualify higher capacity.


### Final retained result

[Failed workload evidence](../capacity/cce/fixed-budget-160-throughput-result-2026-10-15.json) and [independent dispatched-cohort recovery](../capacity/cce/fixed-budget-160-throughput-recovery-2026-10-15.json): run `adr0151-edcf49a8b495`, ledger `bounded_cce_ticket_target__3f90652ffbcd`, final operational status `FAILED_RESTORED`. The offered window was15 October2026,09:21:29-09:26:29 Bangkok time. Original raw result SHA256 `f996f66ca42ed31b36af3a47a6dd2f224643228d16fa72517e28a33d52c066b0` is unchanged and remains failed, with its original integrity flag false. No additional paid stage ran during recovery.

The test scheduled48,000 journeys;47,875 dispatched and fulfilled,125 (0.2604%) dropped at active-journey capacity. Sixteen initial payment503 errors (0.03342% of dispatched journeys) recovered in16 retries; final customer failures were zero. Worst-shard total journey p95 was4507.27ms versus2566.36ms at140/s. Dispatch lag p95 was40113.71ms and maximum41936.35ms; each shard reached250 active journeys, averaging about216, and used0.991 CPU-seconds per wall-second. The generator failed to maintain arrival timing. App-only capacity and a saturation ceiling cannot be inferred from this sample.

PostgreSQL timestamps place41,751 unique paid-and-issued tickets inside the offered window and41,521 inside the two-second inner guard (138.4033/s using the full300-second denominator). These are observed counts from a failed comparison, not a newly qualified rate. The best clean short result remains138.49/s fromADR0328. API process CPU averaged1.92 aggregate cores; worker CPU averaged1.79 cores. Payment-slot capture recorded19 failures. Generator CPU/client-side delays and payment-slot failures were observed together; this does not prove the exact server-side cause or eliminate the need to diagnose slot contention.

The mandatory audit ran all seven predicates, found47,875 unique tickets and no broken relationships, and failed scheduled-count completeness against48,000. Several native integrity gates depend on that overall audit; their false results must not be rewritten as passes or misreported as actual observed double-booking/payment loss. A separate read-only repeatable-read audit verified all47,875 actual dispatched payments, fulfilled orders, bookings, tickets and callback deliveries, with zero duplicates, pending records, broken relations, unpublished outbox or dead letters. Both safety cohorts were re-audited. Normal runtime stability, generator idleness, secondary emptiness, helper/namespace removal and zero queues were checked again. This cleared only the operational recovery block toFAILED_RESTORED; the original test, arrival, zero-drop, scheduled-count and issuance verdicts remain failed.

Next: calibrate generator pacing/CPU and consider more generator processes while retaining total concurrency500 and unchanged application/database budgets. Record a separate decision and validate host headroom and sealed customer behavior before another experiment. No automatic second stage, hourly run, resource resize or application-capacity claim is part of this completed scope.


### Supplemental provider CPU evidence

After the test, the user supplied a [three-hour RDS CPU screenshot](../capacity/cce/rds-cpu-three-hour-user-screenshot-2026-10-15.png), recorded in [supplemental context](../capacity/cce/rds-cpu-three-hour-context-2026-10-15.json). Its displayed Max/1minute series reports53.96% maximum,2.1% minimum and7.791% average. The three visible peaks appear to align with the120/140/160 offered-load windows; this alignment is an inference, not an extracted per-run series. The average includes long idle intervals.

The chart does not support sustained overall RDS CPU exhaustion as the explanation for the160/s failure. It does not rule out short bursts, single-core work, WAL/commit/storage/lock waits or connection-pool contention. The independently captured generator CPU/arrival constraints and19 database-slot failures remain relevant. Retain the database specification and connection budget while calibrating the generator; do not extrapolate RPS linearly from available CPU percentage. The original raw experiment and recovery hashes remain unchanged.
