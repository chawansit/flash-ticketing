# ADR0201: Worker comparison envelope entry point

## Status
Accepted for implementation under ADR0172 and ADR0200. Entry-point/accounting qualification completed and the worker profile is registered; fresh protected cloud preparation and live preflight remain required. No customer gate or production architecture is superseded.

## Context
ADR0200 assembles the guarded worker comparison. The standing runner still expects the legacy separate qualification/paid reports and cannot account for this single paired lifecycle. Allowances, ownership and closure must be recorded before any cloud action without treating a returned passing boolean as financial evidence.

## Decision
Add a dedicated worker profile entry point through run_work_envelope.py. Locally validate a protected prepared input package, original snapshot, exact archive, source expectations, dependency authority, CA and diagnostic target before reserving a fresh standing scope. Derive observer and frozen workload identities locally. Keep the fixed 60 journeys/s, 300-second window, four APIs, 14 workers, unchanged budgets and current customer gates. Use existing resources only.

Hold the existing exclusive experiment lock through reservation, execution and finalization. Record the exact run owner before connecting. Use the original guarded Session, WorkerComparison and protected credentials; no independent load command or replay path is introduced. Persist the paired result and its exact binding before closing ownership. Finalization independently validates arm order, consumed allowances, post-TTL payment durability, zero double-booking, complete queues, job termination, private cleanup and restoration. A failed safely restored control may close as FAILED_RESTORED; an ambiguous outcome remains RECOVERY_REQUIRED and blocks subsequent load.

A connection failure before comparison execution records zero mutation and zero dispatch, with unchanged allowances and no financial cohort created. After comparison execution is attempted, no missing report, failed staging ownership, unknown acknowledgement or absent audit can use that shortcut. Bootstrap zero-dispatch cases retain ADR0200 independent restoration and queue proofs. Failed or interrupted cloud actions are never replayed. Credentials are cleared and transports closed in finally; cleanup and finalization may run after pause/deadline. Lost final receipts retain recovery obligations.

## Alternatives
Adapting booleans into legacy paid gates would fabricate audit evidence. Reserving after connection or staging would leave unaccounted cloud work. Reopening a consumed scope or retrying ambiguous mutations would undermine ownership. Independent manual load commands would bypass the standing boundaries.

## Consequences
The profile is a controlled comparison rather than a capacity qualification. Protected prepared inputs may be rejected as stale by independent runtime checks; no automatic adoption occurs. The CLI and accounting need local fault tests before registration and a fresh live preflight before load. No new resource or spending authorization is granted.

## Failure and recovery behavior
Failed controls stop candidate progression. Source, configuration, receipt, owner, counter or binding drift blocks actions/closure. Original snapshot and consumed counters remain unchanged. A missing final journal, unsafe cleanup, unresolved financial audit or changed owner prevents successful closure. Record actual elapsed time, including failed attempts and cleanup, without refunds or cumulative time caps.

## Validation evidence
Executed evidence: [worker envelope qualification](../capacity/flash-sale-opening/background-service-separation-envelope-2026-10-07.json). The broad suite passed 830 cases with one obsolete unregistered-policy test failure; the corrected affected suite passed 209 cases, including one added preparation replay check. All 832 distinct cases have passing executed evidence across those runs; no single 832-pass rerun is claimed. The broad harness ran 23 real PostgreSQL financial cases and three existing isolated Linux cases and removed its owned database container. Connected entry-point tests use the actual coordinator with simulated transports, staging and customer backend replies. Registration and accounting are locally qualified; cloud throughput and production capacity remain unmeasured.

The entry point uses --execute --profile worker_separation with protected --config, --worker-inputs, --artifact (the fresh images.tar package), --worker-ca, --diagnostic-target and --ssh-runtime paths. The private input package has schema 1, decision ADR0201 and exact inputs, saved, inventory_sources and helpers fields. Archive receipts must be those produced by worker_separation_staging.prepare_package in a fresh canonical staging directory. Credentials are supplied through the protected terminal and are not included in reports. Default status and --worker-preflight remain read-only. Preparing fresh cloud inputs and executing the matched comparison are subsequent work, not completed tests.
