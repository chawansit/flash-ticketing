# ADR0309: Admit terminal audits after paid work stops

## Status

Accepted and implemented for exact terminal recovery and a fresh hourly qualification. Amends ADR0308 terminal admission; supersedes its paid-action clock classification for mandatory terminal audits only. No clean hourly qualification claimed.

## Context

ADR0308 dispatched and fulfilled302,400 journeys with zero first-attempt errors, final failures or generator drops. The retained database window counted302,292 unique paid tickets,302,096 after the conservative guard. The first terminal financial call requested780seconds; the90-minute action guard rejected that reservation near the end of the experiment. Cleanup reran the identical post-TTL audit successfully. Raw result remains false and RECOVERY_REQUIRED because original measurement-gate assembly did not complete. This is an audit-admission failure, not evidence of a customer or payment timeout.

## Decision

Explicitly opt in to terminal read-only audit admission after the paid allowance is consumed, all generator jobs are confirmed stopped and the existing binding is rechecked. At that point the session enters the existing mandatory-cleanup mode before financial audit, queue drain and post-run verification. Preserve the780-second transport,600-second financial-query bound,150-second TTL bound, exact aggregate counts and seven relationship predicates. No extra paid dispatch, database connections, pods, retry or changed application image is authorized.

Retain safe admission diagnostics, including the requested transport and whether jobs stopped. Close only the exact consumed ADR0308 failed run from its authenticated raw financial/cleanup proof plus fresh read-only restoration verification. Keep its original result unchanged and status FAILED_RESTORED, with capacity_qualified false.

Repeat the same hourly runtime to obtain a clean single-run qualification. Recovery and measured ticket throughput do not retrospectively turn the failed run into a passing benchmark.

## Alternatives

Increase the90-minute envelope or shorten financial checks: rejected. Mark the original result passed because cleanup succeeded: rejected. Repeat application development or rebuild the tested image: unrelated. Skip terminal audit when the load window ends: unsafe.

## Consequences

The terminal audit belongs to mandatory recovery and cannot be prevented by reserving its maximum possible duration after paid work already ended. Paid dispatch and resource creation retain their original bounds. Mandatory cleanup may exceed the nominal deadline under the standing envelope, as already allowed; actual elapsed time remains tracked.

## Failure and recovery behavior

Unknown generator ownership or an unconsumed paid allowance blocks this opt-in. Audit, financial, expiry, relationship or drain failures still fail qualification. Original failed evidence is immutable. Recovery closes ownership only, never grants another paid stage or capacity qualification. A fresh hourly reservation remains required.

## Validation evidence

Terminal admission and hourly/observer regressions:46passed. Exact recovery and terminal admission tests:24passed (overlapping suite). Ruff passed. Fresh read-only financial audit matched all302,400 payments/tickets and all seven relationship counts; runtime, absent namespace/helpers, idle generator and queues verified. Recovery closed as FAILED_RESTORED with original result digest unchanged and capacity_qualified false. Evidence: [hourly measurement and recovery](../capacity/cce/committed-runtime-hourly-audit-recovery-2026-10-13.json). A clean repeat remains pending.

The same-resource hourly repeat executed terminal audit admission with stopped generators and binding rechecked, and completed its financial audit without the prior action-budget timeout. Full restoration and integrity passed. Its remaining customer classification issue is addressed separately in ADR0310; do not rewrite its raw result as passed.
