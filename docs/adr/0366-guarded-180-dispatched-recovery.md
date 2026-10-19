# ADR0366: Reconcile the failed guarded180 cohort

## Status
Accepted2026-10-19 for operational recovery only, within the authorized run cleanup. No capacity acceptance change.

## Context
ADR0365 fresh180 run adr0151-4575dd20f256 scheduled54000, dispatched53997 and fulfilled53996. There were3drops,99initial payment errors,98recoveries and1final payment502. Original audit expected54000 paid orders and failed. Retained counts show53997orders,53996paid/issued and1expired unpaid order, with zero pending/duplicate/relationship errors. Raw hash035d79fe0a28d73b147cd45c188265438e204d9583bb733e4731bbb0045b1aed must remain unchanged. Guarded53223<53280; failed quality prohibits200.

## Decision
Use the existing read-only bounded database auditor with expected53997orders and53996paid tickets/callbacks. Independently verify the one expired unpaid order, all payment/ticket relationships, post-TTL completion, zero pending/duplicate records, drained queues, stable restored runtime, idle generator and retained owned-resource/credential cleanup. Register only this exact consumed run as FAILED_RESTORED after actual database evidence; never promote performance or reset the attempt. No persistence, locking, messaging, idempotency, TTL, scaling or resource change. Existing accepted recovery decisions remain in scope for their own exact runs.

## Alternatives
Treat53997as paid: falsely requires payment for the one failed unpaid journey. Assume retained counts prove reconciliation: insufficient; actual fresh database audit is required. Ignore failure or rerun: rejected.

## Consequences
Operational restoration can succeed while customer quality stays failed.200remains blocked. No new load authority.

## Failure and recovery
Any incomplete or inconsistent audit leaves RECOVERY_REQUIRED. Exact raw/binding/fixture hashes, customer counts, clock freshness, cleanup and audit predicates are mandatory. No writes to application data; only the stopped run bookkeeping changes after evidence passes.

## Validation evidence
Nine focused validation cases and Ruff passed. Actual bounded read-only database reconciliation passed:53997orders,53996paid/issued/callbacks,1expired unpaid,0pending/duplicates/relationship errors; post-TTL, stable normal runtime, generator idle and all queues clear. Registered FAILED_RESTORED in47.328s; raw hash unchanged. Receipt: docs/capacity/cce/guarded-180-dispatched-recovery-2026-10-19.json.
