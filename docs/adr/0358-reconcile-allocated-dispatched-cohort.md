# ADR0358: Reconcile the failed allocated180 dispatched cohort

## Status
Accepted for exact operational recovery; extends ADR0356 without changing its retained case.

## Context
ADR0357 run adr0151-20e379923ea3 scheduled54000, dispatched/fulfilled36842, dropped17158, initial/recovered140, final failures0. Conservative35850=119.5/s, journey p9519033.55ms. Original target financial count54000 cannot pass. Runner restored owned namespace/resources/credentials and original new-RDS runtime. Failed quality remains failed.

## Decision
Use the existing bounded repeatable-read read-only financial auditor for the actual36842 dispatched cohort, callbacks1, all seven relationship predicates, post-TTL, uniqueness and pending counts. Bind registration to exact run/ledger/raw hash and original failed target. Independently recheck stable normal new-RDS runtime, generator idle and all global queues zero. Clear only this exact stopped scope after fresh complete evidence. Preserve original raw report/hash, target counts, failed quality and consumed allowance.

## Alternatives
Relabel target audit as pass, reset original report, or proceed upward: rejected. Leave operational recovery unverified: rejected.

## Consequences
Recovery authorizes no throughput qualification. Existing user preapproval continues only after complete recovery; any future attempt needs fresh identity and validated preparation.

## Persistence, locking, messaging, idempotency, TTL and scaling
No application/schema/write/lock/message/idempotency/TTL/scaling change. One bounded read-only database connection uses existing predicates/timeouts. Only exact local journal recovery metadata changes after evidence validation.

## Failure and recovery
Any missing ticket, relationship error, pending delivery, queue, topology drift or mismatched/freshness evidence leaves recovery blocked. No original evidence is rewritten.

## Validation evidence
Eight focused exact-case/negative tests and Ruff passed. Actual bounded database reconciliation executed and passed for all36842 dispatched payments/tickets, all seven relationships, post-TTL/uniqueness/pending, stable normal runtime twice, generator idle and zero global queues. Registered FAILED_RESTORED with original raw/hash unchanged; receipt docs/capacity/cce/allocated-180-dispatched-recovery-2026-10-19.json, log tmp/adr0358-reconciliation.log. No throughput qualification.
