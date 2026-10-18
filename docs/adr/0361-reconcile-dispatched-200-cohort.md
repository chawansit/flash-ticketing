# ADR0361: Reconcile the failed200 dispatched cohort

## Status
Accepted for exact operational recovery. Extends ADR0356/0358 without changing historical cases or failed quality.

## Context
ADR0359-200b adr0151-2cd43c402c23 scheduled60000;54320 dispatched and fulfilled,5680drops,125initial/recovered,0final. Conservative53315=177.7167/s, journey p956758.72ms. All owned runtime/namespace/resources/credentials restored; original financial target60000 failed count equality.

## Decision
Execute the existing bounded repeatable-read read-only database auditor for54320 actual dispatched, callbacks1 and all seven relationship/postTTL/uniqueness/pending predicates. Verify stable normal new-RDS runtime twice, generator idle and all queues zero. Register only exact run/ledger/raw hash with fresh evidence; preserve original target, quality failure and consumed attempt. No upward rate progression or repeat without a validated improvement under continuing human preapproval.

## Alternatives
Ignore drops, relabel target60000 audit, reset original run, or use local evidence audit instead of database reconciliation: rejected.

## Consequences
Operational recovery is independent of failed200 qualification; no capacity pass implied.

## Persistence, locking, messaging, idempotency, TTL and scaling
No application/database writes or semantic/resource changes. One bounded existing audit connection, same isolation/predicates/timeouts. Only exact journal recovery metadata changes afterward.

## Failure and recovery
Any missing financial relationship, pending work, topology/resource mismatch, stale evidence or hash drift leaves recovery blocked. Original report remains immutable.

## Validation evidence
Eight focused exact-case/negative tests and Ruff passed. Actual bounded database auditor executed successfully for all54320 dispatched payments/tickets with all seven relationships/postTTL/uniqueness/pending checks, stable normal new-RDS runtime twice, generator idle/queueszero, retained owned cleanup. Registered FAILED_RESTORED without rewriting raw/hash. Receipt docs/capacity/cce/rate-200-dispatched-recovery-2026-10-19.json, log tmp/adr0361-reconciliation.log. Failed200 quality preserved.
