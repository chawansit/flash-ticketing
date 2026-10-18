# ADR0353: Bound replacement-RDS pair preparation time

## Status

Accepted for preparation only. Supersedes ADR0347 and ADR0352 shared3600s timing for explicitly selected future pairs only. Historical results and consumed allowances remain unchanged. Fresh paid approval is required.

## Context

The replacement-RDS160/s control passed and restored after about29 minutes. Candidate preparation passed safety cohorts and observer startup but could not satisfy the remaining900s reserve within the shared3600s clock. No candidate paid traffic was dispatched. Preparation, safety waits and restoration consume much more than the300s measurement window.

## Decision

Introduce an explicit ADR0353 timing selector restricted to the exact ADR0352 replacement-RDS comparison. A fresh pair has one immutable start/journal boundary, a7200s total ceiling and a3600s control admission ceiling measured from that same start. Candidate may start only after its fresh same-pair control fully passes and restores. Existing per-stage remaining-time reserves continue unchanged. The first hour bounds control admission, preserving the second hour for candidate preparation and measurement. These are admission ceilings; financial reconciliation and recovery must complete even after a deadline.

Both arms bind the exact timing contract and current source hashes. Require a fresh control: the retained passed control cannot match the changed runner source/timing contract. Never extend the consumed pair, reset an arm or admit a candidate from an older pair. Preview remains unable to reserve, deploy or dispatch. No new allowance is created by preparation.

## Alternatives

Reuse the passed control: rejected because source and timing bindings change. Extend the consumed pair: rejected. Remove900s safety reserve: rejected. Keep3600s total: observed insufficient. Unbounded preparation: rejected. Per-arm mutable clocks: unnecessary additional state; one immutable pair clock with a separate control ceiling is simpler to audit.

## Consequences

A complete pair may consume up to two hours of temporary cloud runtime before recovery. Each measurement remains160/s for300s with one paid stage per arm. This fixes experiment scheduling, not application throughput. Candidate performance and300/s remain unmeasured. Failed control prevents candidate dispatch; either arm failure consumes its allowance. No automatic rerun, resize, higher-rate run or main merge.

## Persistence, locking, messaging, idempotency, TTL and scaling

Only experiment timing metadata changes. Retain existing journals, consumed-arm locks, source/image identities, database transactions, messaging, idempotency, TTL checks and fixed resources. Application images remain unchanged. Existing database financial/post-TTL reconciliation, queue drain and restoration remain mandatory.

## Failure and recovery

Reject unknown timing selectors, missing replacement binding, malformed/future/naive starts, expired limits and insufficient stage reserve. Historical goals retain3600s behavior. Candidate rejects control with different timing/source/pair/journal binding. Cleanup remains mandatory after timeout; recovered operations do not imply passed measurement quality.

## Validation evidence

Executed97 focused checks passed, covering timing boundaries, invalid selectors/clocks/reserves, unchanged historical deadlines, preview denial, same-pair/control/source rejection, replacement-RDS binding and historical reproduction. Ruff passed. Actual published-image plans/manifests/bindings passed for both arms; real control reservation passed in isolated copied bookkeeping with live envelope/journal/state unchanged. Both immutable registry manifests and exact configuration digests passed authenticated read-only verification. Seven fresh authorization/execution/result/watch/registry templates compile and require a separate absent approval receipt. No cloud deployment, paid authority or live pair clock was created. Current live readiness must be checked before dispatch. [Readiness receipt](../capacity/cce/replacement-rds-pair-timing-readiness-2026-10-18.json).
