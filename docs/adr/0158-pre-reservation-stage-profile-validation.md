# ADR0158: Validate the bounded stage profile before reserving a cloud run

- Status: Accepted; constructor correction locally validated; failed attempt and consumption counters retained.
- Date: 2026-10-05

## Context

ADR0157 image-cache staging passed on both ECS hosts without service deployments or customer dispatches. The first safety pair, adr0151-9cfc63e2bc5b, stopped after reserving its control arm but before entering the cloud driver. The real RefreshStages constructor raises Unsupported bounded stage contract: the shared Stages whitelist accepts bounded_status_refresh but omits bounded_status_refresh_dedup. Previous protocol tests mocked run_arm and did not exercise this constructor boundary. No SSH Session, safety fixture, paid ticket or capacity stage was started by the failed pair.

## Decision

Add only the exact (60, bounded_status_refresh_dedup, 1) tuple to the shared stage whitelist, requiring an isolated contract as with the original refresh profile. Keep arbitrary rates, ledgers and limits rejected. In the ADR0157 protocol wrapper, construct both real arm contracts and stages locally before entering the original protocol, acquiring its lock or consuming allowances. Add regression tests that exercise real constructors and verify constructor failure leaves the ledger and lock untouched.

This explicitly supersedes ADR0157's assumption that the shared stage constructor already supports the independent ledger. All source, approval, ordered-arm, financial, queue, restoration and freshness checks remain required. No application persistence, Redis locking, messaging, payment/inbox idempotency, hold TTL, cache freshness or scaling pattern changes. Images and machine/connection budgets stay identical.

## Alternatives

Reuse the consumed historical ledger: invalidates authorization and experiment isolation. Remove the whitelist: permits unbounded experiments. Patch a live constructor dynamically: hides the interface contract. Reset failed counters: erases consumption history. Only add the tuple: fixes this failure but still lets future constructor errors consume allowances before cloud access.

## Consequences

The constructor boundary becomes part of local preflight and regression coverage. This is a harness correction, not evidence of increased backend capacity. New adapter hashes require a fresh exact-bound scope and dry qualification. Preserve the original failed report and reserved counters; do not automatically replace the failed pair.

## Failure and recovery

On local preflight failure, propagate the error before any reservation or cloud call. Once the original protocol begins, its conservative lock and allowance rules remain unchanged. For this specific failed attempt, recover the exact owned lock only after proving the process exited, constructor reproduction matches the captured ValueError, and the arm directory contains no Session state or artifacts. Record no remote work separately from restored remote work. Do not claim safety, durability or queues passed for an unexecuted pair.

## Validation evidence

Before implementation: real constructor reproduction raises Unsupported bounded stage contract at run_two_host_paid_comparison.py; the failed arm directory is empty and no protected-password prompt occurred. Staging receipt proves runtime identities unchanged, generator idle and exact remote archive cleanup. Local corrective regression checks pending; no replacement cloud test or capacity measurement.


After correction, **245 targeted harness tests passed**, including four real deduplication stage constructors, exact rate/ledger/limit/contract rejection, and failures in either preflight arm leaving state and lock untouched. The four real constructor cases failed before the fix. Changed-file Ruff and diff checks passed. Existing source, authorization, 32 financial/queue gates, staging and legacy runner regressions were included. Local preparation verified the unchanged 19-module source and made zero cloud calls; images need no rebuild.

The failed attempt's exact lock was released only after process exit, constructor reproduction and empty arm-directory evidence proved no cloud Session had begun. Counters and the failed report remain preserved. A fresh exact-bound replacement proposal permits at most two simulated safety tickets and zero capacity stages; it is **not activated**. No safety/durability/queue gate is reported as passed for the failed pair.

[Correction evidence](../capacity/flash-sale-opening/order-status-dedup-stage-constructor-correction-2026-10-05.json).
