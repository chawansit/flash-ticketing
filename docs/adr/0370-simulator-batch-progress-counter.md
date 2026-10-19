# ADR0370: Preserve the simulator progress counter on batch delivery

## Status
Accepted2026-10-19. Corrects ADR0368's instrumentation and supersedes its candidate image pin for new qualifications. No architectural or acceptance-criteria change.

## Context
ADR0368 run4163debac412 completed all54000customers with0errors/drops, all financial relationships valid and guarded issuance53415. Terminal-log correction passed. However, new batch refill submitted the undecorated delivery helper, bypassing the existing measured_work("simulate_one") wrapper. Actual simulator calls stayed0while process CPU advanced and callbacks completed. The strict worker-progress gate correctly failed. ADR0369 reconciled and closed the failed attempt; it cannot authorize200.

## Decision
Add a measured simulate_claimed_one wrapper exclusively for callbacks submitted by the batch path. Preserve the legacy simulate_one wrapper without double counting. Keep shared claim timing in its existing claim phase and per-delivery completion/error metrics under the existing simulate_one operation label. Preserve all batching, SQL, locks, lease fences, idempotency, TTL, resources, defaults, error rules and financial/observer gates.

Build a fresh immutable candidate from the exact UUID parent plus the frozen ADR0368 overlay and the small progress correction. Retain all historical image/evidence files. Add an actual PostgreSQL regression that runs batch refill, reads emitted Prometheus metrics and requires3completed callbacks to produce exactly3successful operations. Do not invent progress by changing captured traces or relaxing the gate. Fresh180must pass and restore before200.

## Alternatives
Reclassify the old run or tolerate zero simulator progress: rejected. Decorate the shared helper and double-count legacy calls: rejected. Change payment/claim behavior at the same time: unnecessary.

## Consequences
New-image qualification is required even though the correction only restores instrumentation. Per-delivery work duration excludes the shared claim transaction, which remains separately measured. No causal throughput claim is made from the prior clean customer result.

## Failure and recovery
Default-off feature rollback and existing financial/drain/restoration remain unchanged. A failed new round stays failed and stops progression. No reset, infrastructure resize or main merge.

## Validation evidence
Original defect reproduced from actual retained worker metrics:0simulate_one calls from first through last sample despite54000fulfilled. Corrected actual-image PostgreSQL suite51passed/1optional comparison skipped, including3completed batch callbacks emitting exactly3simulate_one operations.12focused mode/authority tests passed, including actual22-helper/22-runtime network-disabled rehearsal and extended log transport. Exact19pod resources/manifests and isolated real-parent admission passed; local cleanup passed. Sourcec1fe95e270e8ae1b723ecc8108d17f59583b557e14ab6ff095e70df6b09eb920; published/pulled manifestcb03defd3cced76e65ee6c353668f61d2f71cf612998382a05140c3e017d7c47. No fresh cloud result claimed by preparation.
