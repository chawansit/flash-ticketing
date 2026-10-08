# ADR0218: Safety-only diagnostic abort recovery

## Status
Accepted under ADR0172. Implemented and locally tested; exact original failed scope resolved with an append-only verified receipt.

## Context
ADR0217 scope bounded_shared_callback_placement__e71e05f76ac7 stopped before paid dispatch. Its 1+3 inventory was rejected by the failure collector, whose allocation resolver recognized ADR0174 and ADR0177 only. Both arms independently passed the 100-way one-owner probe, signed callback replay, customer authorization, one unique paid ticket, post-TTL financial audit, full queues/Kafka drain and exact runtime restoration. The work envelope conservatively classified the failed safety report as RECOVERY_REQUIRED even though owned cleanup completed.

## Decision
Correct the allocation resolver to instantiate the exact ADR0216/ADR0217 contracts and require their complete markers and actual distinct immutable API identities. Exercise the real generated failure and slow-phase collectors for both arms before retrying.

Add a narrowly verified safety-only abort receipt. It may resolve a consumed shared_callback_placement safety failure only when no paid stage started, no customers were dispatched, both financial safety audits and exact original restoration passed, diagnostic credentials and owned resources were removed, generator is idle, and every global queue is drained. Hash and retain the original failed report and arm artifacts. Verify their scope/binding/run/ownership identities and the stored receipt on every subsequent availability check. Preserve the original report, failed diagnostic gate and RECOVERY_REQUIRED journal entry; append a separate resolution rather than rewriting a failure as success. A fresh reservation and full safety pair remain required for retry.

This extends ADR0172 recovery classification for proved safety-only cleanup; it does not supersede payment, inventory, admission or customer gates. No new infrastructure, higher load, scope replay or main merge.

## Alternatives
Ignore or rewrite the failed scope: loses evidence and could hide ambiguity. Repeat paid load while recovery remains unresolved: rejected. Ask the user to decide a routine collector correction: unnecessary within the approved envelope. Rerun financial probes after their owning resources have been restored: cannot reconstruct the original concurrency evidence and is unnecessary when its exact verified artifacts remain intact.

## Consequences
A tooling failure can be closed independently of its failed qualification while remaining visible. Financial or ownership uncertainty still blocks all new load. This receipt cannot qualify performance or bypass the next fresh safety pair.

## Failure and recovery behavior
Reject any missing, changed, symlinked, oversized or mismatched artifact, unpassed financial/durability/drain/restoration gate, dispatched customer, started paid stage, active scope or human pause. Receipt absence or later hash mismatch blocks availability. Do not modify cloud resources to fabricate evidence or reclaim unowned fixtures.

## Validation evidence
130 focused collector, safety recovery, placement and envelope checks passed. The corrected isolated recovery fixture test also passed independently. Ruff and canonical naming passed. The actual retained safety artifacts passed the verifier and a separate receipt was appended without changing the failed scope/report. [Recovery receipt](../capacity/flash-sale-opening/shared-callback-placement-recovery-2026-10-08.json). [Failed safety evidence](../capacity/flash-sale-opening/shared-callback-placement-safety-abort-2026-10-08.json). Original safety run: adr0151-e63b87f19898. No paid traffic dispatched; no capacity improvement measured.

## ADR0225 exact safety-only case extension

Extend this classifier only to consumed scope bounded_writer_write_pipeline_probe__bd6a3461abd9, report adr0151-3a1e553acc1c and candidate arm adr0151-arm-4c250bd06e90. Its backend safety, post-TTL financial, queue, source, ownership and restoration proofs all passed; zero paid journeys were dispatched. The retained pre-dispatch diagnostic capture failed with UnboundLocalError because its policy selector omitted the writer contract type. Require that exact cause, profile, binding, sole arm, zero paid counters and identical read-only index proofs, plus every existing safety/cleanup gate. Hash all three original artifacts; preserve the failed report and journal entry. No arbitrary writer abort is accepted, no paid count is relaxed and no old scope is reopened. Correct the selector, test the real diagnostic allocation path, then use a fresh fully qualified experiment.
