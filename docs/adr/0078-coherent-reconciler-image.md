# ADR 0078: Keep the reconciler on the same seat-map writer version as the API

Status: Proposed

## Context

A controlled 1,000 RPS, 30-second DCS observation on 2026-09-28 recorded 2,207 delta-history overlaps, 533 aggregate-version regressions within the same cache incarnation and 2,936 history-tail mismatches. The Redis server identity did not change. The API ECS still ran a reconciler container created about 26 hours earlier. Its `cache.py` SHA-256 was `215a97bb...`; the active refresh/expiry workers used `a92c148f...`. Both containers pointed to the same DCS address and pooled PostgreSQL service.

The old reconciler's full-snapshot Lua sets the aggregate version to the sum of seat source versions. The current writer maintains a monotone aggregate version and a bounded delta history. The old reconciler can publish a lower version under the existing incarnation and corrupt that history. The capacity deploy built and verified API, refresh and expiry but omitted the reconciler, even though reconciliation remains necessary for repair and periodic inventory warming. This omission also invalidates attribution of prior delta failures solely to DCS behavior or the current API implementation.

## Decision

Treat the reconciler as a seat-map writer in the controlled deployment. Build and force-recreate its single running replica from the same checkout as API and refresh before load, then verify its `cache.py` and `workers.py` hashes, running state and replica count. Verify `cache.py` for every active seat-map writer and API, not just `workers.py` or PostgreSQL source. Fail closed before load if any active writer has an unexpected image version. Rollback retains the current-version reconciler; it must not restore the incompatible image.

This supplements ADR 0057's split-lane deployment verification and ADR 0071's versioned seat-map delta contract. It supersedes ADR 0057 only where its capacity deployment verification omitted the dedicated reconciler. Reconciliation scheduling, leases, ownership, TTL, reservation locking and PostgreSQL durability remain unchanged.

## Alternatives considered

- Stop the reconciler for benchmark runs: rejected because this would conceal a production writer and remove periodic repair from the measured topology.
- Leave the reconciler running and reset Redis between runs: rejected because the old writer would corrupt the fresh maps again.
- Make API reads tolerate regressing versions: rejected because it cannot restore a correct delta chain and would hide incompatible concurrent writers.

## Consequences

The stage rebuilds one more worker and briefly restarts reconciliation before load. Image verification costs a small number of container commands. A mismatched image stops the stage rather than producing an invalid capacity result. Existing capacity comparisons with the stale reconciler need revalidation; they are not retroactively classified as passing.

## Failure and recovery behavior

A failed reconciler rebuild or hash check prevents load. Reconciliation claims remain durable and can be resumed by a new current-version worker. If a stage exits after deploying the new reconciler, rollback leaves that compatible reconciler running and restores the other recorded service counts. No bookings, holds or durable rows are deleted. A later version mismatch requires stopping load and deploying a coherent image set, followed by checking delta continuity and database ownership.

## Validation evidence

Before implementation, the active old reconciler command was `python -m ticketing.workers reconciler`; its Redis host matched the current refresh worker. Its Lua full-snapshot code recomputed aggregate `version` from source rows. The live atomic observer recorded the anomaly counts above without a DCS server-identity change. Acceptance requires a focused deployment test, shell syntax check, a short controlled load with zero same-incarnation regressions/overlaps/tail mismatches, exact durable holds, zero booking overlap and a drained fixed audit. Longer latency and production-capacity claims remain separate.
