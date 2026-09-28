# ADR 0077: Atomically publish the private cloud load manifest

Status: Proposed

## Context

The backend stage starts read-only observers before prewarming the fixture. Both steps use the same private manifest path inside one API container. The prewarm step copied a root-owned mode-600 file directly over the path and then changed its owner to the API user. During the gap, the already-running delta observer could receive PermissionError and exit. This happened in runs 20260928T161113Z-108402fb and 20260928T162150Z-8633a825 even though the observer's synchronous startup smoke test passed.

The manifest contains private fixture data and must remain mode 600. Observers must see a complete readable file throughout their run.

## Decision

The prewarm step copies the manifest to a temporary path in the same container filesystem, changes its owner to the API user, then atomically renames it over the active manifest. The observer startup smoke remains in place. Preflight and observer setup occur before concurrent readers exist, so they retain their current direct copy and ownership sequence.

## Alternatives considered

- Start every observer only after prewarm. Rejected because existing CPU, database and queue measurements intentionally include the prewarm phase and changing that timing would reduce comparison value.
- Make the manifest world-readable. Rejected because it contains private fixture information.
- Retry PermissionError in the observer. Rejected because it would conceal an avoidable publication race and could still lose early diagnostic samples.
- Copy the manifest once and never replace it. Deferred because the current runner may select different API replicas across phases; each phase must ensure its chosen container has the manifest.

## Consequences

The active manifest remains readable by the API user throughout replacement. The temporary file exists briefly with restricted permissions, and a failed ownership or rename step causes the stage to fail before running load. No application booking or persistence behavior changes.

## Failure and recovery behavior

If the copy or ownership change fails, the previous active manifest remains intact; the stage exits before prewarm completes. If the atomic rename succeeds, both prewarm and observers read the same complete fixture version. Stage cleanup may remove leftover temporary files later, but cannot expose fixture data through broader permissions.

## Validation evidence

Before this change, both runs named above recorded an observer PermissionError on the private manifest immediately after observer startup, while the stage continued to prewarm and generate load. The first run's DCS observer succeeded when it happened to use an API container not overwritten at that instant, confirming the race is scheduling dependent. Acceptance requires Linux shell syntax validation and a short cloud stage with a nonempty delta-chain summary, exact durability, zero booking overlap and drained queues. Capacity latency gates remain independent.
