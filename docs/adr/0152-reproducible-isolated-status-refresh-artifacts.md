# ADR0152: Reproducible isolated status-refresh artifacts

- Status: Accepted; reproducible isolated images verified and used in the ADR0155 qualified cloud comparison.
- Date: 2026-10-05

## Context

ADR0151 verifies all 19 isolated modules, but preparation depends on an ignored temporary source tree. Rebuilding the current branch wholesale includes unqualified changes. The exact frozen cloud API parent image is absent from the local Docker daemon; the local test image is not a production substitute. A reproducible, reviewable source overlay and offline image derivation are required before a fresh cloud comparison.

## Decision

Commit an LF-normalized unified patch containing only the eight files already qualified in ADR0149, with a pinned patch digest and frozen base revision. Reproduce a fresh owned tree under repository tmp by exporting the frozen source, dependency files, migrations, scripts and tests, applying the allowlisted patch, and comparing every exported byte against its expected frozen or qualified hash. Reject extra/missing files, symlinks, unsafe paths and dirty or mismatched overlays. Never copy current-branch application sources.

Prepare a minimal build context from the verified 19-module map. Derive each role image from its supplied immutable original parent, retaining all parent layers and inherited runtime configuration. Verify parent source/import/bytecode and locked dependency inputs first. Copy identical qualified Python bytes into /app and the installed package, remove bounded package bytecode caches and update affected installed distribution RECORD entries; keep package version/dependencies and executable configuration unchanged. Do not install dependencies or access the network. Temporarily use root for the image build and restore the exact original user before recording an image. Verify candidate source/import/code, image parent layers and runtime configuration after build. Reuse one derived image for roles sharing a parent.

Default preparation performs no Docker build, SSH or deployment. An explicit local build command requires the complete role-parent map and the known frozen API parent. Produce a runner-compatible receipt only after every role succeeds. Local fixture builds may validate mechanics but cannot produce or qualify a cloud deployment receipt. Use fresh owned directories, preserve failed build evidence and never automatically prune images or retry uncertain builds.

## Alternatives

Copy the entire branch: introduces unrelated changes. Depend on the existing ignored directory: breaks fresh-clone handoff. Reinstall dependencies while building: changes the comparison and requires external downloads. Edit only /app: leaves installed imports stale. Treat a test parent as the cloud baseline: invalidates provenance.

## Consequences

Fresh clones can reproduce the source candidate from Git and a reviewed patch. Image bytes need not be identical across Docker versions; immutable IDs and verified provenance are recorded per actual build. Exact original parents must be available locally before production image derivation. A source bundle or fixture image proves no throughput improvement and does not grant cloud execution allowance.

## Failure and recovery

Hash, archive, import, bytecode, parent/configuration or dependency mismatch stops preparation/build. No receipt is emitted for a partial role set. Failed contexts and logs remain under the owned output for diagnosis; no cloud services are changed and no customer dispatch is possible in this tool. Artifact/config/source/adapter binding and fresh live safety qualification in ADR0151 remain mandatory after image transfer.

## Persistence, messaging, idempotency, TTL and scaling

This changes artifact preparation only. All financial transactions, locking, message delivery, idempotency, hold/cache TTL and connection/topology budgets remain governed by their accepted ADRs. ADR0151's temporary-directory prerequisite is superseded by this reproducible source preparation; its comparison and authorization gates are retained.

## Validation evidence

The [local validation report](../capacity/flash-sale-opening/status-refresh-artifact-local-validation-2026-10-05.json) records 385 passing local harness tests, lint and one executed real offline Docker fixture integration test. Fresh export verified all 207 files and the 19-module runtime map; path/patch/source drift, inherited configuration and partial/missing-receipt failures were exercised. The Docker fixture verified both package roots, import/code equality, all 19 installed RECORD entries and preserved image configuration. It produced two synthetic local images and no deployment receipt. The exact cloud API parent was checked and is absent locally; actual cloud candidate images and live performance remain pending. The patch has an explicit Git LF attribute so fresh Windows checkouts retain its pinned digest.

Git whitespace checking for the patch artifact permits the required space prefix on blank context lines; source application still uses `git apply --whitespace=error`. The pinned patch bytes are unchanged.
