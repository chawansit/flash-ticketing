# ADR0153: Owned retrieval of original cloud parent images

- Status: Accepted; owned retrieval, local derivation and immutable image staging verified. Subsequent deployment outcomes are recorded in ADR0154/0155.
- Date: 2026-10-05

## Context

ADR0152 validated offline derivation but the original cloud parent images were absent locally. Read-only pinned SSH inspection found the primary still at the frozen revision with its normal four-API topology and the generator idle. Public secondary SSH times out; the existing ADR0150 pinned private route succeeds. Six distinct originals serve the seven Python roles. The normal topology has no reservation writer: use the frozen API image for that role, matching the existing snapshot/deployment protocol.

## Decision

Reuse protected in-memory password authentication and pinned host keys. Fetch only the immutable originals derived from inspected running roles, with reservation writer explicitly sourced from the API parent. Validate the complete role map and primary runtime against the inspection before export. Create one fresh owner-only directory beneath the known repository tmp, check free space, and save the exact six images into one bounded archive. Record SHA256, size and file identity after export. Transfer into an exclusive local file, checking size/hash before Docker load, and inspect every resulting immutable original ID locally.

Do not replay export or transfer automatically after an ambiguous failure. Preserve partial evidence and require explicit owned recovery if needed. Remove only the exact owned remote archive and its empty owner directory after identity checks; never recursively remove arbitrary paths. Compare running container identities/start times before and after retrieval. No image pull, service restart, configuration/dependency/financial change, fixture or customer request is part of retrieval. Builds use ADR0152's offline per-role validation. Candidate live qualification remains the separately scoped ADR0151 protocol.

Extension approved 2026-10-05: stage the verified immutable candidates by saving two local image archives: all primary candidate roles, and the secondary API candidate plus its exact original parent. Validate the same full source receipt before saving. Upload into fresh owner-only repository tmp directories using exclusive files, bounded size/time, SHA256 and sealed file identity. Verify the sealed archive before Docker image load, then verify candidate IDs, inherited configuration, source label and original parent layers on each host. Remove only the sealed archive and empty owned directory. Prove unchanged primary and secondary running container identities/start times and idle generator before/after staging. No service recreation, customer request or capacity stage is part of image staging. No automatic upload/load replay after ambiguous failure.

The user approved staging plus exactly one dry pair, capped at two isolated simulated safety tickets and zero paid capacity stages. Activate that exact artifact/configuration/adapter-bound ADR0151 ledger; preserve consumed earlier allowances. Qualification still checks runtime/import identity before safety dispatch, payment replay, post-TTL durability, zero double-booking, full queue drain and original restoration after each arm. It cannot authorize a paid-stage launch.

## Alternatives

Rebuild originals from current source: changes baseline. Use a tag or local test image: does not preserve original provenance. Save live containers: captures runtime data/credentials and is unnecessary. Unbounded retry/recursive cleanup: risks ambiguous ownership. Access secondary through unpinned private SSH: invalidates identity checks.

## Consequences

The original image layers and configuration can be verified locally before candidate construction. Temporary archive storage and network bandwidth are consumed, and local originals/candidates are retained. Archives/private inspection evidence stay under ignored tmp and are never committed. Retrieval and successful image construction are not performance or live safety results and do not grant paid-stage allowance.

## Failure and recovery

Unexpected topology, revision, role/image drift, ownership/symlink mismatch, space/size/hash mismatch or absent image stops progression. A partial file cannot be loaded as a verified artifact. Export/transfer failures remain failed and retain their paths/identities for bounded recovery. Cleanup is confirmed separately; no removal of Docker images or running services. Existing financial, double-booking, freshness, queue, latency and exact restoration gates remain mandatory in ADR0151 after fresh qualification.

## Persistence, messaging, idempotency, TTL and scaling

Artifact transport only; all accepted production decisions and fixed budgets remain unchanged. No financial or scaling ADR is superseded.

## Validation evidence

Read-only cloud inspection and TCP diagnostics executed; primary/generator public SSH reachable, secondary direct timeout, pinned private fallback succeeded. The [real-image preparation report](../capacity/flash-sale-opening/status-refresh-real-image-preparation-2026-10-05.json) records successful retrieval of all six originals in a 78,502,912-byte SHA-verified archive, exact owned remote cleanup and unchanged primary container identities. All six real candidate images built locally with verified 18-module parent/19-module candidate source/import/code, frozen dependency inputs, parent layers and full inherited runtime configuration. Both runner arms accept the completed receipt. Executed 402 local harness tests and lint passed. No cloud candidate staging, service deployment, safety ticket or capacity load has run. The user subsequently approved the dry-only scope. Staging extension checks and cloud outcomes will be recorded after execution; none are claimed by this decision update.

The [staging and dry report](../capacity/flash-sale-opening/status-refresh-staging-and-dry-qualification-2026-10-05.json) records executed immutable staging on both hosts, exact remote archive cleanup and unchanged runtime identities.115 staging/related checks passed. The separately approved dry pair stopped after its control CPU placement failure; financial/post-TTL/drain and restoration passed. One safety protocol, zero capacity stages consumed. ADR0154 records the local correction; replacement is not authorized.
