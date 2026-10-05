# ADR0153: Owned retrieval of original cloud parent images

- Status: Accepted for artifact retrieval and local derivation; no deployment or load authorization
- Date: 2026-10-05

## Context

ADR0152 validated offline derivation but the original cloud parent images were absent locally. Read-only pinned SSH inspection found the primary still at the frozen revision with its normal four-API topology and the generator idle. Public secondary SSH times out; the existing ADR0150 pinned private route succeeds. Six distinct originals serve the seven Python roles. The normal topology has no reservation writer: use the frozen API image for that role, matching the existing snapshot/deployment protocol.

## Decision

Reuse protected in-memory password authentication and pinned host keys. Fetch only the immutable originals derived from inspected running roles, with reservation writer explicitly sourced from the API parent. Validate the complete role map and primary runtime against the inspection before export. Create one fresh owner-only directory beneath the known repository tmp, check free space, and save the exact six images into one bounded archive. Record SHA256, size and file identity after export. Transfer into an exclusive local file, checking size/hash before Docker load, and inspect every resulting immutable original ID locally.

Do not replay export or transfer automatically after an ambiguous failure. Preserve partial evidence and require explicit owned recovery if needed. Remove only the exact owned remote archive and its empty owner directory after identity checks; never recursively remove arbitrary paths. Compare running container identities/start times before and after retrieval. No image pull, service restart, configuration/dependency/financial change, fixture or customer request is part of retrieval. Builds use ADR0152's offline per-role validation. Candidate host staging and live qualification remain subsequent scoped steps.

## Alternatives

Rebuild originals from current source: changes baseline. Use a tag or local test image: does not preserve original provenance. Save live containers: captures runtime data/credentials and is unnecessary. Unbounded retry/recursive cleanup: risks ambiguous ownership. Access secondary through unpinned private SSH: invalidates identity checks.

## Consequences

The original image layers and configuration can be verified locally before candidate construction. Temporary archive storage and network bandwidth are consumed, and local originals/candidates are retained. Archives/private inspection evidence stay under ignored tmp and are never committed. Retrieval and successful image construction are not performance or live safety results and do not grant paid-stage allowance.

## Failure and recovery

Unexpected topology, revision, role/image drift, ownership/symlink mismatch, space/size/hash mismatch or absent image stops progression. A partial file cannot be loaded as a verified artifact. Export/transfer failures remain failed and retain their paths/identities for bounded recovery. Cleanup is confirmed separately; no removal of Docker images or running services. Existing financial, double-booking, freshness, queue, latency and exact restoration gates remain mandatory in ADR0151 after fresh qualification.

## Persistence, messaging, idempotency, TTL and scaling

Artifact transport only; all accepted production decisions and fixed budgets remain unchanged. No financial or scaling ADR is superseded.

## Validation evidence

Read-only cloud inspection and TCP diagnostics executed; primary/generator public SSH reachable, secondary direct timeout, pinned private fallback succeeded. The [real-image preparation report](../capacity/flash-sale-opening/status-refresh-real-image-preparation-2026-10-05.json) records successful retrieval of all six originals in a 78,502,912-byte SHA-verified archive, exact owned remote cleanup and unchanged primary container identities. All six real candidate images built locally with verified 18-module parent/19-module candidate source/import/code, frozen dependency inputs, parent layers and full inherited runtime configuration. Both runner arms accept the completed receipt. Executed 402 local harness tests and lint passed. No cloud candidate staging, service deployment, safety ticket or capacity load has run. A concrete dry-only scope proposal remains inactive pending reply.
