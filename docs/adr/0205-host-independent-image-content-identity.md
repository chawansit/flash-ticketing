# ADR0205: Host-independent image content identity

## Status
Accepted for implementation. Supersedes ADR0187's inclusion of Docker-reported Size in cross-host immutable metadata identity. Archive bounds, platform, configuration, filesystem-layer, source/import and ownership checks remain mandatory.

## Context
The failed worker staging round stopped before deployment or load because Docker Desktop reports approximately 326 MB per image while the ECS daemon reports approximately 78.5 MB for the same six image IDs. Independent read-only comparison found only Size differed; Id, Os, Architecture, Config and RootFS matched exactly. Storage accounting is not portable across these daemon/storage implementations. The existing archive and image content proofs remain intact.

## Decision
Introduce staging contract schema 2 with decision ADR0205. Hash only immutable content metadata: Id, Os, Architecture, complete Config and complete RootFS. Continue validating a positive bounded Size on each daemon and retaining local image_sizes for storage/archive admission. Preserve exact archive bytes/hash, manifest image identities, all layer identities, worker source/import proofs and configuration bindings. Remote sizes must remain bounded but need not equal local accounting. Schema 1 contracts retain their existing exact metadata behavior; do not reinterpret old receipts or replay the consumed package. New preparation produces a fresh contract and owner.

## Alternatives
Using only image tags permits mutable drift. Ignoring configuration or filesystem layers would weaken identity. Requiring identical daemon Size falsely rejects matching content. Altering the old contract or rerunning an ambiguous scope loses traceability.

## Consequences
Cross-host validation follows immutable content while storage admission remains conservative and bounded. Existing historical receipts remain interpretable. No application, payment, database, Redis or Kafka delivery behavior changes and no capacity improvement is claimed from this correction.

## Failure and recovery behavior
Any content/platform/source/archive difference still blocks deployment and load. Out-of-bounds remote Size blocks proof. Failed scopes retain original receipts and require independently verified recovery. Fresh preparation cannot adopt old ownership intent or reset allowances.

## Validation evidence
Protected read-only recovery: tmp/adr0153-parents-7d0948b2f176/recovery-verification.json and image-difference-summary.json. All six immutable images differed only in Docker Size. Original containers/starts, retained resources, queue drain, generator idle and global zero-double-booking checks passed. At decision time, implementation and regression qualification were pending; executed results are recorded below. No cloud customer load started.

Executed qualification: [staging recovery and portable image identity](../capacity/flash-sale-opening/background-service-separation-staging-recovery-2026-10-07.json). The final affected suite passed 587 tests in 307.29 seconds; a separately added changed-runtime recovery test passed in 4.06 seconds. Recovery closure/rejection tests passed 37 cases. Independent cloud read-only recovery passed and closed the old scope as FAILED_RESTORED, preserving its original failure and elapsed accounting. A fresh schema-2 package passed local validation; no customer load or capacity improvement is claimed.
