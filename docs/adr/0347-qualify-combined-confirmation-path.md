# ADR0347: Qualify the combined confirmation path

## Status

Accepted for preparation and one fresh160/s control followed by one conditional160/s candidate, each300seconds, within unchanged resources and shared3600second pair deadline. User authorized required cloud spending on2026-10-17. Supersedes ADR0345 runner candidate selection; retains its historical receipt/image and all safety gates. Extends locally qualified ADR0346, including ADR0344 worker correction.

## Context

The user prioritizes verified300/s. Local callback and simulator corrections reduce distinct confirmation costs, but neither establishes cloud capacity. The last180/s control failed and is restored.160/s is the last clean offered load and serves as the initial regression comparison. Repeating a failed180/s control would prevent any measurement of these corrections.

## Decision

Reuse the existing source-bound matched pair runner and its established restoration/audits. Replace its active candidate selection with the exact combined ADR0346 local image, publish to a fresh existing-repository tag, verify immutable manifest and pulled22-module identity, and enable PAYMENT_CALLBACK_WRITE_PIPELINE only in candidate API pods. Control stays immutable ADR0324 and uses no callback flag. Preserve common ECS bootstrap/restoration on ADR0324. Require actual preparation, role settings validation and guard checks before live admission. Update receipt/source contracts explicitly; reject old ADR0345 authorization hashes. Control must fully pass and restore before candidate. No automatic retry or rate escalation is included; select the next rate from measured limiting evidence.

## Alternatives

Test only ADR0344: misses locally demonstrated callback cost. Jump directly to300/s: far above current passing evidence and unlikely to isolate useful limiting behavior. Repeat180/s control: two failures already show this is an unsuitable regression gate. Increase resources or weaken correctness/freshness: excluded. Copy the whole checkout: would reintroduce unrelated rejected authentication/payment-initiation changes.

## Persistence, locking, messaging, idempotency, TTL and scaling

No new store, lock, message, idempotency key or TTL. ADR0346 locks/fresh clock/durable callback commit and transactional outbox remain authoritative.16delivery slots,10simulator clients,24physical backends,4API pods and15worker pods are unchanged. Fixed polling/cache age and recovery remain. A160/s comparison is a regression gate toward300/s, not300/s qualification.

## Failure and recovery

Missing publication, source/configuration drift, failed or unrelated control, expired deadline or consumed arm reject before paid dispatch. Existing financial, safety, callback/TTL drain and exact restoration checks remain mandatory. Stop after failed control or candidate; retain failed quality separately from operational recovery. No automatic paid rerun, resize, relaxed gates or main merge.

## Validation evidence

ADR0346 already passed29 local checks and22-module actual build verification. Combined runner:235 guard/compatibility checks passed, with56 direct guards rerun after normalization changes (overlapping cases); Ruff passed. Actual existing plan/manifests/bindings passed and final summary revision was prepared again; image/source and candidate-only flag are the only measured-arm differences. Network-disabled candidate validation passed8 role configurations and22 modules. Historical reproduction passed. Detailed owned logs: tmp/adr0347-focused-qualified.log, tmp/adr0347-direct-qualified.log, tmp/adr0347-preview-reviewable.log, tmp/adr0347-reproduction.log. [Compact local receipt](../capacity/cce/combined-confirmation-runner-local-2026-10-17.json) retains limits.

Automatic approval review rejected execution of the registry publication helper because general cloud-spending approval did not specifically authorize uploading the application image to the external SWR destination. Exact image/repository approval was requested; no workaround attempted. Registry remains UNPUBLISHED, no live allowance opened, no deployment/load or new cloud financial reconciliation executed. Cloud validation pending; no cloud gain claimed.
