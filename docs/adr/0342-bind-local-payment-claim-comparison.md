# ADR0342: Bind the local payment claim comparison through existing builders

## Status
Accepted for local preparation and the explicitly approved candidate publication. Extends ADR0341; supersedes no accepted decision. Live runner registration and paid authorization remain incomplete.

## Context
The reproducible ADR0341 candidate has the same 22-module source map as the tested local correction. The existing bounded runner already generates API and background role manifests and enforces financial, drain, restoration and spending checks. Its defaults select the retained production experiment; using those implicit defaults for this new proposal could silently select the old image or the failed 180/s workload.

## Decision
Add an explicit local proposal entry point to the existing bounded runner, reusing its API and worker manifest builders. Allow those builders to receive explicit goal and image inputs for preparation while retaining their existing defaults. Generate both arms from one unchanged configuration, binding each role to the measured control or exact locally qualified candidate image and complete runtime map. Local generation is not deployment admission: reject local-only proposals for dispatch. Do not alter the envelope, journal, allowance, existing cloud selection or correctness gates.

The proposal is control then candidate at 160 offered journeys/s for 300 seconds each, within one proposed 3,600-second experiment; stop after failed control. Keep four 1-vCPU/2Gi API pods with 3+3 clients, fifteen 250m/512Mi background pods, 24 physical backends, shared admission20, simulator16/10, four generator processes/800 active journeys/500 connections/16 clients, gateway delays, recovery and freshness unchanged. No stage is reserved or approved here.

Persistence/locking, callback messaging, idempotency, 15-second payment leases and cache TTLs retain ADR0341/ADR0338 behavior. Scaling and resource limits remain fixed. Manifests can contain private connection material in memory; retain only hashes, allowlisted settings and counters in public evidence.

## Alternatives
Another runner: duplicates spending and cleanup logic. Implicit current goal/image: risks selecting a historical workload or arm. Pretend a local image ID is a verified registry manifest: rejected. Publish before verifying permission and identity: rejected.

## Consequences
Preparation becomes independently testable through the actual builders. Local receipts cannot satisfy paid admission. Exact registry publication/pull verification and live profile registration remain required before a new explicitly approved comparison can run.

## Failure and recovery behavior
Reject source/image drift, unknown arms, changed budgets or generated role/settings drift. Never use a failed control as a candidate prerequisite. Existing cloud runner admission remains unchanged; a local proposal fails closed. Preparation has no database writes, messaging or service mutation, so recovery is removal of owned local artifacts. Preserve the passing baseline and failed load classification.

## Validation evidence
Executed: 19 focused local binding checks passed after the final binding edit. They construct the actual four API and fifteen worker manifests, verify the sole workers source delta, unchanged per-arm configuration, exact resources/pools/500ms shared admission, reject manifest/source/budget drift, verify sanitized output and reject local-only dispatch admission. The 109 existing API adapter, fulfillment and client-budget compatibility cases passed in the earlier combined invocation; that invocation also reported 14 new-fixture setup errors before canonical run identity/runtime-view projection were corrected. The final focused binding invocation passed all 19 cases. Ruff passed for all five changed Python files.

The actual existing-runner preparation entry point generated both arms from a synthetic configuration fixture. A network-disabled run of the exact locally qualified candidate image validated all eight generated API/worker environment types through its own Settings.validate(); zero services started. No image rebuild was needed because runtime source bytes were unchanged. [Sanitized local binding receipt](../capacity/cce/payment-claim-binding-local-2026-10-17.json) binds preparation-source hashes, both runtime maps/images and private manifest hashes. It retains no Secret objects or credential values. This proves local construction and configuration validation, not reconciliation against an actual live service snapshot.

Registry credentials were available privately. A read-only authentication check verified repository-specific pull and push permission using the registry's namespace/repo/actions claims. No credentials or token values were retained publicly. Automatic approval review initially rejected publication before execution; the user then explicitly approved the exact image transfer to `swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing:adr0341-046b8b0fe4ea`. The approved step verified the local image identity/platform/user/source label and its 22-module source map, confirmed the target tag was absent, published, verified the registry manifest byte digest, pulled the immutable reference and repeated the network-disabled 22-module copied-source/imported-worker verification. The pulled image ID exactly matched the approved local image.

[Registry verification receipt](../capacity/cce/payment-claim-cutoff-registry-2026-10-17.json) retains manifest `sha256:730bff63781ccffa1aa8ab081e5ab73621d01ea86269d4e6aa74846857e57ff4`, its separate configuration digest, runtime map and verification scope. This is registry-qualified, not cloud-measured. Live candidate registration and actual snapshot reconciliation remain incomplete. The local proposal still fails dispatch admission; no allowance, journal or live selection was changed. No cloud service was deployed and no paid traffic started.

## Concrete next proposal
With immutable publication/pull verification complete, next reconcile the actual live snapshot and register the candidate in the existing guard. Then request a separate fresh allowance for control then candidate, each 160/s for 300 seconds (48,000 scheduled journeys per arm), within a 60-minute experiment. Stop after failed control. Keep the qualified ADR0338 budgets and all existing customer latency, zero-drop, financial, post-TTL, drain and exact restoration gates. Compare conservative paid-issued throughput, pickup latency, cache misses per dispatched journey and API CPU/acquisition latency. Nothing in this proposal authorizes paid traffic or deployment. Best passing capacity remains 158.0633 tickets/s; ADR0339 remains failed/restored.
