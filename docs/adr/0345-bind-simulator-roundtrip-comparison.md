# ADR0345: Bind the simulator round-trip comparison

## Status

Implemented and locally qualified; live dispatch remains blocked by unpublished image proof and missing fresh paid authorization. Extends ADR0344 with a distinct guarded runner identity. Proposes160/s for300seconds per arm, control first and candidate only after a fully passed/restored fresh control. No image publication, paid authorization, cloud deployment or new load is granted by this decision. The consumed ADR0343 sequence is not resumed.

## Context

ADR0344 reduced local claim/ack operation time22.18% and passed53 checks; its22-module image is local and unpublished. Its source differs from the earlier published ADR0341 image. Existing API/worker builders accept an explicit source/image receipt, but normal runner selectors, worker receipt validation and dispatch still select older identities. A local-only receipt must never authorize a cloud deployment or be called registry-qualified.

## Decision

Add one explicit simulator-roundtrip discriminator to the existing160/s fixed-budget runner. Both arms retain four1-vCPU/2Gi APIs, fifteen250m/512Mi workers,3+3 API clients,16simulator delivery slots/10clients,24physical database backends, existing generator/recovery/latency/freshness gates and financial audits. Bind all current orchestration sources and exact application image/source maps. Control uses immutable ADR0324; candidate uses only the reviewed ADR0344 simulate_one change. Keep the common ECS bootstrap and exact original restoration model on ADR0324; change images only in measured native pods. No new general runner or alternate financial audit is introduced.

Provide explicit local preview preparation through the actual existing plan, API/worker manifest and binding builders. Preview records are marked local-only and cannot reserve, execute or dispatch. A stable publication-status file initially records unpublished; live image selection requires complete actual immutable registry manifest/configuration/source verification. Never invent a registry digest from a local tag or claim publication mocks are real proof. Publication changes the bound receipt/source contract and requires refreshed preparation.

Bind a distinct pair ID, journal boundary and aware start timestamp to future live arms, with one paid stage per arm and one shared3600-second deadline. Candidate admission requires the exact fully passed/restored fresh control raw report, matching ledger/state/hash, same pair identity, unchanged source/configuration/settings and complete financial/drain/cleanup evidence. Reject historical control substitution, changed common factors, failed control, duplicate paid arms, missing publication, stale source and elapsed deadline. Recheck the deadline through existing paid-stage checks; recovery and cleanup remain mandatory even after it expires.

## Alternatives

Reuse the ADR0341 publication or its parked unqualified integration: rejected for source mismatch. Use only builder preview as live registration: rejected. Loosen parent source guards globally: rejected; allow only this declared integration and verify all other retained bindings. Use a failed180/s control to admit the candidate: rejected. Increase rate, resources, TTL or retry budget: outside scope.

## Persistence, locking, messaging, idempotency, TTL and scaling

Financial persistence, locking, token-fenced acknowledgement, callback identities and outbox/inbox remain ADR0344. Cache age1s, polling500ms and leases15s remain unchanged. Existing journal reservation, run lock and consumed-stage bookkeeping remain authoritative; preview adds no real reservation and no durable work. Both arms reuse existing owned namespace/job lifecycle, audits and exact restoration. Scaling remains fixed; no new service, backend, thread or message path is introduced.

## Consequences

Application source and comparison-arm identities become independently checkable. Local qualification can demonstrate complete preparation and fail-closed admission without consuming a paid allowance. It cannot demonstrate registry publication, live snapshot reconciliation or cloud improvement. Future publication and fresh human paid authorization remain distinct prerequisites.

## Failure and recovery behavior

Fail before reservation/deployment on preview markers, absent publication, swapped image/source, changed budgets, unknown pair/arm or stale proofs. Candidate never follows failed control. Preserve original failures and paid counters on any observer/transport error; reuse existing post-TTL financial audits, drain and restoration. A shared deadline never skips cleanup. No automatic retry or main merge.

## Validation evidence

Inspected the handoff, ADR0344 local receipt, current transaction/image selectors, native worker identity/lifecycle, binding construction, rate/reference validators and isolated prior admission recipe. Implementation and executed local qualification are pending. Append only checks actually executed.

## Executed local qualification

The actual preparation entry point generated both160/s300s plans, four API/fifteen worker pods per arm and bindings from retained real configuration, service model and dependency proofs. Normalizing only image/source identity made both complete manifest sets identical. Common ECS bootstrap and restoration remain on ADR0324. No registry publication was mocked in this actual preparation; preview explicitly selects the real local image and cannot reserve, execute or dispatch. The real envelope/journal/state file hashes were unchanged.

The focused compatibility suite passed222 checks. The final direct profile suite passed54 checks,43 overlapping the broad suite, for233 unique checks. These include actual unpublished-receipt rejection, preview rejection at reservation/execute/stage checks, source/budget drift, shared deadline, consumed-arm rejection, fresh same-pair control checks and direct failed-load/financial/TTL/drain/restoration/measurement rejection. Synthetic publication fixtures test only schema/selector behavior and grant no real registry proof. Ruff passed. The existing reproduction verifier passed535 historical files and31 current overlays.

The exact local candidate verified all22 source modules and all8 generated role settings with networking disabled. The initial reproduction manifest refresh had the wrong overlay value structure; compatibility tests caught it and the historical blob/identity structure was restored before final qualification. Unit collection inherited the consumed host-specific goal; the broad run used an isolated inactive authorization file and the new direct suite now isolates authorization per test. No production authorization guard was relaxed.

Resource/settings equivalence is checked against the clean160/s reference. Source deltas are checked separately against the latest reconciled ADR0343 runner, allowing only this declared integration and its five new inputs; that failed load is never used as a passing-control permit. A future candidate still requires a fresh fully passed/restored same-pair control.

[Sanitized qualified preparation](../capacity/cce/simulator-roundtrip-runner-local-2026-10-17.json). Local application source remains ADR0344, with no additional application change. The publication-status file remains UNPUBLISHED, no paid allowance was opened, no cloud call or live database reconciliation occurred, and the shared pair clock is unstarted. Publication/pull verification, refreshed source/goal hashes, human paid approval and live snapshot/readiness qualification remain mandatory before the next control. The conditional candidate follows only after that control passes/restores within the shared3600-second ceiling. No higher-rate step is authorized.
