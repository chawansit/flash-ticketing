# ADR0336: Remove the extra thread handoff for local JWT verification

## Status

Accepted for one isolated implementation and local verification. No deployment, paid traffic or capacity improvement is qualified by this decision.

## Context

ADR0335 failed at 160 offered journeys/s with 800 active journeys. API CPU averaged 3.6476 of four allocated cores, and 358,646 status requests were sent for 39,152 dispatched journeys. Retained terminal counters show approximately 56% cache hits. Cache hits already bypass PostgreSQL; changing that ordering is unnecessary. Most recorded acquisition rejections were shared-budget rejections, but incomplete failure history does not establish the underlying cloud CPU or database-delay cause.

The real API declares JWT authentication as a synchronous dependency, followed by a synchronous order-status handler. FastAPI therefore performs an additional worker-thread handoff for authentication. Verification uses local HS256 computation and claim validation only; it performs no network, database, Redis or filesystem access.

Exploratory offline ASGI replays identified the authentication handoff as a candidate. Review found unequal FastAPI dependency-override setup paths, so their initial 5.56% Windows and 16.76% Linux differences are not accepted as fair benefit measurements. A corrected equal-path Linux replay is recorded below. Removing all pool-state metric work saved only 1.66% in a separate paired Windows replay, so that distraction is deferred. None of these replays attributes cloud CPU or forecasts paid throughput.

## Decision

Declare only the existing `actor` dependency asynchronous. Preserve its verification body, HS256 allowlist, key, required expiry/subject, audience, issuer, subject bounds and error mapping. Do not move Redis, SQL, payment or other blocking work onto the event loop. Keep status handlers in their existing worker execution path.

Use the current tested source as the control. Preserve application settings, cache freshness, polling/recovery, resources and all connection budgets. Before cloud use, verify the candidate image differs only in this reviewed module and execute authentication and status regressions. A fresh paid-stage allowance is required for another comparison; the ADR0335 allowance is consumed.

## Alternatives

Keep synchronous verification: the rollback and valid control. Increase thread limits or API CPU: changes resources before isolating avoidable dispatch work. Cache JWT verification results: introduces credential-lifetime and invalidation behavior. Lengthen status polling or cache TTL: changes the customer workload or freshness. Optimize pool metric sampling: the measured local ceiling was too small to prioritize.

## Consequences

One worker dispatch is removed from each authenticated request. HS256 verification now occupies the event-loop thread briefly; this is not a general conversion to async I/O. Invalid and large tokens still require validation work. Local authentication-cost and event-loop responsiveness checks must accompany correctness checks. A modest local saving may be neutral under real I/O or CCE scheduling and does not establish that 160/s will pass.

No persistence, locking, messaging, idempotency, TTL, financial transaction or scaling policy changes. ADR0021's diagnostic-only restriction is superseded only for this explicitly reviewed authentication scheduling candidate; historical diagnostic evidence remains unchanged.

## Failure and recovery behavior

Authentication failures retain the same response. No authentication cache, new retry or database bypass is introduced. Roll back to the previous image if latency, event-loop responsiveness or customer gates regress. Preserve first-attempt errors, recoveries, final failures, generator drops and incomplete diagnostics separately. Zero double-booking/payment loss, financial reconciliation, queue drain and restoration remain mandatory.

## Validation evidence

### Runner integration decision

For the explicit ADR0336 comparison, deploy the new full shared-application image to the four native CCE APIs only. Keep the ECS bootstrap and the fifteen CCE workers on their tested ADR0324 digest. This is a staged rollout of the same application image, not separate per-service builds: all 21 non-API modules remain byte-identical. Avoid redeploying unchanged worker code or introducing a new worker topology decision. Retain all native API source/import checks against the candidate map, and worker checks against the original map; record both identities explicitly.

Select the overlay only with an exact source-bound ADR0336 goal. Compare against the failed-but-independently-reconciled ADR0335 800-slot result, never relabel it a passing control. Keep 160 offered journeys/s for 300 seconds, 800 active journeys, four generator processes, 500 HTTP connections, sixteen clients, API/worker resources, settings, recovery, freshness, 24 physical DB backends and 2+2 API pools unchanged. Require authenticated original evidence, a verified publication receipt and a new paid-stage allowance before dispatch. Preparation must neither register an allowance nor modify the standing goal. Partial selection, unpublished images and undeclared budget/source differences must fail closed.

Implemented the single scheduling-keyword change. The verification body has an identical AST after normalizing only the function's async declaration. Built a Linux image from the exact immutable ADR0324 parent: both installed and application source roots match all 22 expected module hashes, and the other 21 modules and dependency inputs are unchanged. Unrelated local persistence changes were excluded by constructing the candidate from the measured image, not the working source tree.

Executed 51 Windows checks (46 authentication/API/cache/admission plus five source-overlay rejection checks), and 90 checks inside the actual network-disabled Linux candidate (46 authentication/API/cache/admission plus 44 payment-partition/domain checks). Ruff F/I passed. Authentication progressed with the sole worker-thread token deliberately occupied. Existing authorization, cache freshness and payment partition checks passed; these tests do not constitute a fresh financial database integration run.

Three matched Linux offline ASGI pairs, each with 4,000 requests per arm, 50 concurrent requests and a simulated 56% cache-hit mix, averaged 6.3703 versus 5.5432 process CPU seconds: a 12.98% reduction in that replay. Pair order alternated, and both arms used the same dependency-override setup path. The earlier unequal-path 16.76% result is superseded and retained privately for traceability. Requests used real authentication, cache and pool-adapter logic with simulated Redis/PostgreSQL I/O, one CPU and 512 MiB. No production throughput extrapolation is justified.

Additional local verification timings: normal token p95 0.056 ms; a 22,035-byte token p95 0.261 ms; an unusually large 1,398,291-byte token p95 9.227 ms. No token-size policy changed. Adversarial traffic and live ingress header limits are not qualified, so this scheduling choice is not production-qualified.

Retained native observer counter deltas, including safety observations before the paid window, record 200,425 cache hits, 157,910 misses and 311 stale reads; 907 general/global rejections, 219 payment/global and 38 payment/role rejections, with zero native timeout/limit counters. These full aggregate counters identify rejection categories; partial failure history still cannot establish each holder or the exact cloud CPU cause.

See [local image and measurement evidence](../capacity/cce/authentication-dispatch-local-2026-10-17.json). Docker Desktop was started for local verification; all temporary test containers exited. Best qualified clean short throughput remains 138.49 paid-and-issued tickets/s.

### Executed local runner qualification

Implemented the explicit API-only image overlay in the existing runner. Worker topology, image, bootstrap, resources and connection policies remain unchanged. The overlay authenticates the exact failed ADR0335 result and independent financial/runtime recovery before comparison; both rate and headroom admission paths require the same reserved API-image contract. Partial selection, changed resources, changed worker/image proof, unrelated source drift and incomplete publication fail closed. Historical unselected profiles retain their image selection.

Executed 240 focused tests covering the new selector/publication/binding paths and existing generator, fulfillment, headroom, rate, API adapter, paid stage and creator-clock behavior. Ruff F/I and historical/current reproduction passed. Two existing tests depended on the live machine's spent authorization; they now declare their own isolated goal fixtures. No production guard was relaxed to make those tests pass.

Full generated API manifests, source/configuration binding and reservation admission passed using isolated envelope/journal/state copies and **simulated publication only**. File hashes confirmed that the live goal, journal and reservation accounting were unchanged. The real publication gate separately rejected the actual unpublished image. This is not actual registry or cloud admission verification.

The SWR upload attempt failed authentication; a fresh login was requested. The public image receipt explicitly remains pending and unpublished. No CCE deployment, new live allowance, paid traffic or capacity improvement occurred. After publication and independent pull verification, regenerate the source-bound prepared goal and run unmocked local admission before requesting one new 160/s, 300-second paid comparison under the stated 3,600-second experiment bound.

[Runner qualification and remaining blockers](../capacity/cce/authentication-dispatch-runner-local-2026-10-17.json).

### Verified registry publication

The supplied login succeeded after refreshing Docker authentication. Published and pulled the immutable candidate manifest `sha256:cde635d647a83758065d6e7bb6d36efaed79aa7baeb0dab91368dcd047aecde3`; verified the registry manifest bytes, configuration digest, Linux/amd64 platform and source label. Executed verification inside the pulled image: all 22 copied/installed/imported modules and dependency inputs match, eight role settings validate and the recovery endpoint remains present. The earlier authentication rejection alone did not prove the supplied password had expired.

With the real publication receipt, full generated API manifests, source/configuration binding and reservation admission passed without mocking image publication. Approval and reservation accounting were still simulated only in isolated local files; live goal/journal/state hashes remained unchanged during qualification. Regenerated the prepared, unauthorized goal from the final publication and runner source fingerprints. Thirty-one fresh profile checks and historical/current reproduction passed.

See [release evidence](../capacity/cce/authentication-dispatch-release-2026-10-17.json). No CCE deployment or paid traffic started. The previous single paid-stage allowance is consumed. The candidate is ready to request one fresh 160 offered journeys/s, 300-second comparison, including mandatory safety, post-TTL financial audit, drain and exact restoration within 3,600 seconds. The actual cloud performance benefit remains unmeasured.

### Fresh bounded comparison authorization

The user replied Continue to the concrete one-run approval request after publication and unmocked local admission. Registered one fresh allowance at journal index149:160offered journeys/s for300seconds, unchanged800 active journeys/500 HTTP connections/16 clients, four API/fifteen worker pods and24 physical DB backends with2+2 API pools. Mandatory safety, post-TTL financial audit, drain and exact restoration remain inside3600seconds. Prior ADR0335 allowance and failed evidence remain consumed/preserved. This authorization is not a test result. No further stage or load increase is authorized by it.

### Executed cloud comparison: not adopted

Executed one 160 offered journeys/s, 300-second comparison on 17 October 2026, 09:31:44–09:36:44 Bangkok. Four CCE APIs verified the candidate digest; fifteen unchanged workers verified ADR0324. Resources, 800 active journeys, four generator processes, 500 connections, sixteen clients and 24 physical DB backends stayed fixed.

48,000 journeys were scheduled, 37,542 dispatched and all 37,542 customer-confirmed. All 349 initially failing journeys recovered in 381 retries; zero final customer failures. However, 10,458 generator drops (21.79%) failed arrival quality. The database recorded 36,798 unique paid-and-issued tickets within the five-minute window, or 36,532 with the conservative boundary guard: 121.77/s. Worst-shard total journey p95 was 7,284.96 ms. API process CPU averaged 3.253 of four allocated cores; this does not measure node CPU or throttling.

Compared with the same-budget ADR0335 failed reference, conservative throughput fell from 127.15/s and p95 increased from 6,726.12 ms, despite lower API CPU and fewer initial errors. One sample does not prove the scheduling change caused the regression, but it does not demonstrate an improvement. Do not adopt the candidate as a capacity fix. Best qualified clean short capacity remains 138.49/s; no hourly or maximum capacity claim is added.

Independent read-only post-TTL reconciliation verified all 37,542 actual payments/tickets, both safety cohorts, zero double-booking, valid relationships and empty queues. Original services are restored and stable; temporary namespace/helpers are absent and the generator is idle. The original 48,000-completeness audit remains failed, not relabeled as passing. Only operational recovery was cleared; ledger is FAILED_RESTORED, one paid allowance consumed.

Independent database wait sampling and query snapshots were complete, but slot failure history remains incomplete. Pending payment attempts peaked at 638 while paid-but-unissued orders peaked at 32; these snapshots suggest focusing on payment progress and polling pressure, but do not establish the exact holder or root cause. Inspect existing evidence before selecting another isolated correction; do not increase offered load.

[Cloud result](../capacity/cce/authentication-dispatch-result-2026-10-17.json) and [independent reconciliation](../capacity/cce/authentication-dispatch-recovery-2026-10-17.json).
