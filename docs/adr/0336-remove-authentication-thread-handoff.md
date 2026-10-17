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

Implemented the single scheduling-keyword change. The verification body has an identical AST after normalizing only the function's async declaration. Built a Linux image from the exact immutable ADR0324 parent: both installed and application source roots match all 22 expected module hashes, and the other 21 modules and dependency inputs are unchanged. Unrelated local persistence changes were excluded by constructing the candidate from the measured image, not the working source tree.

Executed 51 Windows checks (46 authentication/API/cache/admission plus five source-overlay rejection checks), and 90 checks inside the actual network-disabled Linux candidate (46 authentication/API/cache/admission plus 44 payment-partition/domain checks). Ruff F/I passed. Authentication progressed with the sole worker-thread token deliberately occupied. Existing authorization, cache freshness and payment partition checks passed; these tests do not constitute a fresh financial database integration run.

Three matched Linux offline ASGI pairs, each with 4,000 requests per arm, 50 concurrent requests and a simulated 56% cache-hit mix, averaged 6.3703 versus 5.5432 process CPU seconds: a 12.98% reduction in that replay. Pair order alternated, and both arms used the same dependency-override setup path. The earlier unequal-path 16.76% result is superseded and retained privately for traceability. Requests used real authentication, cache and pool-adapter logic with simulated Redis/PostgreSQL I/O, one CPU and 512 MiB. No production throughput extrapolation is justified.

Additional local verification timings: normal token p95 0.056 ms; a 22,035-byte token p95 0.261 ms; an unusually large 1,398,291-byte token p95 9.227 ms. No token-size policy changed. Adversarial traffic and live ingress header limits are not qualified, so this scheduling choice is not production-qualified.

Retained native observer counter deltas, including safety observations before the paid window, record 200,425 cache hits, 157,910 misses and 311 stale reads; 907 general/global rejections, 219 payment/global and 38 payment/role rejections, with zero native timeout/limit counters. These full aggregate counters identify rejection categories; partial failure history still cannot establish each holder or the exact cloud CPU cause.

See [local image and measurement evidence](../capacity/cce/authentication-dispatch-local-2026-10-17.json). Docker Desktop was started for local verification; all temporary test containers exited. No registry publication, CCE deployment, cloud-runner admission or paid traffic occurred. Next, bind the reviewed image to a fresh single-factor runner scope before requesting a new paid-stage allowance. Best qualified clean short throughput remains 138.49 paid-and-issued tickets/s.
