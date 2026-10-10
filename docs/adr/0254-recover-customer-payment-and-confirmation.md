# ADR0254: Recover customer payment and confirmation failures

## Status
Accepted for bounded recovery implementation. Locally verified and exercised in a restored five-minute CCE comparison; hourly and production qualification remain pending.

## Context
ADR0252 reached the numerical hourly ticket target but had 67 payment 503 failures, 37 status 503 failures and 372 undispatched journeys. The paid client abandoned transient responses. A missing response can follow a committed payment initiation; a new identity must not create another payment. Existing durable idempotency and callback replay remain the financial authority.

## Decision
Provide an authenticated, actor-and-order-and-idempotency-key scoped payment-operation read from PostgreSQL, never the advisory order cache. Return the existing operation and its state, or NOT_STARTED with the current hold expiry and database clock. NOT_STARTED is an observation, not proof a concurrent initiation cannot commit: replay is always the identical request with the original key. Check this operation after ambiguous transport errors and temporary payment rejection before replaying. If it exists, poll the existing order instead of posting payment again. Bound recovery to three HTTP attempts per failed request and the original journey deadline; use exponential jittered backoff (100 ms base, 1000 ms cap). Honor bounded Retry-After without shortening it. Do not retry authorization, payload, seat or expiry conflicts. If the operation lookup is unavailable, do not replay payment blindly. Pending confirmation remains processing and retries only the authorized read. No frontend is introduced.

Enable recovery explicitly in the existing paid client; leave historical no-retry profiles unchanged. Report first-attempt failures, physical retries, operation checks, recovered journeys, final failures and end-to-end time including recovery separately. Generator drops remain undispatched and visible. Database bottleneck diagnosis/correction is a separate decision; recovery does not establish capacity improvement or waive existing quality gates.

## Supersession
Extends existing payment idempotency/replay contracts. Does not supersede seat TTL, atomic ownership, callback authenticity, outbox, Kafka or scaling guarantees. Historical results remain unchanged.

## Alternatives
Blind new-key payment retries risk duplicate operations. Abandoning every transient failure loses recoverable journeys. Unbounded retries or a longer hold hide overload. Cached payment decisions can be stale. These alternatives are rejected.

## Consequences
Recovery adds bounded read/HTTP work under failure; measure this amplification. A safe unresolved journey remains processing/failed at its deadline and can later retrieve its existing operation. Production gateway integration remains separate from the development simulator. Exactly-once external charging requires provider idempotency; this MVP proves one simulated operation and durable booking/ticket effects, not a live-bank guarantee.

## Failure and recovery behavior
Use existing unique constraints and request hashes for concurrent replay. Never extend holds or reinterpret uncertain payment as failure. Existing callbacks and ticket events recover through durable leases/idempotency after process restart. Expired holds prevent new attempts, but an existing paid operation is still inspected. Ownership denial reveals no payment details. Exhausted retries do not erase the first error or generator drops.

## Validation evidence
Executed local validation: 139 selected correctness tests passed across PostgreSQL/Redis reservations, atomic ownership, replay, payment commit recovery, pool partitions, simulator concurrency and recovery behavior. The new lookup tests verify ownership, read-only access, expiry and retrieval of paid tickets after expiry. Existing lost-ack callback/event tests and duplicate callbacks retain one payment/booking/ticket effect. API lifespan/pool reopening is tested. A subsequent 22-case recovery/pipeline integration run also passed, including two actual uvicorn process termination/restart cases (pipeline off/on): the loopback proxy loses an already-committed payment response, a fresh OS process retrieves the original operation, and identical-key replay retains one payment/booking/ticket effect. Cloud service/failover qualification remains separate.

The complete unit suite passed 2,098 tests with two skipped. Final 67 recovery/runner tests passed separately after accounting changes, including loading the generator from only its transferred files with PYTHONPATH removed. A real mock HTTP transport verifies identical-key replay, first-attempt errors, physical retries and final outcome accounting. These suites overlap and their counts must not be added as distinct tests.

The recovery overlay uses the proven 78-file parent and seals three changed customer modules plus the standalone recovery client and the existing fixture-layout dependency (80 files total). Both arms retain distributed 84-show/300-seat fixtures, the original synchronized schedule, two shards, 500 active journeys, eight clients per shard and unchanged poll interval. Single-concert mode remains outside this comparison.

The API image was built offline from the accepted immutable registry parent; all 22 source modules and unchanged dependency inputs were verified and imported. The recovery route is present in OpenAPI. SWR rejected the previous login with Authenticate Error, so registry publication/pull verification and the 84/s cloud comparison remain pending. No capacity improvement or production qualification is claimed.

## Matched cloud validation, 2026-10-10

Fresh SWR authentication succeeded; the published linux/amd64 image was pulled by manifest digest and all 22 runtime modules were verified. A pre-dispatch runner bug was corrected and tested: archived hourly reproduction checks use the historical plan independently of the active short scope. Hourly execution still rejects the recovery short profile. The publication guards passed 31 selected tests and the runner/reproduction/hourly guards passed 45 selected tests; these counts overlap earlier suites.

Both fresh five-minute CCE arms offered 84 journeys/s and completed all 25,200 scheduled journeys with zero generator drops and final customer failures. Control had no first-attempt errors/retries. Candidate had three payment 503 first-attempt errors (0.011905% of dispatched journeys), three authoritative payment-operation lookups and three identical-key recovery attempts; all three journeys recovered. Each arm retained 25,200 succeeded payments, bookings and tickets after TTL, zero double-booking, full queue drain and exact restoration. Both retained ledgers are PASSED_RESTORED. The temporary registry authentication file and owned CCE resources were removed.

This validates the bounded simulator recovery path under three real temporary rejections, not a live-bank charging guarantee. Five-minute offered cohorts include bounded completion tails; they do not qualify 300,000 unique tickets in one hour or maximum production capacity. See [sanitized paired evidence](../capacity/cce/customer-recovery-comparison-2026-10-10.json). The default-off status-read candidate was not adopted; see ADR0255.
