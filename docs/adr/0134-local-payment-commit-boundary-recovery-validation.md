# ADR 0134: Local payment recovery validation at commit and acknowledgement boundaries

- Status: Accepted for local validation; cloud fault injection and automatic paid-client retries remain outside this decision
- Date: 2026-10-04

## Context

ADR0133 completed 18,000 paid and issued tickets at unchanged 60 buyers/s for 300 seconds, with all 20 gates passing. It did not inject failures. Existing tests prove pre-commit rollback and callback lease recovery after an HTTP rejection, but do not jointly prove recovery when an API response disappears after a financial commit, delivery succeeds before acknowledgement fails, or an old dispatch returns after another worker owns its lease.

The strict paid generator stops on its first error and reports zero retries. ADR0049's read/hold recovery diagnostic cannot qualify paid-client recovery. Step 7 needs evidence at ambiguous commit boundaries while preserving the original errors and no-retry capacity baseline. Opening validation also remains separate: the current two-process generator partitions distinct shows; the smoke runner fixes 300 seats/show; the fixture preparer limits seats/show to 1,000. These controls cannot represent one concert with 18,000 distinct sales without tooling changes.

## Decision

Add a local integration matrix using fresh PostgreSQL schemas, loopback Redis and real API routes with JWT/HMAC checks. Execute it against both the restored normal unpartitioned pool and ADR0133's qualified isolated general2/payment2, shared12/purpose10 configuration.

Use a test-owned HTTP/1.1 loopback proxy to forward requests to the actual ASGI application and deliberately close a socket after the application has committed, before returning its response. Record the first response internally and require the client/worker to observe the original transport failure. Recreate API pool ownership for explicit same-key recovery; do not introduce an automatic retry helper.

Validate lost payment responses, callback response loss, failure before dispatch acknowledgement commit, stale dispatch-token fencing, fulfillment commit followed by lost acknowledgement, and replay after a hold expires. Each scenario allows only its explicitly enumerated recovery calls, retains the failure assertion, checks unchanged hold deadlines and verifies exact payment/callback/booking/ticket/outbox effects. Tests may move their own lease/hold deadlines backward to avoid real-time sleeps; they do not measure live failover recovery time or alter production TTLs.

Production code, generator behavior, resource budgets and cloud services remain unchanged. Follow ADR0040's bounded lifecycle and ADR0090's isolation principles for local orchestration. Prepare the requirements and failed preflight evidence for a future single-concert comparison, without dispatching it.

## Persistence, locking, messaging, idempotency, TTL and scaling

PostgreSQL remains authoritative. Keep ADR0004 durable actor/operation/key/request-hash/response records; recovery uses the same actor, key and request. Changed payloads and unauthorized actors remain errors. Keep ADR0129 order/payment discovery and the existing order/payment/hold/sorted-seat lock order. Financial commits, inbox/outbox deduplication, booking/ticket uniqueness and lease-token predicates are exercised unchanged.

Callback delivery retains its existing 15-second lease and five-second network timeout. A failed delivery has no inline resend; only a later eligible claim may deliver again with the same callback ID. Lease-expiry advancement is test-only. Hold, cache, command and signature TTLs remain unchanged. Real-provider capture/refund reconciliation is not implemented by these tests.

No new connection scheduler, replica, pool budget, admission queue or scaling choice. Normal shared and isolated shared-budget modes are controls, not simultaneous cloud candidates. This decision supplements ADR0004, ADR0121 and ADR0133 validation; it supersedes no accepted production decision.

## Alternatives and consequences

Pure mocked successful responses cannot establish a financial commit before a lost socket response. Re-running the healthy cloud load would not exercise recovery. Adding paid-generator retries would change physical load and hide the strict baseline unless separately designed and reported. Cloud process kills or datastore failover introduce broader side effects and are deferred until a concrete bounded drill exists.

Loopback HTTP exercises actual transport failure plus signed API behavior and real SQL durability, but ASGI runs in process and the database has no network fault. Recreated pool ownership represents loss of process-local state, not an operating-system restart. The evidence qualifies only the enumerated local recovery contracts.

## Failure and recovery behavior

Every injected error must be observed; a recovered final result cannot erase it. Local runner timeouts fail the validation and remove only containers it created. Tests restore application settings/state, close proxy/client/transport/pools, unblock worker threads in finally and drop their isolated schemas.

An exact-count, authorization, TTL, lease-fencing or concurrency failure stops qualification. Diagnose and retain evidence before changing production behavior; any architectural fix requires an ADR amendment or a new ADR before implementation. No cloud fault injection, increased load, push or main merge is authorized by this validation.

## Validation evidence

At decision time this matrix has not been implemented or executed. The baseline remains cloud application source deb330e with normal budgets and both acquisition feature flags disabled; temporary access is absent.

- [Passing ADR0133 comparison](../capacity/flash-sale-opening/shared-acquisition-budget-control-2026-10-04.json)
- [ADR0004 payment idempotency](0004-payment-idempotency.md)
- [ADR0121 callback connection reuse](0121-bounded-payment-callback-connection-reuse.md)
- [ADR0049 recovery diagnostic limits](0049-bounded-idempotent-retry-diagnostic.md)
- [ADR0040 orchestration](0040-unattended-distributed-capacity-stages.md)
- [ADR0090 generator isolation](0090-isolate-paid-generator-with-loopback-responder.md)
