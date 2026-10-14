# Ticketing contracts for implementation in any language

These contracts define outcomes and transaction boundaries. Pseudocode describes the proposed customer port; it is not executable deployment code. Reference file links point to Python implementations for comparison.

## Shared identifiers and states

Scope every customer operation by authenticated tenant/customer, operation kind and idempotency key. The reference is actor-scoped; tenancy must be added if the customer platform is multi-tenant. Never trust an actor supplied in a request body.

Define request normalization once: validate and sort unique seat IDs, encode money as integer minor units, preserve currency and bind operation/order/show identifiers. Use a versioned deterministic encoding and hash. A different language must produce the same hash for golden request examples, or explicitly migrate the hash version. Never hash floating-point money or silently change normalization for existing keys.

Separate these state machines:

| Object | States and meaning |
| --- | --- |
| Provisional reservation command | PENDING, DURABLE, FAILED; PENDING is not checkout permission |
| Hold | ACTIVE, CONSUMED, RELEASED, EXPIRED |
| Order | PENDING, PAID, FULFILLED, FAILED, EXPIRED, REFUND_PENDING, REFUNDED |
| Payment operation | A durable identity with a known result or unresolved outcome; NOT_STARTED is an inquiry observation |
| Customer display | Processing until confirmation/issuance is known; never infer failure from a timeout |

Order PAID means financial and booking effects committed; FULFILLED means tickets committed. The real provider's payment states need an explicit mapping, including unknown outcomes and inquiry.

### Logical interfaces

```text
ReservationStore.reserve(actor, showId, sortedSeatIds, idempotencyKey)
ReservationStore.release(actor, holdId, ownerToken)
PaymentStore.initiate(actor, orderId, idempotencyKey, normalizedRequest)
PaymentStore.getOperation(actor, orderId, idempotencyKey)  // PRIMARY
PaymentStore.applyVerifiedCallback(providerIdentity, normalizedCallback)
OrderReader.getAuthorizedOrder(actor, orderId)
EventTransport.publishDurably(envelope, stableMessageId)
EventTransport.receiveBounded(consumerIdentity, limit, deadline)
EventTransport.ackCommitted(delivery)
StatusCache.lookup(actor, orderId, freshnessPolicy)
StatusCache.publishAfterCommit(snapshot, originalFreshnessDeadline)
```

The API facade can retain existing customer paths. Reference routes include POST /v1/holds, GET /v1/orders/{order_id}, GET /v1/orders/{order_id}/payment-operation with Idempotency-Key, and POST /v1/webhooks/payments. The reference POST payments route is development-only simulator input, not a real gateway contract.

## 1 Atomic seat holds and expiry

### PostgreSQL first pilot

```text
validate authenticated actor, show window and distinct sorted seats
BEGIN on PRIMARY
  claim idempotency record with unique scoped key
  if existing:
    reject changed request hash; otherwise return original committed result
  lock requested seat rows in canonical order
  read current sale and ownership conditions under the required locks
  verify ALL seats available or legitimately expired
  write one hold and one pending order, plus every selected seat owner
  record original expiresAt and durable result
  append any required inventory outbox event in this transaction
COMMIT and release connection
return durable hold/order identity
```

A conflict rolls back the whole selection. All seat writers, expiry jobs and callbacks must follow a compatible lock order. The reference locks existing entities as order, payment, hold, then sorted seats; new reservations lock only sorted seats and do not acquire a previous owner's order/hold locks.

Use server time for authoritative expiry checks. Retrying the original request returns its original expiry; it does not extend the hold. Release only if the current owner token still matches. Expiry cleanup must never release a newer owner's seat.

Enforce unique booked seat ownership in PostgreSQL, normally (show_id, seat_id), and one durable order per hold. Adapt cancellation/resale rules explicitly rather than removing the uniqueness protection.

### Optional Redis first intake

Atomically validate the sale window, all selected seats, request identity and bounded intake capacity; claim provisional owners and record the command in a Redis Stream. Use one script execution and compatible Redis Cluster hash-slot placement for every accessed key. Redis atomic execution does not provide rollback for a script that errors after writes: validate types/arguments before mutations and fail closed on corruption.

Return 202 with command_id, hold_id, order_id, original expires_at and persistence_status=PENDING. The writer commits the PostgreSQL command receipt, hold, order and seat ownership together, rechecking ownership and expiry. Only after commit may it mark DURABLE and acknowledge the stream entry. Payment is blocked while PENDING.

Replay after a writer crash uses the same command identity. A lost Redis ACK does not create a second order. Redis failover can lose provisional state despite AOF/replica acknowledgements; reconcile against PostgreSQL and never treat a provisional response as a durable booking. Never release another token's hold or refresh expiry on replay.

Reference: [Redis intake scripts](../../../src/ticketing/infrastructure/redis_reservations.py), [durable reservation adapter](../../../src/ticketing/infrastructure/reservations.py), [Redis-first contract](../../redis-first-reservations.md).

## 2 Payment idempotency and recovery

Payment identity must survive API, dispatcher and provider retries. Require database uniqueness for the operation scope and provider payment reference. The reference permits one payment operation per order; multiple business attempts need a separately defined state transition, not a new key after a timeout.

```text
BEGIN on PRIMARY
  claim or replay scoped idempotency record
  lock order; verify authenticated owner
  read payment context AFTER acquiring order lock
  return existing payment operation if already present
  otherwise verify durable hold and original expiry
  create durable operation and dispatch obligation with stable provider key
  persist response identity
COMMIT and release connection
call/dispatch gateway using the SAME provider operation key
persist or reconcile its response in a separate short transaction
```

The dispatch obligation and real provider adapter are customer work. Gateway calls and customer waiting must not retain database connections. A gateway timeout means unknown, not declined.

After an ambiguous timeout or temporary rejection:

```text
inquire existing operation on PRIMARY with same actor/order/key
if operation exists: follow it; do not create another payment
if NOT_STARTED and still payable:
  replay identical request/key within remaining recovery budget
if inquiry unavailable: show processing; do not blindly replay payment
if expired or deterministic conflict: stop initiation; inspect existing payments
```

NOT_STARTED does not prove a concurrent initiation cannot commit. Uniqueness and identical-key replay must close that race.

Reference recovery allows at most three HTTP attempts per failed request including the first, within the original journey deadline; exponential jitter uses 100 ms base and 1000 ms cap. Honor Retry-After without shortening it; if it cannot fit the deadline, stop automated attempts and retain the operation for later retrieval. Never retry authorization, changed payload, seat conflict or expiry errors. Customer polling is separately bounded.

Verify the real provider supports stable idempotency or authoritative inquiry. If neither exists, do not claim exactly-once charging; unresolved operations require a reconciliation/review process. Never substitute simulator behavior for settlement proof.

Reference: [ADR0254](../../adr/0254-recover-customer-payment-and-confirmation.md), [primary operation inquiry and initiation](../../../src/ticketing/infrastructure/reservations.py), [recovery client](../../../scripts/customer_recovery_client.py).

## 3 Payment record and outbox committed together

Verify callback authentication/signature using the provider contract, and check order identity, amount and currency. Scope callback IDs by provider/merchant where necessary. The same callback identity with changed payload must be rejected or quarantined; different callback IDs for the same success cannot repeat financial effects.

```text
BEGIN on PRIMARY
  lock order, payment, hold, sorted seats in established order
  deduplicate verified callback and validate request hash
  validate current ownership and original expiry
  persist payment outcome
  if success with valid ownership:
    create unique bookings; consume hold; set order PAID
    INSERT outbox OrderPaid with stable event ID
  if success after ownership/expiry invalidation:
    persist payment success AND refund/review obligation
    INSERT outbox RefundRequested
    never reclaim another customer's seat
  persist callback receipt and any inventory event
COMMIT and release connection
acknowledge callback; refresh cache best effort outside transaction
```

If commit fails, none of payment state, bookings or outbox effects may become visible. If commit succeeds but the HTTP response is lost, duplicate delivery returns the existing outcome.

For an asynchronous callback receiver, acknowledge only after a durable receipt commits, then apply the same financial transaction in a worker. That is a separate option; an in-memory queue is not sufficient acknowledgement.

The outbox publisher claims work with a recoverable lease in a short database transaction, releases the connection, publishes with a stable event ID and waits for broker persistence acknowledgement. It then marks publication in another short transaction. A crash between broker ACK and the published marker can produce duplicates; it must not cause event loss. An unavailable broker leaves durable outbox work pending.

Reference: [callback and event insertion](../../../src/ticketing/infrastructure/reservations.py), [publisher](../../../src/ticketing/workers.py), [optional durable confirmation receiver](../../../src/ticketing/infrastructure/payment_confirmation.py).

## 4 Asynchronous issuance with duplicate protection

The event envelope preserves event_id, schema_version, aggregate_id, event_type, occurred_at and payload. For OrderPaid, aggregate_id is the order identity and payload includes order_id; correlation data is diagnostic, not authentication. event_id must remain unchanged through publication retries and replay.

```json
{
  "event_id": "10000000-0000-4000-8000-000000000001",
  "schema_version": 1,
  "aggregate_id": "20000000-0000-4000-8000-000000000001",
  "event_type": "OrderPaid",
  "occurred_at": "2026-10-14T12:30:00Z",
  "payload": {
    "order_id": "20000000-0000-4000-8000-000000000001",
    "correlation_id": "example-only"
  }
}
```

```text
validate envelope before acquiring database slot
BEGIN on PRIMARY
  INSERT inbox(consumerIdentity, eventId) with unique constraint
  if duplicate: commit no repeated business effect
  otherwise lock order and verify committed financial/booking state
  create one ticket per booking with unique booking_id
  set order FULFILLED
  append TicketsIssued outbox event in this transaction
COMMIT and release connection
publish committed status snapshot best effort
ACK delivery or advance only the successfully committed Kafka offsets
```

Inbox insertion and tickets must share the transaction. A crash after commit but before ACK replays safely. Distinct events about the same payment also remain safe through business uniqueness. Poison messages have bounded retries and a durable quarantine/review record; never silently discard a paid order.

Kafka reference uses keyed partitions and committed offset boundaries. A JetStream adapter needs durable replicated storage, publish ACKs, durable consumers, explicit ACK after commit, bounded outstanding deliveries and recoverable retention. Map stable event IDs to broker deduplication where supported, but database inbox protection must outlive the broker's deduplication window. Serialize conflicting order transitions or enforce them under database locks; do not assume parallel consumers complete in delivery order. Use separate consumer identities for fulfillment and projection. NATS KV does not replace Redis multi-seat reservation atomicity.

Start with one event per transaction. ADR0324 grouping up to four was qualified only in the reference short test; a port must reverify full rollback, same-order boundaries, cache-after-commit and acknowledgement semantics before enabling it.

Reference: [consumer transaction and issuance](../../../src/ticketing/workers.py), [ADR0324 grouping boundary](../../adr/0324-bound-fulfillment-transaction-batches.md).

## 5 Cached status reads and controlled connections

Authenticate every read. Scope cache keys by immutable customer/tenant identity and order ID; validate cached ownership, structure, state and ticket identities. Invalidate/reauthorize if the customer supports ownership transfer. Cache content is advisory and never authorizes payment or ownership mutations.

Set freshness from the ORIGINAL snapshot start, not the time a delayed worker publishes it. A failed, delayed or duplicate event cannot extend the freshness deadline or overwrite a newer valid snapshot with an older one. Reference cache freshness is configurable from 1 to 3000 ms and disabled by default; choose and record the customer value.

```text
authenticate; validate customer/order-scoped fresh cache snapshot
if valid: return advisory display state
on miss:
  coalesce concurrent misses per customer/order within bounded wait
  acquire bounded general/status slot
  read authorized state from PRIMARY
  release connection
  cache until original freshness deadline; return result
```

Per-process coalescing still permits one miss per pod; keep the aggregate fallback budget bounded. Cache unavailability must not unleash unlimited database reads. Return processing/retry guidance when read capacity is unavailable. Payment recovery inquiry bypasses the cache and replica routes. Responses containing personal state use private/no-store HTTP policy.

After a payment/ticket transaction commits and releases its connection, publish its authorized snapshot or schedule a refresh. Preserve monotonic/fresh snapshots and a fallback path when publication is lost. Tell clients a minimum polling interval plus jitter; the reference advertises 20% jitter when polling guidance is enabled. No polling loop may hold a connection between requests.

### PGpool and replica routing

| Operation | Required route |
| --- | --- |
| Hold, expiry, payment initiation/callback, issuance, inbox/outbox mutation | Primary |
| Payment-operation inquiry and immediate durable booking/ticket verification | Primary |
| Advisory status cache miss | Primary initially; replicas need an explicit freshness contract |
| Catalogue, reporting, suitable historical reads | Replica permitted within agreed lag bound |

Verify actual PGpool routing for prepared statements, explicit transactions and driver behavior. A SELECT after a write or on a different pooled session can otherwise see replica lag. Retain managed RDS primary/failover discovery.

Budget application connections at maximum replicas including rollout surge, not only current pod count. Retained application pools can occupy PGpool frontend children even while SQL is idle. Count payment/callback, status, writers, fulfillment, publisher, expiry/reconciliation and operational reserve separately.

For each PGpool instance, inspect num_init_children and max_pool: per-backend connections can approach their product. Sum across instances and add direct/admin/health/cancellation requirements. Confirm the version's capacity model and keep both frontend capacity and each RDS node's limits within budget. Distinct database users can increase cached backend combinations; separate role pools do not by themselves reserve backend capacity.

Protect payment/callback admission from status traffic with bounded role concurrency inside the total budget. Determine actual limits through baseline measurements; the reference's 24 PgBouncer backends are not a customer sizing recommendation. Measure acquisition, lock, SQL and COMMIT time separately. Never hold SQL resources while waiting for gateway, broker, cache or customer.

Reference: [status cache](../../../src/ticketing/infrastructure/order_status_cache.py), [post-commit projector](../../../src/ticketing/infrastructure/order_status_projector.py), [configuration defaults](../../../src/ticketing/config.py).

## Storage constraints to map into the customer schema

| Logical uniqueness | Purpose |
| --- | --- |
| (tenant/customer, operation kind, idempotency key) plus request hash | Concurrent identical replay; reject changed payload |
| One order per hold; stable command ID in Redis-first mode | No duplicate durable reservation |
| Booking ownership for (show, seat) | No double-booking under the defined resale policy |
| Provider/merchant payment reference; operation-to-order rules | No duplicated financial effect |
| Provider/merchant callback identity plus payload hash | Detect conflicting duplicate callbacks |
| Outbox event ID | Stable identity across publication retry |
| (consumer identity, event ID) | Durable consumer replay protection |
| Ticket booking ID | One issued ticket per booking |

Use real constraints, not only application checks. Map transaction isolation, lock behavior and cancellation/resale policy before producing migrations. Retention must cover the provider replay/inquiry horizon and operational recovery; do not expire durable financial deduplication just because a seat hold expired.

## Primary technical references

- [Redis atomic scripts](https://redis.io/docs/latest/develop/programmability/eval-intro/) and [WAIT durability limitations](https://redis.io/docs/latest/commands/wait/).
- [NATS JetStream persistence and consumers](https://docs.nats.io/concepts/jetstream).
- [Kafka delivery semantics and external-system boundaries](https://kafka.apache.org/42/design/design/).
- [PGpool connection caching](https://www.pgpool.net/docs/latest/en/html/runtime-config-connection-pooling.html), [frontend and backend limits](https://www.pgpool.net/docs/latest/en/html/runtime-config-connection.html), and [read routing](https://www.pgpool.net/docs/latest/en/html/runtime-config-load-balancing.html).
