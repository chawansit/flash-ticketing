# Architecture and concurrency contract

## Alignment with the concert optimization document

This implements the document's no-waiting-room direction: memory-backed browsing, per-seat
contention shielding, one seat-write owner, short transactions, payment outside locks,
non-blocking expiry, bounded connections and asynchronous post-payment effects. Kafka replaces
the reference deck's NATS, following the requested stack. PgBouncer runs in transaction mode.

## Data and invariants

- `event_seats`: one row per `(event_id, seat_id)`; nullable current hold/deadline and booked order.
- `holds`: owner, expiry and ACTIVE/RELEASED/EXPIRED/CONSUMED lifecycle.
- `orders`: one per hold (unique constraint), immutable amount/currency and order state.
- `order_items`: immutable captured seat prices. Money uses integer minor units.
- `bookings`: unique `(event_id, seat_id)`, the final duplicate-booking backstop.
- `payment_attempts`: one durable simulated attempt per order with a reclaimable delivery lease.
- `payment_callbacks`: delivery-ID deduplication and payload-hash validation.
- `refund_requests`: unique payment ID, preventing duplicate simulated refunds.
- `idempotency_records`: actor/operation/key uniqueness and canonical request hash.
- `outbox_events`, `consumer_inbox`, `tickets`, `dead_letters`: reliable processing and recovery.

Every write uses the primary database. Read replicas are not used for ownership decisions.

## Reservation linearization

1. Pass a non-waiting per-process API admission gate (8 concurrent reservation requests by
   default), then acquire a Redis Lua all-or-nothing, short-lived per-seat shield. Its token-checked release
   cannot erase another attempt's lease. Failure returns immediately. Redis outage fails admission closed.
2. Start a PostgreSQL READ COMMITTED transaction; claim the key or replay its original result.
3. Lock all requested seat rows in sorted order using `FOR UPDATE NOWAIT`.
4. Read `clock_timestamp()` **after locks are acquired**; validate sale window, booking state and expiry.
5. Create the hold, pending order and immutable order items; assign seats; write a seat-change outbox event.
6. Commit everything together, then release the Redis shield.

Any exception rolls back the complete multi-seat request. A Redis lease that expires early cannot
cause double-booking because the database still serializes ownership. Redis is an optimization,
not a distributed booking lock.

The fast path uses explicit NOWAIT locks rather than treating conditional UPDATE as inherently
non-blocking. This supports all-or-nothing multi-seat requests. The deck's single-statement
reservation example and this implementation share the same atomic ownership requirement.

## Lock ordering and deadlines

Existing order mutations lock **order -> payment (when applicable) -> hold -> sorted seats**.
New reservations lock seats only; they never mutate or lock previous owners' orders or holds.
Expiry locks one candidate order with SKIP LOCKED, then uses NOWAIT for the remaining rows.
It returns to housekeeping later if busy. No gateway, Redis or Kafka call occurs while seat locks
are held. Application connection acquisition is bounded to 150 ms; DB lock timeout is 75 ms and
statement timeout is 1.5 seconds. No synchronous blind contention retries.

Expiry is logical: a seat can be reclaimed when its stored deadline passes, independently of
the cleanup worker. Cleanup clears only seats still referencing its own hold. Cached HELD state
is therefore advisory and includes the deadline.

## Payment transaction

Payment initiation persists a dispatchable attempt and returns before any network call.
The simulator leases the attempt, commits, calls the signed HTTP webhook, then acknowledges
delivery in a new transaction. A crash repeats the same callback.

The callback validates its HMAC and timestamp, then locks the order/payment/hold/seats. Amount,
currency and order must match. Callback ID plus payload hash protects exact replays; payment
state protects semantic duplicates with new delivery IDs.

A valid unexpired hold becomes bookings + SOLD seats + PAID order + OrderPaid outbox event in
one transaction. The decision linearizes at the post-lock database-time check. Later callbacks
cannot regress an already successful payment. Success after expiry, release or prior failure
creates a durable refund request and never takes another owner's seat. Initiating payment does
not extend the hold. This explicit late-success rule supplements the reference document.

## Outbox and consumers

Publisher leases commit before Kafka network I/O. Publication is acknowledged in PostgreSQL
only after broker acknowledgement. A crash between these steps repeats the stable event ID.
The aggregate ID is the Kafka key. Multiple publishers can publish an aggregate's events out of
order; consumers must remain order-independent, as these handlers do. No global ordering claim.

Ticket creation, order fulfillment and the consumer inbox entry share a database transaction.
Kafka offsets commit afterward. Unique booking-to-ticket references guard even semantically
duplicate OrderPaid events. Redis updates cannot share that transaction: seat events rebuild
current authoritative snapshots before inbox acknowledgement, and periodic refresh repairs missed
updates. Cache replacement uses monotonically increasing sums of seat versions to reject stale
rebuilds; unchanged seats preserve their delta cursor. Seat inventory is static in this MVP.

After five failed processing attempts, the consumer writes a durable dead letter before advancing
its offset. If that write fails, it seeks back to the message. Operators can replay after repair.
Delivery is at least once; external notification logs are best-effort development output.
