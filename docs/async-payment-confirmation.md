# Durable asynchronous payment confirmation

ADR0160 adds a default-off receipt boundary to the signed simulator webhook. PostgreSQL remains the financial authority. Redis atomic holds and expiry, PostgreSQL seat ownership, payment idempotency, transactional outbox and Kafka ticket fulfillment retain their existing behavior.

## Local use

Apply migrations with the existing CLI before enabling intake. Set `PAYMENT_CONFIRMATION_ASYNC=1` for the API and run the `confirmation` worker; the Compose `async-payments` profile provides this worker. For example, after starting dependencies and migrating:

```powershell
$env:PAYMENT_CONFIRMATION_ASYNC="1"
docker compose --profile async-payments up -d api confirmation
```

The signed `POST /v1/webhooks/payments` continues to validate signature, timestamp and schema before admission. With asynchronous intake enabled, HTTP200 with `status: received` and `receipt_id` means the verified callback has committed to the receipt table. It does **not** mean payment was applied or tickets issued. A failed receipt commit never receives a successful acknowledgement. A reused callback ID with a different normalized payload is rejected.

The provider namespace defaults to `simulator`. A real gateway adapter must define its own signature and acknowledgement contract before production use. The default-off endpoint retains the previous synchronous response behavior. OpenAPI route documentation describes both behaviors.

## Processing and recovery

The worker claims receipts with a lease and `FOR UPDATE SKIP LOCKED`. Receipt completion, payment/booking changes, outbox events and capacity release commit together. Duplicate receipts/events remain idempotent. A crash or lost response after commit is safe to replay. A stale worker cannot complete another worker's lease.

Transient database failures use bounded jittered retries. Invalid financial identities or exhausted attempts enter visible `REVIEW`; they retain outstanding capacity and fail queue-drain checks. Operator review resolution and completed-receipt retention tooling are future scope. Never delete unresolved receipts or remove their worker as a shortcut to draining a queue.

No hold TTL is extended by a receipt. A late successful payment for an expired/reassigned seat follows existing refund rules and cannot book the new owner's seat. Receipt acknowledgement does not promise a ticket for such a late payment.

Default limits: 10000 outstanding receipts per provider, 30-second lease, 2 processing slots, 8 claims, retry base100ms capped30seconds. Capacity admission is a short serialized provider-counter update; queueing absorbs bounded bursts and does not establish higher sustained throughput.

## Customer status and capacity

`GET /v1/orders/{id}` preserves customer authorization and private/no-store headers. `ORDER_STATUS_POLL_MS` optionally adds `X-Poll-Interval-Ms` and jitter guidance; the paid journey client honors a validated interval with ±20% jitter. Existing committed-event Redis projections and bounded cache freshness remain in place. An acknowledged callback can leave an order pending until confirmation and ticket fulfillment finish.

Receipt intake uses the existing payment API pool, separated from status-read capacity. The ADR0161 comparison allocates2 worker connections and reduces simulator pool12->10 **in both arms**, retaining simulator8-slot refill, PgBouncer24 and existing API/consumer/writer ceilings. Both arms use500ms polling, refresh-on/cache1000ms and dedup-off. Only asynchronous intake differs.

Metrics expose receipt outcome, processing outcome, receipt/claim/financial duration, receipt age, pending/review count and oldest age. Global audit covers Redis streams, outbox, refresh, simulator callbacks, refunds, Kafka and unresolved receipts/counter consistency. Zero double-booking, post-TTL durability, customer-issued tickets and complete drain remain mandatory.

Local evidence and cloud comparison status are recorded in `docs/capacity/CURRENT_STATE.json`. Preparation and passing local tests are not a production capacity result.
