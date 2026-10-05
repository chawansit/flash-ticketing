# ADR0160: Durable asynchronous payment confirmation

- Status: Proposed; design only, not implemented or capacity-qualified
- Date: 2026-10-05

## Context

The user proposed decoupling gateway confirmation from the rest of checkout. Today the signed webhook calls the financial callback synchronously: it acquires order/payment/hold/seat locks, validates the payment, books or records refund intent, and commits outbox events before responding. Ticket issuance is already asynchronous through OrderPaid and Kafka.

Measured control adr0151-f550731d5714 had callback due-to-claim mean 6,769.57 ms, backlog peak 525, 7.97 status checks per dispatched journey and primary CPU92.08%. Successful callback HTTP delivery averaged130.70ms. The simulator already uses eight independently replenished slots, not a batch barrier. These observations support investigating confirmation throughput and polling amplification; they do not prove the initiating regression cause. The previous passing reference uses different image revisions.

## Proposed decision

Separate durable receipt, financial confirmation and ticket fulfillment. Keep PostgreSQL as payment/seat authority and Kafka/outbox as the committed-event transport.

1. The webhook authenticates the original signed body and validates bounded schema/size before opening a short receipt transaction. Insert a durable payment_webhook_receipts row, uniquely keyed by provider and callback identity, with canonical payload hash, verified payload, database received_at, state, attempts and retry/lease fields. A same-key/same-payload replay returns the existing receipt; changed payload fails explicitly. Return the provider-required successful acknowledgement only after commit. Response means received, not paid, booked or fulfilled. Exact 200/202 response/body depends on the selected gateway contract, which has not yet been supplied.
2. Dedicated bounded confirmation workers claim pending receipts with lease-token fencing and SKIP LOCKED. Commit the lease before doing work; never retain a DB connection across external provider calls. Workers revalidate trusted stored identity/content against the payment/order, amount and currency, and apply existing financial rules. Inbox receipt completion, callback/payment deduplication, booking or refund intent and outbox events commit atomically in the same PostgreSQL transaction. Refactor the existing callback transaction into a shared in-transaction operation rather than call it inside a second transaction.
3. Publish committed OrderPaid/RefundRequested/SeatsChanged through the existing transactional outbox. Existing Kafka fulfillment issues unique tickets and projects committed order/ticket state into Redis. Customer status distinguishes confirmation pending from committed payment and ticket issuance; do not display paid from webhook acceptance alone.
4. Initially default this boundary off and retain the existing route behind a controlled feature flag. Preparation/local validation does not authorize cloud deployment or paid load. Allocate receipt/confirmation capacity within a documented total connection budget; independently scale confirmation processing after measurement. Dedicated compute is a separate topology decision.

## Idempotency, ordering and locks

Delivery is at least once, with effectively single business effects enforced by durable keys/constraints and transactions; do not claim exactly-once transport. Distinct callback IDs for the same payment must not create another booking, ticket or refund. Same-key changed payload remains a conflict. Successful payment cannot regress on a later failure notification; conflicting captures require explicit reconciliation. Preserve financial lock ordering and PostgreSQL booking uniqueness across direct/queued paths and replicas. Process a receipt only while holding its current lease ownership, with token validation in the financial transaction; stale workers cannot mark completion or overwrite newer work.

## Hold TTL and seat ownership

Receipt acceptance is not an inventory guarantee. Preserve the existing hold deadline and current-ownership checks in the first implementation. A receipt arriving before expiry but processed after expiry can require a refund under current policy. Never use received_at or provider timestamps to reclaim a seat already released/reassigned. Test this case explicitly.

If business policy requires every timely successful payment to retain its seat despite processing delay, define a bounded payment-pending inventory lease at checkout, expiry/recovery rules and maximum pending duration in a separate ADR before changing TTL. An unbounded freeze or simply extending Redis TTL is not selected here. Measure confirmation queue age against the remaining hold window; short gateway acknowledgement alone does not qualify successful sales.

## Alternatives

Keep synchronous financial callbacks and protect more API slots: smaller change, but gateway acknowledgement still waits for booking locks and financial work. Publish raw callbacks directly to Kafka: can be durable with correctly configured broker acknowledgements/replication, but introduces Kafka availability into receipt admission and a new authoritative receipt/replay boundary; not selected for the first implementation. In-memory FastAPI background tasks or Redis-only acknowledgement: insufficient durable ownership for acknowledged financial input. Redis can remain an advisory projection, not the authoritative receipt.

## Consequences

The gateway request performs less work and no booking locks; gateway acknowledgement can progress independently of ticket issuance. Payment confirmation still has to acquire financial locks and commit, and the database gains durable receipt/lease/completion writes. Queueing absorbs bounded bursts but does not create sustained processing capacity or eliminate errors. Bounded retries, protected worker capacity, freshness-aware status delivery and queue-age monitoring are required to avoid reproducing the polling feedback loop. No throughput improvement is claimed from this proposal.

## Failure and recovery

- Before receipt commit: do not acknowledge; return a provider-compatible transient failure. After commit with lost response: replay the same receipt safely. Invalid signature/schema is rejected before durable admission; signature freshness is checked at receipt time, not again against a later worker clock.
- Before financial commit: rollback business effects and retry transient lock/network/database failures using bounded exponential backoff and jitter. After financial commit with worker response loss: persisted completion and business idempotency prevent duplicate effects. Expired leases permit recovery with stale-token fencing.
- Broker unavailable: retain committed outbox work; do not roll back a committed payment or require gateway redelivery. Consumer restart/replay retains unique tickets and inbox handling.
- Bound outstanding new receipts and worker concurrency. On admission overload, preserve accepted work, allow same-receipt replay and return a provider-compatible retryable response for new receipts. Do not drop accepted receipts or move funds-related failures into an unobserved dead-letter queue.
- Permanent mismatch, exhausted retries and unknown gateway outcome require visible durable review/reconciliation state. Provider lookup/refund adapters remain future scope; current payments/refunds are simulations. Receipt acknowledgement is not reconciliation proof.
- Rollback must drain or continue processing previously acknowledged receipts before removing confirmation workers; disabling receipt intake cannot abandon accepted work. Keep sensitive receipt payload retention bounded by a documented retention policy after terminal resolution, never expiring unresolved work.

## Decision relationships

If accepted/implemented, partially supersede ADR0004 only at the synchronous webhook acknowledgement boundary. Preserve its payment idempotency, late-payment refund and single-booking rules. ADR0002 outbox and ADR0003 at-least-once delivery remain accepted. ADR0134 financial commit/response-loss tests remain relevant and require extension to the new receipt boundary. ADR0145 admission reservations require review for receipt versus financial-worker capacity; no accepted budget decision is superseded by this design-only proposal.

## Validation evidence and acceptance plan

Only existing code/evidence was read for this proposal; no runtime implementation, migration, new application tests, cloud calls or load occurred. Documentation JSON/link checks are separate from behavioral validation.

Before enabling: execute concurrent duplicate receipts, changed-payload rejection, distinct-ID payment duplicates, invalid signatures, receipt commit/response loss, worker crash/lease takeover, financial commit/ack loss, mixed direct/queued rollout, expired-and-reassigned seats/refunds, invalid amount/currency, out-of-order callbacks and Kafka outage/replay. Retain100-request same-seat exactly-one-winner tests, payment durability, authorization and complete DB/Redis/Kafka queue drain. Verify no DB connection is retained during external requests and total connections/waiters remain bounded.

Measure receipt acknowledgement separately from receipt-to-financial-commit, payment-to-ticket/customer-confirmation, oldest pending receipt, retries/review states, gateway redeliveries, CPU, DB queries/locks/pool waits and status polling. Proposed receipt p95 target100ms must be validated rather than declared achieved. Compare an identified passing control against one candidate with unchanged machines, total budgets, offered journeys and customer-completion gates. At one ticket per successful journey,300000tickets/hour requires83.33 paid-issued tickets/s sustained; acknowledgement RPS is not that measure. A separate bounded cloud scope is required after local correctness qualification.

[Latest measured failure and restoration evidence](../capacity/flash-sale-opening/order-status-dedup-measured-control-failure-2026-10-05.json).
