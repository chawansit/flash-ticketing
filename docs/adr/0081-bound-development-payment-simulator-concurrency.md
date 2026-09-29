# 0081 — Bound development payment-simulator concurrency during paid-ticket validation

- **Status:** Accepted diagnostic experiment; the 8-thread candidate was not
  promoted. Production payment-provider sizing remains undecided.
- **Context:** On 2026-09-29, a Redis-first 30 paid-journeys/s stage for 60 seconds
  scheduled 1,800 journeys but dropped 67 at the generator's in-flight limit
  of 500. The 1,733 accepted journeys all became one paid, fulfilled order and
  one ticket after drain. Stage-aligned samples showed up to 254 payment
  attempts with callback deliveries outstanding and up to 370 PENDING orders.
  The single development simulator's four threads accumulated about 247 busy
  seconds over 62 sampled seconds, close to their 248-second total capacity.
  One consumer accumulated about 48 busy seconds. Outbox backlog peaked at
  18; PostgreSQL lock waiters peaked at one. This points to simulator
  saturation as a likely test-harness limiter, but does not prove that the
  webhook handler or database would sustain the target after simulator
  scaling. The simulator sends three intentional callbacks per payment.
- **Decision:** Compare the identical 30/s, 60-second paid-ticket stage after
  increasing only the development simulator from four to eight concurrent
  delivery threads in its existing replica. Keep its existing DB pool maximum
  of 12, API count, reservation writers, consumer count, workload, fixture,
  callback duplication, and read-poll interval unchanged. Use a one-command
  Compose environment override, verify the live container value, and restore
  four threads in a finally block. Continue collecting per-second callback
  backlog, issued tickets, outbox, worker busy time and PostgreSQL waits.
  Do not interpret the simulator throughput as a production payment-provider
  SLA.
- **Alternatives:** Adding a second simulator replica would also raise
  delivery concurrency but changes process and connection topology. Reducing
  duplicate callbacks would weaken the idempotency workload. Scaling the
  Kafka consumer first does not address the largest observed backlog.
  Asynchronous payment queuing is a separate architectural decision and is
  not selected here.
- **Consequences:** The simulator can issue up to four more concurrent
  webhook calls and use more of its existing 12-connection DB pool. PgBouncer,
  RDS or API webhook admission may become the next limiter. The controlled
  comparison keeps the same number of buyers and sellable seats. It cannot
  certify 300,000 tickets/hour without a successful one-hour stage.
- **Failure and recovery:** If the simulator or API becomes unhealthy,
  callbacks fail, queue depth grows, any accepted order lacks its ticket,
  or the strict stage gate fails, stop escalation. The runner restores four
  simulator threads, rolls back Redis-first candidate settings and retires the
  synthetic fixture. Existing leased payment attempts can be retried after
  their lease expires; duplicate callbacks remain idempotent. Audit every
  accepted journey and drain queues before concluding data integrity.
- **Validation evidence:** The 4-thread baseline and accepted-order audit are
  in [the 2026-09-29 paid-ticket stage evidence](../capacity/flash-sale-opening/paid-ticket-stages-2026-09-29.json).
  The 8-thread comparison is in
  [the stage-aligned pipeline evidence](../capacity/flash-sale-opening/paid-ticket-pipeline-2026-09-29.json).
  It reduced peak pending callback attempts from 254 to 25, but peak PAID
  orders awaiting fulfillment rose from 53 to 279. The strict stage still
  failed: 65 generator drops and 17 unexpected HTTP 503 responses; hold-to-ticket
  p95 rose from 23.20 to 25.80 seconds. After TTL, 1,731 accepted payments
  had 1,731 tickets and the four payment-rejected orders had expired. No
  duplicate booking or queue residue remained. The runner restored the
  original four-thread simulator. This result rejects eight threads as a
  standalone fix and points to the downstream webhook/fulfillment path for
  the next investigation; it does not prove one specific downstream cause.
