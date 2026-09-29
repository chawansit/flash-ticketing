# End-to-end flash-sale validation protocol

Status: proposed measurement protocol. A bounded Redis-first HTTP journey
probe was run on Huawei on 2026-09-29; the small functional smoke passed.
The opening-burst evidence does not certify payment or ticket capacity.

## Objective and counting rule

The target is at least 300,000 distinct tickets issued for successfully paid
orders between 10:00:00 and 11:00:00 local sale time. Count a ticket only
when the order is FULFILLED and the unique ticket row is durable. A provisional
HTTP 202 hold, durable hold, payment initiation, callback receipt, or PAID
order alone is not a completed sale.

300,000 / 3,600 = 83.34 issued tickets/s over the hour. If arrivals close
at 10:55 to leave five minutes for fulfillment, the hold/payment pipeline
must average at least 300,000 / 3,300 = 90.91 successful purchases/s during
the admission period. This is an arithmetic floor, not measured capacity.
At 80% purchase conversion, at least 375,000 hold attempts are required
before accounting for seat conflicts. Those attempts need to recycle
abandoned inventory correctly; adding extra seats would conceal that behavior.

## Workload stages

1. Build a development-only, distributed journey generator with coordinated
   start time, bounded concurrency and no hidden retries. Each purchase
   journey holds one seat, polls its reservation command until DURABLE,
   initiates simulated payment, and observes the order until FULFILLED with
   one ticket. Record per-step status, latency and wall-clock completion.
   Do not let slow payment polling delay or alter the scheduled arrival
   rate without reporting generator drops.
2. Run a small smoke stage with the real RDS, DCS and Kafka topology to
   validate the 202/PENDING-to-DURABLE boundary and duplicate payment
   callbacks. Audit every issued ticket against its booking and payment.
3. Increase completed-journey throughput in controlled stages, holding
   the same code/image versions and connection budget. At each stage,
   check request fidelity, reservation guard, payment backlog, Kafka lag,
   ticket issuance delay, PostgreSQL waits and full queue drain. Stop
   at the first failed gate.
4. Run a one-hour positive-payment stage with at least 300,000 sellable
   distinct seats (1,000 shows x 300 seats is the exact minimum) and
   enough buyers. Open at 10:00 and stop admitting new journeys no later
   than 10:55. Verify 300,000 FULFILLED orders and 300,000 unique tickets
   by 11:00. This isolates the maximum end-to-end pipeline before adding
   abandonment.
5. Separately run a realistic conversion/contested-seat stage with a
   fixed 300,000-seat inventory. Include abandoned holds, expiry, seat
   reuse, hot-seat conflicts and duplicate callbacks. Distinguish
   expected conflict responses from unexpected failures. A waiting-room
   or edge admission design requires a separate ADR before implementation.

## Strict result gates

- Scheduled versus dispatched journeys and all physical HTTP attempts
  reconcile. Generator drops and retry counts are reported; they cannot
  be omitted from a passing result.
- Every accepted Redis-first hold reaches a durable terminal status
  before payment. Unknown or failed commands are never counted as sales.
- Every successful payment maps to exactly one fulfilled order, one
  booking per sold seat, and one ticket per booking. There are zero
  duplicate bookings, broken links or missing issued tickets.
- Payment callbacks and Kafka events may be delivered more than once,
  but final payment and ticket state is idempotent. Outbox, consumer,
  callback and reservation queues drain within an explicitly measured
  post-run window.
- Ticket count is measured at the deadline, not after an unbounded
  drain. Report p50/p95/p99 hold-to-durable, payment-to-paid and
  paid-to-ticket delays separately.
- Post-TTL audit, source/image identity, healthy rollback, and zero
  remaining synthetic fixtures/due work are required for certification.

The 2026-09-29 opening probe only tests read/hold HTTP traffic and
post-TTL reservation durability. It contains no paid-ticket throughput
measurement. The size and timing of the 10:00 visitor burst remain input
parameters until the expected unique-visitor count is supplied.


## Bounded journey smoke probe

The development-only HTTP probe in scripts/checkout_journey_probe.py uses a
fresh private load manifest. It sends a provisional hold, polls the reservation
command until DURABLE, starts simulated payment with configurable duplicate
callbacks, then polls the order until FULFILLED with exactly one ticket. It
checks distinct order and ticket IDs across its bounded journeys and writes
aggregate outcomes only; it never writes bearer tokens or individual IDs to
its result. The probe is limited to 1,000 journeys and a 110-second per-journey
deadline, below the current 120-second hold TTL. It is deliberately not an
arrival-rate generator and does not replace PostgreSQL/Kafka audit.

Focused unit tests cover waiting for durability before payment, refusing
payment after a failed reservation command, and rejecting an invalid
two-ticket outcome, and confirming its result excludes private tokens and IDs.
The 2026-09-29 smoke used a fresh isolated cloud fixture and verified all ten
paid-ticket journeys against PostgreSQL and drained queues. It does not
measure an arrival rate or establish one-hour paid-ticket capacity.
