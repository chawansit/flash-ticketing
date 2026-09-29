# Flash-sale opening workload

Status: two Huawei opening profiles measured; end-to-end ticket
capacity has **not yet been measured**. This profile changes only the load generator and
stage fixture calculation. It does not add a waiting room or change reservation,
payment, or persistence architecture.

## Demand and sale objective

The business objective is 300,000 paid and issued tickets within one hour after
a 10:00 sale opening. That requires at least 83.34 completed tickets/s on average
over the hour, plus headroom for failed holds, abandoned checkout, payment failure,
and uneven arrivals. A seat-map/hold benchmark does not establish this objective.
The previous measured point is one passing 30-minute run at 1,100 total RPS with
94% delta reads and 6% distinct-seat holds (66 holds/s), and a failed 15-minute
run at 1,200 total RPS due to 32 persistence-lag rejections. Neither exercised
payment and ticket delivery under this load.

The old burst profile has one minute at base rate before its peak, unlike a
10:00 opening. The new opt-in opening-burst profile starts at its peak:

| Time after 10:00 | Offered rate | At base 250 RPS | At base 300 RPS |
| --- | ---: | ---: | ---: |
| 0–30 s | 4 x base | 1,000 RPS | 1,200 RPS |
| 30–120 s | 2 x base | 500 RPS | 600 RPS |
| 120–300 s | base | 250 RPS | 300 RPS |

Total offered requests equal 480 times the base rate. With eight generator
workers selecting 6% holds independently, this is 7,202 scheduled holds at base
250 or 8,644 at base 300. The two or four extra holds are per-worker rounding.
This is a **five-minute
opening diagnostic**, not a one-hour sales simulation or a model of how many
distinct people simultaneously click at precisely 10:00. The read/hold mix is
fixed for the diagnostic. Real visitor counts, polling cadence, hot-seat
contention, checkout conversion, and payment/ticket timings still need a
separate workload model.

## Controlled validation

1. Verify matching source and image versions, healthy services, empty work
   queues, an open synthetic sale window, and enough fresh distinct seats for
   the entire profile. The stage runner now counts all three phases before
   preparing fixtures. Keep the measured topology and connection budget fixed.
2. Use base 250 RPS first. This briefly reaches the previously validated
   1,000 RPS region. Review per-30-second latency, HTTP status, generator
   drops, admission, writer command age, Redis stream age, PostgreSQL waits,
   RDS resource use, and queue growth. A generator drop or protective 503 is
   a failed stage, not successful backpressure.
3. Only if every gate passes, repeat at base 300 RPS, peaking at 1,200 RPS
   for 30 seconds. Stop escalation on the first failed gate; do not retry a
   failed rate to manufacture a pass. Retain redacted compact evidence.
4. For each stage, run post-TTL exact durability and zero-overlap checks,
   wait for queue drain, restore the original topology, and retire synthetic
   sale fixtures. Do not count provisional HTTP 202 as a completed sale.
5. Separately add a one-hour end-to-end scenario covering hold durability,
   order, payment outcome, Kafka processing, and ticket issuance. Its pass
   condition is 300,000 *unique paid and issued* tickets by the one-hour
   deadline, with zero double booking and bounded queue/issuance latency.
   Inventory must exceed 300,000 distinct sellable seats; the existing
   800 x 300-seat fixture has only 240,000.

The existing unattended runner accepts --opening-burst --seconds 300. Its
strict response, latency, generator fidelity, durability, overlap, queue, and
rollback gates remain in force. The profile is opt-in so earlier flat-load and
legacy burst evidence remain comparable.

If measurements show admission must absorb arrivals beyond safe hold capacity,
propose a waiting-room or rate-shaping architecture in a new ADR before
implementation. This document does not select that architecture or assume
that horizontal scale is linear.

## First measured opening control

On 2026-09-29, the Huawei control stage at base 250 RPS passed every strict
gate on revision db373f1. It offered 1,000 RPS immediately for 30 seconds,
then 500 RPS for 90 seconds and 250 RPS for 180 seconds. All 120,000
scheduled requests were sent once without retries or generator drops:
112,798 reads returned HTTP 200 and 7,202 holds returned provisional HTTP
202. Worst-worker p95 was 10.15 ms for reads and 20.48 ms for holds. The
post-TTL audit verified all 7,202 holds, with zero overlapping seat intervals,
zero broken links and zero remaining queues; rollback passed. The audit took
16.22 seconds. Synthetic sale fixtures were subsequently retired and the
active capacity and due work returned to zero.

This is a disjoint-seat opening diagnostic, **not** proof of 300,000 paid
tickets/hour or hot-seat contention capacity. The next controlled stage is
base 300 RPS, peaking at 1,200 RPS, only while the environment is clean.

Evidence: [strict stage verdict](stages/opening-250/stage-result.json),
[generator summary](stages/opening-250/generator-summary.json),
[post-TTL audit](stages/opening-250/durability.json), and
[rollback](stages/opening-250/rollback.json).


## Second opening probe: request path passed, stage failed

The base-300 stage on revision 79ed9ea immediately offered 1,200 RPS for
30 seconds, then 600 RPS for 90 seconds and 300 RPS for 180 seconds. All
144,000 scheduled requests were sent once: 135,356 reads returned HTTP 200
and 8,644 holds returned provisional HTTP 202, with zero generator drops,
transport errors, retries or admission rejections. Worst-worker read/hold p95
was 11.67/24.16 ms. Post-TTL audit verified all 8,644 accepted holds,
zero overlapping seat intervals, zero broken links and empty queues;
rollback passed. Synthetic fixtures were retired afterward.

**The strict stage verdict is failed.** The first observer-stop command
returned exit code 1, although the second cleanup attempt completed and
the other gates passed. The original helper produced no failure reason.
The raw observer files and compact summaries were present, so the
available evidence cannot identify which individual check failed.
A follow-up change records explicit observer failure labels and preserves
them in compact evidence. The old stop helper invoked shell wait on processes
started by an earlier shell; that does not wait for those non-child processes
to flush samples. It now uses a bounded process-exit check before summarizing.
This timing race is a plausible cause, **not a proven attribution** for the
old run. Do not promote this result to a passed opening-sale capacity point
or rerun merely to obtain a pass.

Evidence: [failed stage verdict](stages/opening-300-first/stage-result.json),
[generator summary](stages/opening-300-first/generator-summary.json),
[post-TTL audit](stages/opening-300-first/durability.json), and
[rollback](stages/opening-300-first/rollback.json).


## Observer-fix validation

After changing observer teardown to wait for process exit and record failed
checks, the unchanged base-300 opening workload was repeated on revision
cdbba10. **This repeat passed every strict stage gate**, including observer
collection and rollback. All 144,000 scheduled requests were sent once:
135,356 reads returned HTTP 200 and 8,644 holds returned provisional HTTP
202, with zero generator drops, transport errors, retries, or admission
rejections. Worst-worker read/hold p95 was 10.35/21.03 ms. Post-TTL audit
verified 8,644 durable holds, zero overlapping seat intervals, zero broken
links and all queues at zero. The audit took 14.84 seconds. The observer
failure-label file was empty. Synthetic fixtures were retired afterward.

This is **one passing five-minute opening profile**, with a 30-second peak
of 1,200 RPS. The first base-300 result remains failed in the evidence.
Neither run validates sustained 1,200 RPS (the earlier 15-minute flat run
failed), simultaneous contention for the same seat, 300,000 paid and issued
tickets in one hour, or a production SLA.

Evidence: [passed stage verdict](stages/opening-300-repeat/stage-result.json),
[generator summary](stages/opening-300-repeat/generator-summary.json),
[post-TTL audit](stages/opening-300-repeat/durability.json),
[reservation-writer metrics](stages/opening-300-repeat/reservation-writer-metrics.json),
and [rollback](stages/opening-300-repeat/rollback.json).

The [end-to-end ticket validation protocol](checkout-validation-plan.md) defines
the separate one-hour measurement needed for the 300,000-ticket target.

## Paid-ticket checkout smoke — 2026-09-29

A bounded functional probe exercised the actual Huawei API, PostgreSQL RDS,
Redis/DCS, simulator and Kafka/consumer path on two fresh, isolated development
shows. Ten concurrent-style journeys (maximum five in flight) each waited for
a Redis-first hold to become PostgreSQL-durable, initiated a successful
simulated payment with three callback delivery attempts, and observed one
issued ticket. All ten reached FULFILLED. PostgreSQL then showed 10 orders,
10 successful payments, 10 bookings and 10 tickets; the callback delivery
targets were complete, the outbox and reservation queues were empty, and no
duplicate booked seat or dead letter was found. The fixture sale windows were
closed and the prior PostgreSQL reservation mode was restored.

The hold-to-ticket p95 was 687.17 ms for this tiny sample; payment-to-ticket
p95 was 460.31 ms. These are **functional smoke timings**, not capacity
percentiles. The deployed application revision was `cdbba10`. See the
[redacted aggregate result](checkout-smoke-2026-09-29.json). The new reusable
probe/audit/runner scripts on this branch were transferred as test tooling;
they were not part of the deployed application image. A measured
300,000-paid-tickets/hour result still requires a distributed, controlled
arrival-rate generator and a one-hour run.
## Controlled paid-ticket stages — 2026-09-29

A scheduled development-only journey generator now measures paid and issued
tickets rather than treating hold HTTP responses as completed sales. It records
scheduled/dispatched journeys, generator drops, physical HTTP attempts, retry
count, dispatch lag and completion by a fixed deadline. The short-stage runner
uses a fresh isolated fixture, three simulated callback deliveries per payment,
PostgreSQL/queue audit, fixture retirement and topology rollback. Failed stages
remain failed even when every accepted order eventually produces a ticket.
The [aggregate evidence](paid-ticket-stages-2026-09-29.json) contains no
credentials, buyer tokens or individual order/ticket IDs.

| Stage | Strict result | Scheduled / dispatched / tickets observed | Drops / unexpected responses | Hold-to-ticket p95 |
| --- | --- | --- | --- | --- |
| 10 purchases/s for 30 s, in-flight 100, poll 0.2 s | Pass | 300 / 300 / 300 | 0 / 0 | 0.69 s |
| 30 purchases/s for 30 s, in-flight 100, poll 0.2 s | Fail | 900 / 769 / 766 | 131 / three order GET 503 | 5.23 s |
| 30 purchases/s for 30 s, in-flight 250, poll 0.2 s | Pass | 900 / 900 / 900 | 0 / 0 | 9.90 s |
| 30 purchases/s for 30 s, in-flight 250, poll 1 s | Fail | 900 / 874 / 873 | 26 / one order GET 503 | 10.25 s |

The 30/s failure with in-flight 100 was partly a generator limit: increasing
that limit to 250 allowed all 900 journeys to dispatch and finish in the
otherwise matching run. Slower order polling did not improve the strict gate
in its single comparison run. Each accepted journey in both failed stages was
subsequently audited as a durable fulfilled order and unique ticket (769 and
874 respectively); all callback delivery targets and queues drained. The
failed-stage audit does **not** convert those stages into passes because
scheduled buyers were dropped and status reads returned 503.

Even the passing 30/s run took 38.7 seconds to finish journeys scheduled over
30 seconds, with payment-to-ticket p95 at 7.63 seconds. This suggests backlog
during the burst, but these snapshots do not isolate whether the simulator,
payment callback handler, Kafka consumer or PostgreSQL transaction path is the
limiter. Before raising the paid-ticket rate, collect stage-aligned payment
queue depth, simulator throughput and busy time, callback latency, outbox age,
consumer lag and RDS waits. The 300,000 tickets/hour goal requires at least
83.34 durable tickets/s for a full hour; no hourly result has been measured.
## Stage-aligned payment pipeline diagnostic

A one-minute 30-purchases/s run failed the strict gate at both four and eight
development simulator threads. [The paired aggregate evidence](paid-ticket-pipeline-2026-09-29.json)
records one-second samples. With four threads, 1,733/1,800 journeys were
dispatched, 67 dropped, callback backlog peaked at 254 attempts, and
hold-to-ticket p95 was 23.20 s. The simulator accumulated 247 busy seconds
over 62 sampled seconds on four threads. All 1,733 accepted journeys were
fulfilled and audited after drain.

With eight threads, 1,735/1,800 were dispatched, 65 dropped, and 17 unexpected
503 responses occurred (13 order reads, four payment starts). Peak callback
backlog fell to 25, but PAID orders awaiting fulfillment peaked at 279 instead
of 53; hold-to-ticket p95 rose to 25.80 s. This is evidence that simulator
concurrency alone moved pressure downstream, not a passed capacity gain.
After hold expiry, 1,731 paid orders had 1,731 tickets and four payment-rejected
orders were EXPIRED. All 5,193 callback delivery targets completed, no
duplicate booking was found, and queues drained. The eight-thread setting was
restored to four under [ADR 0081](../../adr/0081-bound-development-payment-simulator-concurrency.md).

The next experiment should instrument API admission/connection waits, payment
webhook latency and PAID-to-FULFILLED event lag per worker while keeping a
fixed paid-journey rate. Do not infer a one-hour sales rate from these
one-minute diagnostic runs.

## API pool saturation in paid-ticket validation

Two further one-minute development-only stages kept 30 scheduled paid
journeys/s, four API replicas, a three-connection/three-waiter DB pool per API,
one Kafka consumer, eight simulator threads, three callback deliveries and
zero generator retries. Only order-status polling changed from 0.2 to 1
second. Both stages **failed** the strict gate.

| Poll interval | Dispatched / scheduled | Generator drops | Checkout HTTP 503 | API DB-pool `TooManyRequests` | Peak PAID awaiting ticket | Hold-to-ticket p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.2 s | 1,757 / 1,800 | 43 | 15 | 28 | 296 | 24.91 s |
| 1 s | 1,694 / 1,800 | 106 | 9 | 13 | 355 | 26.49 s |

The per-replica metrics identify the 503 source as the API DB pool rejecting
waiters (`TooManyRequests`), not PostgreSQL lock waits. The pool reached its
three-connection ceiling and three waiting requests per replica. The 28 and
13 DB-pool failures include webhook and other routes; the checkout HTTP 503
column counts only buyer-visible order reads/payment starts. Slower polling
reduced those failures but also reduced completed journeys and increased the
PAID-to-FULFILLED backlog, so it is not a capacity fix. The single consumer
was active for about 52 of 60 sampled seconds in each comparison; this
suggests fulfillment processing needs a separate controlled scaling test.

Post-TTL read-only audits found 1,756 and 1,693 successfully paid orders,
respectively, each with exactly one ticket. The one payment-rejected order in
each stage expired. All callback targets completed, no duplicate booking
was found, queues drained, synthetic fixtures were retired, and the simulator
and API topology were restored. This integrity result does not change either
strict stage failure. See the [redacted API-pool comparison](paid-ticket-api-pool-2026-09-29.json).
A one-hour 300,000-ticket result remains unmeasured.


## Two-consumer paid-fulfillment diagnostic

Under [ADR 0082](../../adr/0082-two-consumer-paid-fulfillment-diagnostic.md),
one bounded repeat changed only the Kafka consumer count from one to two.
The 30 paid-journeys/s, 60-second development stage sent all 1,800
scheduled journeys with zero generator drops. Peak PAID orders awaiting a
ticket fell from 296 to 9 and hold-to-ticket p95 from 24.91 to 1.11 seconds.
The quicker completion also reduced physical order-status GETs from 13,674
to 5,149, though fixture and timing variability prevent attributing every
difference to the consumer change.

The strict stage **still failed**: four order GETs and one payment POST
returned 503. Per-replica API metrics recorded 12 DB-pool
`TooManyRequests` failures across all routes. No retry hid these
responses. After TTL, the 1,799 paid orders each had one ticket and the
payment-rejected order expired; 5,397 callback delivery targets completed,
zero double-booking was found and every queue drained. The fixture was
retired; rollback restored one consumer and the original simulator setting.
The [redacted one-versus-two comparison](paid-ticket-two-consumers-2026-09-29.json)
is a diagnostic, not a passed production capacity test. The next isolated
work is removing API pool 503s without extending transaction time or
weakening the strict ticket gate, then repeating the two-consumer stage.
