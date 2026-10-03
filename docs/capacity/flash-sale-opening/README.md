# Flash-sale opening workload

Status: two Huawei request-opening profiles and bounded one-minute paid-ticket
stages measured. **One-hour paid-ticket capacity remains unverified.** The original
opening profile changes only the load generator and stage fixture calculation. It does not add a waiting room or change reservation,
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


## Bounded API pool-waiter candidate

[ADR 0083](../../adr/0083-bounded-api-db-pool-waiters.md) separates the API
DB-pool waiter cap from its three-connection limit. An opt-in comparison
raised the waiter cap from three to twelve on each of four API replicas.
With two consumers, the same one-minute 30/s workload dispatched all
1,800 buyers, issued 1,800 durable tickets by deadline, and recorded no
unexpected HTTP response or retry. API metrics recorded no pool acquisition
error; hold-to-ticket p95 was 1.51 seconds. The prior three-waiter comparison
had five buyer-visible 503s and failed.

The **overall stage still failed** because the original runner required every
seat-refresh queue to be zero at the instant it checked. Six refreshes were
pending then, and a later read-only audit found zero; the runner had not
measured a drain window. The candidate was rolled back. The
[redacted comparison](paid-ticket-pool-waiters-2026-09-29.json) preserves that
failure. A corrected runner will allow at most 120 seconds for queues to
drain, record initial/final counts and elapsed time, and repeat the same
stage before any capacity claim.


## Callback multiplicity and the 45/s boundary

With two consumers and twelve bounded API DB-pool waiters, the 30/s,
60-second stage using **three callback deliveries for every payment**
passed all runner and post-TTL gates: 1,800/1,800 scheduled buyers got a
durable ticket by deadline, with zero unexpected HTTP responses, zero
duplicate booking and drained queues. Hold-to-ticket p95 was 1.14 seconds.

The matching **45/s, three-delivery** stage failed: 2,206/2,700 buyers
were dispatched, 494 were dropped at the generator's bounded in-flight
limit, and hold-to-ticket p95 rose to 22.53 seconds. Peak pending
callback deliveries reached 296. No API pool failure was observed.
All 2,206 accepted payments ultimately had unique tickets, all 6,618
delivery targets completed after drain and post-TTL queues were empty.
The stage remains a strict failure. See the
[redacted callback-mix evidence](paid-ticket-callback-mix-2026-09-29.json).

Sending three callbacks for every payment is an intentional idempotency
stress profile, not an assumed production payment-provider duplicate rate.
The next controlled capacity stage uses one callback per payment at 45/s,
while keeping the three-delivery profile as a separate correctness stress.
This comparison changes workload and cannot by itself promote a higher
production capacity estimate.


The subsequent **45/s, one-delivery** stage also failed its strict gate:
2,550/2,700 journeys were dispatched, 150 dropped at the generator's
in-flight limit, and hold-to-ticket p95 was 19.94 seconds. Reducing callback
multiplicity lowered peak pending deliveries from 296 to 41, while peak
PAID orders awaiting tickets rose from 22 to 140. The single publisher
accumulated 57.8 busy seconds across 57 sampled seconds; both consumers
combined accumulated 87.7 busy seconds. These measurements do not by
themselves prove which event-processing stage is primary; Kafka partition
lag must be captured before scaling a worker. API pool 503s stayed at zero.

The original failure-audit script incorrectly assumed exactly three
callback deliveries per payment. A corrected read-only post-TTL audit with
the declared one-delivery target passed: all 2,550 accepted payments had
one ticket, 2,550/2,550 callbacks completed, there was no double-booking,
and all queues were empty. The runner stage remains **failed** because
150 scheduled buyers were dropped. See the updated
[callback-profile evidence](paid-ticket-callback-mix-2026-09-29.json).

## Stage-aligned Kafka lag at 45 paid journeys/s

A repeat 45/s, one-callback, 60-second stage on revision a7a394d **failed**:
2,268 of 2,700 journeys were dispatched, 432 were dropped at the 500
in-flight cap, and hold-to-ticket p95 was 23.55 seconds. The Kafka group
had two members throughout 22 valid samples; total lag peaked at 912 and
then drained to zero. All six partitions accumulated lag (per-partition
peaks 134–217). The outbox peak was 27 and PAID orders awaiting tickets
peaked at 223. Durable hold p95 was also 10.71 seconds, so ticket issuance
is not the only source of journey latency.

After TTL, all 2,268 dispatched buyers had unique paid tickets; 2,268
callbacks completed, no double booking or dead letters were found, and
the synthetic fixture was retired. Rollback restored one consumer, three
API DB-pool waiters per replica, and four simulator threads. The strict
stage remains **failed**. This evidence motivates bounded paid-event
batching, but it does not prove that batching will reach 45/s or the
83.34 completed tickets/s business minimum. See the
[redacted Kafka diagnostic](paid-ticket-kafka-lag-2026-09-29.json).

## Four-consumer paid-ticket diagnostic

With four Kafka consumers and the same 45/s one-callback workload, the
60-second stage on revision cad7e30 **passed** all strict, post-TTL and
rollback gates. It dispatched 2,700/2,700 buyers with zero retries or
drops; all had distinct paid tickets by the deadline. Hold-to-ticket p95
was 5.00 s versus 23.55 s in the failed two-consumer repeat, and peak
Kafka group lag fell from 912 to 142. No double booking or dead letters
were found. This is a one-minute synthetic diagnostic, not a sustained
production capacity estimate. The original one-consumer topology was
restored.

At 60/s, four consumers **failed**: only 2,772/3,600 journeys were
dispatched, with 828 generator drops and hold-to-ticket p95 of 21.04 s.
Hold-to-durable p95 grew from 2.39 s at 45/s to 12.49 s; peak PAID
waiting for tickets was only 36, and Kafka lag peaked at 206 before
draining. All 2,772 accepted payments later had unique tickets, callback
deliveries completed and post-TTL integrity passed. Rollback and fixture
retirement passed. This points to a substantial pre-fulfillment delay
under 60/s, but the current probe does not separate HTTP hold latency
from command-durability polling. Stop escalation; instrument those
phases before changing fulfillment transactions. See the
[redacted four-consumer comparison](paid-ticket-four-consumers-2026-09-29.json).

The paid-journey probe now reports separate p95 durations for the hold
HTTP request, command-durability wait, payment-initiation HTTP request
and ticket wait, in addition to end-to-end spans. These fields contain
only aggregate timings. The next same-rate diagnostic can identify
which upstream phase grows at 60/s without changing the sale path.

A same-profile 60/s repeat on revision 517e56f with the new timing
fields also **failed**: 2,719/3,600 dispatched, 881 generator drops,
and every dispatched buyer eventually got a unique paid ticket.
Independent p95s were 8.33 s for hold HTTP, 8.28 s for the later
command-durability wait, 8.25 s for payment-initiation HTTP, and
11.22 s for ticket wait. These p95 values come from different buyers
and must not be summed. Kafka lag peaked at 299 then drained; no
API DB-pool rejection or sampled DB lock waiter was observed. The
post-TTL audit and rollback passed. Multiple phases slowed together,
so the next diagnostic should sample API event-loop, DB query/commit
and pool wait durations along with host/RDS resource use. See the
[redacted phase timing](paid-ticket-phase-timing-2026-09-29.json).

The paid pipeline observer now also records redacted mean client-side
DB query, commit, connection-hold and pool-acquisition durations from
per-replica Prometheus counter deltas, plus the highest sampled API
event-loop lag gauge. These are diagnostics, not substitutes for
per-request p95s or Cloud Eye RDS CPU/storage metrics. A further
same-profile stage is required to populate them.

That 60/s repeat on revision 6f8fc8f still **failed**: 2,759/3,600
dispatched, 841 drops and all dispatched buyers eventually ticketed.
The per-API mean DB SELECT was 2.58 ms, commit 2.74 ms, pool
acquisition 1.54 ms and connection hold 25.97 ms. Sampled API
event-loop lag averaged 0.82 ms, with a 31.97 ms peak gauge. These
means do not rule out tails, but are far below the 7.08 s hold HTTP
and 6.68 s payment HTTP p95 seen by buyers. No DB pool rejection or
sampled lock waiter occurred; Kafka lag reached 307 and drained.
Post-TTL integrity and rollback passed. The existing API
`Server-Timing: app;dur` response header can directly split time
inside FastAPI from the remaining client/proxy time on the next
diagnostic. See the [redacted API/DB timing evidence](paid-ticket-api-db-timing-2026-09-29.json).

The generator now extracts the already-emitted `Server-Timing: app;dur`
header on hold and payment responses. It reports how many responses
had the header, API duration p95, and the remaining client-observed
duration p95 for each operation. A read-only check from the generator
ECS confirmed the header survives the Nginx proxy. A same-profile
load stage is still required to locate the long HTTP tails.

A bounded diagnostic trace in the generator now records, for hold and
payment requests, elapsed time before sending HTTP headers, elapsed time
waiting for response headers, and TCP connect time. It reports only
aggregate p95s and sample counts. The installed HTTP client trace hook
was exercised against a local HTTP server; missing trace events are
reported as missing samples rather than inferred as zero. This can
separate generator-side connection-pool wait from Nginx/upstream wait.

The 60/s trace stage on revision c278b31 **failed**: 2,830/3,600
dispatched, 770 generator drops, four order GET 503s and two payment
POST 503s. Hold pre-send p95 was 6.10 s versus 42 ms response-header
wait and 11.91 ms FastAPI time. Payment pre-send p95 was 6.75 s
versus 91 ms response-header wait and 60.44 ms FastAPI time. The
long tail is mainly before the shared generator client sends headers,
not inside FastAPI or the observed Nginx/upstream wait. Only 22/2,830
hold and 37/2,830 payment requests opened new TCP connections, so
connect duration cannot explain the stage-wide p95.

The immediate automated failure audit assumed every dispatched payment
succeeded and failed when two payment POSTs returned 503. A corrected
read-only audit after TTL verified 2,828 paid orders/tickets, two
expired unpaid orders, zero duplicate bookings and empty queues.
Fixture retirement and deployment rollback passed. The strict stage
remains **failed** due to drops and 503s. See the
[redacted transport trace](paid-ticket-client-pool-trace-2026-09-29.json).

Increasing only the shared generator HTTP pool from 500 to 1,000
connections at 60/s on revision b9d8f2a also **failed**: 2,551/3,600
dispatched, 1,049 drops. Hold/payment pre-send p95 was 7.28/7.50 s,
worse than the 500-connection control; response-header waits remained
51/86 ms. All 2,551 dispatched payments were ticketed, post-TTL
integrity and rollback passed, and no checkout 503 was observed. A
larger connection limit alone is not a remedy. Next, split the fixed
60/s offered load across two independent generator processes, each
with disjoint shows and half the original in-flight budget, to test
whether the single-process client is the limit. See the
[redacted pool comparison](paid-ticket-client-pool-headroom-2026-09-29.json).

The synchronized two-process 60/s diagnostic on revision 9ca687f also
**failed**: 2,709/3,600 journeys dispatched, 891 generator drops, 43
order-poll GET 503s and eight payment POST 503s. Each process used a
disjoint fixture and half the original in-flight/HTTP connection budget.
The worse shard hold/payment pre-send p95 was 5.31/4.49 s, compared
with 6.10/6.75 s in the earlier single-process trace; these are
independent runs, not a controlled backend pass. API DB-pool acquisition
failed 53 times, with up to ten requests waiting per replica on a
three-connection pool. Kafka lag peaked at 749 and later reached zero.
The first post-TTL audit caught eight unpaid orders before expiry; a
second read-only audit after expiry verified 2,701 paid orders with
2,701 unique tickets, eight expired unpaid orders, zero duplicate
bookings and all queues drained. Rollback passed. The run remains a
strict failure and cannot establish 60/s, let alone the 83.34/s average
needed for 300,000 paid tickets/hour. See the
[redacted two-process diagnostic](paid-ticket-sharded-generator-2026-09-29.json).

A same-topology two-process 60/s stage with the synthetic command/order
poll interval increased from 0.2 to 1.0 second also **failed**:
2,699/3,600 dispatched, 901 drops, 105 order GET 503s and 19 payment
POST 503s. DB-pool acquisition errors rose from 53 to 135 and Kafka
lag peaked at 864. Observed order GETs were 12,101, compared with
10,872 in the preceding 0.2-second stage; longer in-flight journeys
may have offset the slower cadence. These separate runs do not prove
that polling caused the failure. After TTL, 2,680 paid orders had
2,680 unique tickets, 19 unpaid orders expired, all queues drained,
and rollback passed. Slower polling alone is not promoted. See the
[redacted polling diagnostic](paid-ticket-slower-polling-2026-09-29.json)
and [ADR 0088](../../adr/0088-bound-synthetic-checkout-status-polling.md).

A 10-journey real-PostgreSQL smoke of the single-statement order-read
candidate passed. The subsequent same-profile two-process 60/s stage on
revision b874844 **failed**: 2,714/3,600 dispatched, 886 drops, 35
order GET 503s and 14 payment POST 503s. Mean API DB connection hold
was 24.33 ms versus 29.46 ms in the preceding 0.2-second control, but
57 DB-pool acquisitions still failed and hold-to-ticket p95 was 23.71 s.
Post-TTL exact audit confirmed 2,700 paid orders with 2,700 unique
tickets, 14 expired unpaid orders, no duplicates and drained queues;
rollback passed. The change is not a validated capacity improvement.
See the [redacted order-read candidate](paid-ticket-single-query-order-read-2026-09-29.json)
and [ADR 0089](../../adr/0089-single-statement-order-status-read.md).

The generator transport trace now includes reservation-command and order
status GETs, in addition to hold and payment requests. The next same-profile
60/s diagnostic will show whether those frequent polls are occupying the
shared client connection pools; this instrumentation does not alter the
request schedule, application code or correctness gates.

The four-route transport diagnostic at 60/s on revision 1430f5f
**failed**: 2,603/3,600 dispatched, 997 drops, 125 order GET 503s,
10 payment POST 503s and 151 API DB-pool acquisition errors. The
worst-shard pre-send p95 was 3.34 s for reservation-command GET and
3.73 s for order GET, while response-header waits were 43 and 162 ms.
Hold and payment showed the same multi-second pre-send pattern. This
locates a shared synthetic-client bottleneck before HTTP send, but does
not prove whether pool acquisition, socket management or another client
factor is responsible. The API DB-pool errors are an independent real
backend problem. After TTL, 2,593 paid orders had 2,593 unique tickets,
10 unpaid orders expired, queues drained and rollback passed. The
original sharded aggregate omitted new GET timing fields, so this
report uses per-shard values; the aggregator was fixed afterward without
rerunning the load. See the
[redacted four-route trace](paid-ticket-status-get-transport-2026-09-29.json).


### Paused handoff - 2026-10-03

After the ECS/RDS/DCS restart, existing PgBouncer, Kafka and load-balancer
containers were stopped; they were started and existing API/background services
restarted. Four API replicas and readiness are healthy. RDS SELECT 1 and DCS
PING passed. The point-in-time [backlog audit](resume-backlog-audit-2026-10-03.json)
found zero outbox, refresh, dead-letter, overdue-hold, pending-order,
callback-delivery and reservation-stream backlog, no paid orders without tickets,
and zero Kafka lag across six partitions. No new real-backend load was run.
Production application images remain the reverted `db91fff` baseline; the
`ab51fcc` checkout update adds only diagnostic tooling/tests/ADR, without an
application deployment or topology change. Temporary recovery probes were removed.

The [loopback-only generator control](synthetic-generator-control-2026-10-03.json)
on clean generator revision `ab51fcc` failed at 60 journeys/s for 60 seconds:
3,206/3,600 dispatched and fulfilled, 394 generator drops (10.94%), no HTTP error
outcomes, and no retries. The responder was healthy (31.06 ms handler p95 and
1.03 ms event-loop-lag p95); hold pre-send p95 still reached 2.98 s and order
pre-send p95 2.61 s. These are worst-shard percentiles, not combined percentiles.
This reproduces a generator-side limitation without PostgreSQL/Redis/Kafka and
prevents attributing the previous 60/s drops solely to the backend. The earlier
real API pool errors remain a separate backend issue. Synthetic fulfillment
and uniqueness do not validate production durability or zero double booking.
See [ADR 0090](../../adr/0090-isolate-paid-generator-with-loopback-responder.md).

Executed validation: 209 unit tests passed; diagnostic lint passed. Local Windows
smoke completed eight journeys but failed the responder scheduling-lag gate, so
it was not used for capacity attribution. No integration suite was run in this
resumed session. The 20 s saturation control and higher rates were not run.

Work is paused at the user's request to stop soon. The diagnostic and its child
processes finished, responder connections closed and temporary shard manifests
were removed. No unattended test is scheduled. Cloud services remain running;
the temporary authorized SSH identity remains available for the next session
and must be removed from both ECS accounts when the overall cloud work ends.

Next after explicit resume: profile the generator's per-process CPU/event loop,
HTTPX/httpcore connection acquisition, response-body completion and connection
release against the same controlled responder. Make an ADR before changing the
client/pool/sharding pattern. Compare one change at a time with the same offered
rate, polling and total connection/concurrency budget. Require zero drops and
valid responder timing before repeating real 45/s control and 60/s paid stages
with post-TTL exact audits and queue drain. The short 45/s real pass remains the
latest paid throughput evidence; 300,000 paid tickets/hour is not yet validated.
