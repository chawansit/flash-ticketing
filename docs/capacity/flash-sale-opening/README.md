# Flash-sale opening workload

Status: first Huawei opening control measured; end-to-end ticket
capacity are **not yet measured**. This profile changes only the load generator and
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
