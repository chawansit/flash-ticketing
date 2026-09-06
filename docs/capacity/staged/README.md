# Measured local capacity — 2026-09-06 UTC

## Answer

On this computer, with the current single-process application configuration:

- **75 hot-seat attempts/sec** was the cleanest longer contention result: 90 seconds, no generator drops, 0.19% overload responses, conflict p95 64 ms. Exactly one hold succeeded.
- **About 50 successful independent-seat holds/sec at the API boundary** was demonstrated for 60 seconds, with success p95 111 ms and 0.43% overload. However, **2,091 seat-change events remained unconsumed** at completion. This is not a sustainable full-pipeline capacity claim.
- **2 complete checkout journeys/sec** was the healthier tested end-to-end point: 120 orders over 60 seconds, 120 tickets, order-creation-to-ticket p95 0.90 seconds, small sampled queues. Each journey generates two client requests and three simulated callback deliveries.
- At **5 checkout journeys/sec**, HTTP acceptance stayed fast but fulfillment fell behind: p95 20.49 seconds and 39.33 seconds of post-load drain. All 300 orders eventually received tickets.

These are tested operating points and failure boundaries, not the maximum capability of the hardware. The sustainable independent-seat rate including cache/event processing has **not** been established. The checkout boundary lies somewhere between the tested 2/sec healthy point and 5/sec backlog-building point; intermediate rates were not tested. Nothing here demonstrates 100,000 RPS.

## Environment

Intel Core i7-7700, 4 physical cores / 8 logical processors; 15.97 GiB host RAM. Docker: 8 CPUs and 8,308,514,816 bytes (about 7.74 GiB) RAM. Other application containers were running. See [recorded configuration](environment.json).

One API process with admission cap 8; one publisher, consumer, simulator and maintenance process. PostgreSQL, Redis and Kafka are single instances. Generator runs on the Windows host against the Docker-published API port. Generator and application share CPU, memory and network resources. Maximum generator in-flight journeys: 50.

Baseline application commit: ce12967. The measurement changes in this report add two-second database backlog observations and order-to-ticket latency; they do not tune or scale the application.

## Measured stages

All rates in the offered column are journeys/sec. Hot/spread journeys generate one client request; successful checkout journeys generate two. Callback traffic is excluded from the completed client HTTP RPS column.

| Profile | Offered / sec | Duration | Completed client HTTP RPS | Hold successes | HTTP 503 hold responses | Generator drops | Relevant hold p95 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Spread | 25 | 30 s | 25.00 | 750 | 0 | 0 | 81.62 ms |
| Spread | 50 | 30 s | 49.99 | 1,499 | 1 | 0 | 73.48 ms |
| Spread | 100 | 30 s | 79.60 | 2,087 | 342 | 571 | 853.32 ms |
| Hot | 200 | 30 s | 95.90 | 1 | 34 | 3,113 | 271.61 ms |
| Hot | 100 | 60 s | 99.61 | 1 | 81 | 0 | 97.33 ms |
| Hot | 75 | 90 s | 75.00 | 1 | 13 | 0 | 64.04 ms |
| Checkout | 2 | 60 s | 4.03 | 120 | 0 | 0 | 55.13 ms |
| Checkout | 5 | 60 s | 10.03 | 300 | 0 | 0 | 61.20 ms |
| Spread confirmation | 50 | 60 s | 50.01 | 2,987 | 13 | 0 | 110.60 ms |

Hot p95 refers to expected HTTP 409 conflicts. Other rows refer to successful HTTP 201 holds. A successful hot hold is a single observation and cannot establish a meaningful success percentile.

[Machine-readable summary](summary.csv) and individual JSON files preserve the results and time-series observations. No unexpected client HTTP status or transport error occurred in these stages. Every hot run had exactly one durable hold. Both checkout runs had zero duplicate booked seats and drained every configured callback delivery.

## HTTP success does not imply pipeline capacity

The 50-RPS confirmation wrote 2,987 holds to PostgreSQL but had 2,091 run events absent from the consumer inbox at the final observation. The publisher's outbox was empty then: publication had caught up while consumption had not. This rules out calling the entire system sustainable at 50 successful holds/sec under this workload.

At 100 offered spread RPS, the generator missed 571 arrivals, 14.08% of dispatched requests received 503, successful-request p99 was 1.30 seconds, and oldest unpublished outbox age reached about 9.13 seconds. This fails the provisional HTTP and generator gates.

At 200 offered hot RPS, the generator missed 51.88% of arrivals. The roughly 96 completed RPS is therefore a limit of this combined test setup under that stage, not a calibrated server-only ceiling. At 100 hot RPS the generator kept up, but 1.35% overload exceeded the less-than-1% comparison gate used for the lower 75-RPS point.

## Checkout processing

| Offered checkouts/sec | Tickets | Order creation to ticket p95 | Maximum sampled pending callback deliveries | Post-HTTP drain |
|---|---:|---:|---:|---:|
| 2 | 120 / 120 | 0.896 s | 14 | 14.265 s |
| 5 | 300 / 300 | 20.494 s | 226 | 39.328 s |

The final load-phase sample at 5/sec showed 296 orders, 226 fulfilled orders and 226 pending callback deliveries. The queue grew during load rather than staying near zero. At 2/sec the equivalent sample showed 119 orders, 117 fulfilled and 14 pending callback deliveries.

Post-load drain includes duplicate callback completion; it is not individual customer latency. The simulator uses a 15-second retry lease, so a drain tail can include retry delay even when ticket latency is low. Both checkout stages began after earlier event work had drained. Callback retries are not counted in client HTTP metrics; the requested three deliveries per payment are a minimum logical delivery count, not a count of every HTTP retry.

Order-to-ticket timing uses PostgreSQL order.created_at and ticket.issued_at. It includes order creation through payment and fulfillment; it is not a gateway-only or end-user network latency measurement.

## Interpretation and limitations

The consumer rebuilds the full event seat snapshot for every SeatsChanged event, and the simulator dispatches callbacks serially. These are concrete optimization candidates supported by the code and observed queue growth; this benchmark does not prove either is the sole hardware bottleneck.

Each stage uses a fresh event. Spread/checkout inventory size equals scheduled journeys; hot inventory has one seat. Inventory sizes therefore vary between stages (750, 1,500 and 3,000 for spread tests). Full-snapshot cost depends on inventory size. The 100-RPS and longer 50-RPS spread stages both used 3,000 seats, but differ in test duration and background work.

Earlier expired holds generate additional events and retained fixtures enlarge periodic refresh work. Hot stages overlapped recovery from earlier spread traffic. Read [drain observations](drain-observations.jsonl) alongside stage files. The final spread run also leaves asynchronous work and subsequent expiry events; this report does not claim an empty queue immediately after stress.

The initial stages sampled only global unpublished outbox backlog. Per-run unconsumed-event counts were added before the longer hot and checkout stages. Blank CSV values mean unmeasured, not zero. All recorded observer samples completed without errors. Inbox differences are a database proxy for pending processing, not Kafka consumer-offset lag. Two-second sampling may miss short spikes.

The 30–90-second tests establish local bounds, not a production soak result. No replicas were added, no pool settings were changed, no existing business data was deleted and no external services were stressed.

## Reproduce a stage

Use the setup in [the parent runbook](../README.md), then run, for example:

~~~powershell
.venv\Scripts\python.exe scripts/capacity_test.py --profile spread --rate 50 --seconds 60 --output docs/capacity/staged/spread-50-confirm.json
.venv\Scripts\python.exe scripts/capacity_test.py --profile checkout --rate 2 --seconds 60 --drain 60 --output docs/capacity/staged/checkout-2.json
~~~

Use different output names to preserve the original evidence. Wait for prior event work to drain when comparing isolated stages. Use fixed-size inventories, dedicated generators and longer steady-state runs for a tighter sustainable-capacity estimate after addressing the observed queue bottlenecks.

The JSON correctness_pass flag checks business invariants and final delivery, not performance acceptance. A true value must never be read as a throughput/SLO pass.


## Final checks

Nine stages issued 24,156 client HTTP requests. Ruff and whitespace checks passed. No application behavior changed, so the application regression suite was not repeated for this measurement-only turn; the real HTTP stages exercised the updated observer. At 16:26:47 UTC the API readiness response was 200, unpublished outbox count was zero, 570 events remained unconsumed, dead-letter count was zero and the database-wide duplicate booked-seat count was zero. Workers remained running to drain expiry and seat-change work. See timestamped drain observations for subsequent state.
