# Background optimizations and same-machine comparison

Date: 2026-09-07 UTC

## Result

The optimizations remove the measured background bottlenecks at the tested rates:

- At **5 checkout journeys/sec**, order-creation-to-ticket p95 fell from **20.33 seconds to 0.381 seconds** in the matched 60-second runs (about 53x lower).
- At **50 independent-seat requests/sec**, successful-hold p95 fell from **228.49 ms to 82.03 ms**. The old code failed the 60-second projection-drain check; the optimized run drained event work, refresh requests and cache lag in **0.781 seconds** after its HTTP phase.
- The final build completed **1,200 checkouts at 10/sec over two minutes**, with **0.313-second ticket p95**, zero generator drops and exactly 1,200 tickets. All configured duplicate callbacks and cache work completed.

These are local measured improvements, not production capacity guarantees. The final checkout test demonstrates a higher operating point than the earlier healthy 2/sec test; it does not establish the maximum sustainable rate or justify linear replica sizing.

## Implemented changes

[ADR 0008](../../adr/0008-bounded-background-processing.md) was recorded before implementation and partially supersedes the previous scheduling decisions.

1. **Durable coalesced refreshes:** SeatsChanged commits a refresh generation together with its inbox entry. Maintenance combines repeated requests into one current-state snapshot, with a 250-ms cooldown per event. A token-fenced acknowledgement cannot erase newer generations. Periodic rebuilds remain.
2. **Batched outbox publication:** lease up to 32 rows, enqueue Kafka sends, then mark only broker-acknowledged rows published. Failures retain stable IDs and finite leases for retry.
3. **Bounded callback dispatch:** four simulator threads share the existing pool. Payment leases and callback idempotency remain authoritative. No HTTP request runs under SQL row locks.
4. **Visible projection debt:** new pending-refresh/age metrics and benchmark observations check Redis version against PostgreSQL. Moving work out of the consumer cannot falsely appear as completed cache processing.

Migration [002_seat_refresh.sql](../../../migrations/002_seat_refresh.sql) adds the refresh-request table. PostgreSQL seat ownership, NOWAIT locks, hold TTL, booking uniqueness, API admission cap and database pool sizes are unchanged.

## Matched measurements

Both sides used the same Windows computer, Docker allocation, generator, endpoints, offered rate, duration, inventory size and duplicate-callback count. Services were restarted from existing images for the fresh baseline, then rebuilt for the optimization. Queues were checked before starting the after-runs. No separate load test or test suite ran concurrently with the measured request phases.

| Measurement | Before | After |
|---|---:|---:|
| Checkout workload | 5 journeys/sec, 60 s, 300 seats | Same |
| Holds / payments accepted / tickets | 300 / 300 / 300 | 300 / 300 / 300 |
| Order creation to ticket p95 | 20.330 s | 0.381 s |
| Pending callback deliveries at HTTP end | 242 | 26 |
| Final callback/projection drain | 31.484 s | 29.844 s |
| Reservation workload | 50 requests/sec, 60 s, 3,000 seats | Same |
| Successful holds | 2,844 | 2,992 |
| HTTP 503 responses | 108 | 8 |
| Generator-dropped arrivals | 48 | 0 |
| Successful-hold p95 | 228.491 ms | 82.026 ms |
| Unconsumed run events at HTTP end | 2,000 | 3 |
| New refresh generations pending at HTTP end | Not applicable | 18 |
| Cache version lag at HTTP end | 1 | 15 |
| Projection drain | Failed after 60.640 s | Completed in 0.781 s |

The periodic refresh already kept the old Redis map nearly current despite a large queue of redundant rebuild work. Coalescing reduces that wasted work; it does not guarantee a fresher cache at every instant. The optimized reservation run finished with zero unconsumed events, zero dirty generations and zero cache version lag.

Callback retry tails remain: the simulator deliberately repeats callbacks, and transient conflicts retain a 15-second retry lease. Ticket p95 improved substantially, while completion of the last duplicate callback improved much less. Do not describe all customers or callbacks as completing within the p95 value.

## Higher-rate confirmation

| Workload | Tickets | Ticket p95 | Client HTTP RPS | Generator drops | Final drain |
|---|---:|---:|---:|---:|---:|
| 10 checkout journeys/sec for 60 s | 600 / 600 | 0.461 s | 20.016 | 0 | 15.047 s |
| Final build: 10/sec for 120 s | 1,200 / 1,200 | 0.313 s | 20.007 | 0 | 15.438 s |

The final run had hold p95/p99 of 52.14/63.73 ms and payment-initiation p95/p99 of 34.57/50.96 ms. At the end of its HTTP phase, 1,197 orders were already fulfilled, two run events were unconsumed, one refresh generation was pending and 27 callback deliveries remained. All completed during drain. Zero duplicate booked seats were observed.

Ten checkout journeys/sec means about 20 client API requests/sec plus a nominal 30 callback deliveries/sec; retries can add callback traffic. It is not ten arbitrary requests/sec or a benchmark of real payment-provider latency.

## Correctness and recovery validation

- **44 unit/integration cases passed** on the host, including 12 new integration cases for refresh coalescing, duplicate intent, atomic rollback, overlapping updates, refresh failure before/after Redis write, stale leases, partial publisher acknowledgements and simulator lease/concurrency recovery.
- **46 tests passed, zero skipped**, in the container suite (10.39 s), including a real HTTP/Kafka checkout and a new synchronized **100-request same-seat test with exactly one durable winner**.
- A subsequent refresh-age/fairness refinement resets the age lower bound after completed generations. Its **12 targeted integration cases passed** (3.83 s), and the final two-minute checkout run used that refined build.
- Ruff passed. Two existing dependency deprecation warnings remain.
- The [real Kafka outage drill](kafka-recovery.txt) passed: PAID with zero tickets while the broker was stopped, then FULFILLED with exactly one ticket after restart.
- [Final health](final-health.json): API readiness 200; zero unpublished events, unconsumed events, dirty refresh requests, dead letters and duplicate booked seats.

The Redis write-failure tests inject failure around the real SQL transaction and use a test cache adapter; existing Redis integration tests cover Lua ownership/version fencing. The outage drill uses the actual local Kafka broker.

## Environment and evidence

Intel i7-7700, four physical cores/eight logical processors, about 16 GiB host RAM. Docker exposes eight CPUs and about 7.74 GiB RAM. One API process and one container per worker role. Other application containers remain running. The simulator's bounded thread count changes as part of the optimization; hardware and API/pool limits do not.

[Hardware/settings](environment.json), [summary CSV](summary.csv), individual result JSON files, and [before](before-images.jsonl), [initial optimized](after-images.jsonl), [final](final-images.jsonl) image identities are retained. The .jsonl image files each contain one JSON array record.

The matched runs and initial 10/sec run used the first optimized build. The final build additionally corrected dirty-request age/fairness accounting; its separate two-minute result is preserved. The baseline runtime code predates the optimization; repository changes since its image build were documentation, tests and measurement tools.

This is one matched pair per workload and one longer confirmation, on a shared development host. Retained fixtures, operating-system scheduling and background applications introduce variability. No resets or destructive data cleanup were used to manufacture a better result. Inventory sizes are fixed within each matched pair, but differ across profiles. The new observer runs on both sides and records projection debt and actual cache versions.

## Reproduce

Start the migrated Compose stack and use the settings in the parent runbook. To preserve evidence, choose new output names:

~~~powershell
$env:TEST_DATABASE_URL='postgresql://ticketing:ticketing@127.0.0.1:5432/ticketing?connect_timeout=5'
$env:TEST_REDIS_URL='redis://127.0.0.1:6379/0'
.venv\Scripts\python.exe scripts/capacity_test.py --profile spread --rate 50 --seconds 60 --drain 60 --wait-projection --output comparison-spread.json
.venv\Scripts\python.exe scripts/capacity_test.py --profile checkout --rate 10 --seconds 120 --drain 60 --wait-projection --output comparison-checkout.json
~~~

The benchmark retains fixtures. correctness_pass covers business invariants; projection_drained separately records completion of consumer intent, refresh work and cache convergence. Neither alone certifies production SLOs. Measurements use two-second samples and database inbox differences, not direct Kafka offset-lag telemetry.

Production sizing still needs dedicated hardware, a representative traffic mix and inventory size, repeated longer runs, and failover testing. No 1,000-RPS or 100,000-RPS claim follows from these results.

