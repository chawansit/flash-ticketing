# Capacity validation for work packages 2Ã¢â‚¬â€œ4

## Status and scope

100,000 HTTP requests/second is an unverified future offered-load target, not a demonstrated transaction rate. The new local harness is a bounded smoke/baseline tool, not the distributed generator required for that target. No production scaling changes are included.

[ADR 0007](../adr/0007-capacity-validation.md) records the measurement decision.

## Provisional acceptance targets

These are engineering starting assumptions, not agreed production SLOs:

- Zero duplicate booked seats; one winner for a fresh hot seat within one hold lifetime.
- Independent-seat and payment initiation p95 below 250 ms and p99 below 500 ms.
- Unexpected HTTP/transport errors below 0.1%; independent-seat overload rejections below 1%.
- Hot-seat conflicts are expected; report 409 separately from 429/503 and successful hold latency.
- No generator drops for an accepted capacity result; reconcile offered arrivals with server counters.
- Every accepted successful simulated payment produces exactly one ticket. Drain all three callback deliveries; track per-order fulfillment latency separately before claiming a fulfillment SLO.
- Provisional recovery budget: backlog drains within 60 seconds after a 30-second broker interruption at the tested admitted rate. Validate separately; this budget has not been demonstrated by these runs.

## Workload profiles

| Profile | Generated journey | Expected evidence |
|---|---|---|
| hot | One hold request, distinct actor/key, same fresh seat | One committed hold; other requests conflict or reject |
| spread | One hold request for a distinct fresh seat | Successful hold rate without same-seat contention |
| checkout | Distinct-seat hold, then payment initiation if successful | Two client HTTP requests per successful journey, plus three server-side callback deliveries and Kafka fulfillment |

Do not mix profile rates or count callbacks as client-generator HTTP traffic. For a future blended 100k client HTTP RPS experiment, one provisional mix is 70k hot holds + 20k independent holds + 5k checkout journeys/sec (up to 10k client HTTP requests/sec). This creates up to another 15k callback deliveries/sec. Failures reduce downstream traffic, so measure actual endpoint rates. Pure 100k successful holds/sec is a separate test.

## Reproduce locally

Start the existing Compose services, including the publisher, consumer and simulator. Use a development environment with matching JWT configuration and a migrated database. The generator refuses an unhealthy API before creating fixtures.

PowerShell from the repository root:

~~~powershell
$env:TEST_DATABASE_URL='postgresql://ticketing:ticketing@127.0.0.1:5432/ticketing?connect_timeout=5'
.venv\Scripts\python.exe scripts/capacity_test.py --profile hot --rate 10 --seconds 5 --output docs/capacity/hot-baseline.json
.venv\Scripts\python.exe scripts/capacity_test.py --profile spread --rate 10 --seconds 5 --output docs/capacity/spread-baseline.json
.venv\Scripts\python.exe scripts/capacity_test.py --profile checkout --rate 10 --seconds 5 --output docs/capacity/checkout-baseline.json
~~~

Each run creates its own event/inventory and preserves it for inspection. No automatic business-data deletion occurs. Hot runs must end before hold expiry. Configure the harness hold TTL to match the server. Default maximum in-flight journeys is 50; the local cap is 100,000 scheduled journeys total, not per second. Secrets are read from the environment and are not written into result files.

The scheduler does not queue excess work: arrivals are dropped when the in-flight limit is full or the scheduler is more than max(50 ms, one arrival interval) late. Keep completed-task/results in memory for this bounded tool only. Token preparation and SQL seeding are excluded from elapsed HTTP time. Per-status percentiles use nearest rank; a percentile from one successful hot-seat request is only that one observation.

## Measurement and distributed test plan

1. Establish a healthy baseline; save commit, topology, CPU/RAM, pool limits, Redis/Kafka configuration and fresh fixture IDs.
2. Increase sustained offered load in measured stages (for example 100, 500, 1k, 5k RPS). Use a dedicated environment and minutes-long stages for capacity evidence; do not infer capacity from a five-second sample.
3. Calibrate dedicated distributed generators against a cheap endpoint. Partition independent-seat inventory and actor/key namespaces by generator; share the hot seat deliberately. Synchronize stage start and aggregate time windows. Use a proper distributed arrival-rate tool before approaching 100k RPS; this repository does not yet implement that orchestration.
4. Record API request counters and latency, rejection codes, generator drops/CPU/network, PostgreSQL lock waits and connection saturation, PgBouncer queues, Redis latency, Kafka partition lag and outbox oldest age.
5. Stop increasing load when an acceptance gate fails. Re-run after a measured bottleneck is addressed, recording a new ADR for any architectural change.

Existing Prometheus series include ticketing_http_requests_total, ticketing_http_seconds, ticketing_db_transaction_seconds, ticketing_db_errors_total, ticketing_worker_errors_total and ticketing_outbox_oldest_seconds. Kafka lag, PgBouncer and PostgreSQL saturation require external collection; current Prometheus configuration does not provide those exporters. HTTP latency histograms are route-wide, so use generator per-status latency to avoid rejection bias.

Useful read-only database inspection:

~~~sql
SELECT count(*), min(occurred_at) FROM outbox_events WHERE published_at IS NULL;
SELECT count(*) FROM dead_letters;
SELECT wait_event_type, wait_event, count(*) FROM pg_stat_activity
GROUP BY wait_event_type, wait_event;
~~~

These are instance-wide observations; use isolated environments or correlate run IDs. A final zero backlog does not prove the queue stayed bounded throughout the run.

## Failure validation

Run existing PostgreSQL/Redis persistence rollback tests, multi-process contention tests, payment race/duplicate callback tests and publisher/consumer recovery tests separately. The existing recovery_drill.py covers a local Kafka outage. Extend staged testing to replica loss, ambiguous commit acknowledgement and poison-message replay before production acceptance. Do not describe unexecuted drills as passing.

## Local evidence, 2026-09-06 UTC

See the raw *-baseline.json files for timings and statuses. This is the existing Windows/Docker development deployment, one API process with admission cap 8 and single infrastructure instances. The host is shared, not a dedicated benchmark machine.

The initial *-startup-failure.json files captured 503 DATABASE_UNAVAILABLE while the API readiness endpoint was failing following infrastructure restart. Restarting API/maintenance/simulator restored service. The exact pool-recovery cause was not established. The harness now performs readiness preflight and requires at least one successful hold. The initial spread file's correctness_pass=true used the earlier invariant-only check; it is NOT a capacity pass.

Successful initial hot/spread baselines scheduled 50 journeys each at 10/sec:
- Hot: 1 success, 49 conflicts, zero generator drops; conflict p95 43.49 ms.
- Spread: 50 successes, zero generator drops; success p95 119.81 ms.

Checkout is revalidated with a stronger drain condition requiring all configured callback deliveries, as well as fulfillment. Worker logs showed retryable callback 409s during the earlier checkout run; HTTP generator success alone does not mean downstream processing was error-free.

These samples validate local behavior only. They do not establish sustainable throughput, distributed operation, fulfillment percentiles, or 100k-RPS readiness.


Final checkout baseline: 50 holds, 50 accepted payments, 50 fulfilled orders and 50 tickets; zero duplicate booked seats, zero remaining simulated callback deliveries and zero generator drops. Hold p95 62.00 ms; payment initiation p95 55.43 ms. The drain after HTTP completion took 17.891 seconds; this is not per-order fulfillment p95.

Regression validation: 32 unit/integration tests passed, zero skipped, in 8.96 seconds. Two existing dependency deprecation warnings remain. Ruff and git diff whitespace checks passed. HTTP E2E pytest was not rerun; the checkout baseline exercised real HTTP, simulator, Kafka and ticket fulfillment.

## Staged capacity measurement

See [the longer staged results](staged/README.md) for measured operating points, overload boundaries, checkout backlog and hardware details. The later tests demonstrate why fast API acceptance must not be reported as full-pipeline throughput.

## Optimized implementation, 2026-09-07

See [the matched optimization comparison](optimized/README.md), including Redis freshness, durable refresh debt and a two-minute checkout confirmation. Earlier staged results describe the previous worker implementation.

Latest: [database profiling and incremental projection measurements](incremental/README.md).

Latest read optimization: [300-seat cinema map comparison](seatmap/README.md).

Latest integration: [bounded scheduler and conditional reads](reconciliation/local-integration.md).

Follow-up: [three-minute read and expiry validation](reconciliation/sustained-validation.md).

Earlier measured result: [isolated reconciliation, 352 RPS for 30 minutes](huawei-isolated-reconciliation/README.md). No seat-map warming errors observed; 83 admission rejections remain, so the overall zero-error gate failed.

Latest sustained result: [Redis-validated browse bodies, 352 RPS for 30 minutes](validated-browse-body/README.md): 633,600 completed requests, zero errors/drops; read p95 13.217 ms and hold p95 43.976 ms. This is a verified operating point for the measured workload, not a maximum capacity claim.
