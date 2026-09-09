# Validated browse bodies: 352 RPS for 30 minutes

**Passed the sustained zero-error load gate: 633,600 completed requests, zero errors
or generator drops.** This verifies one operating point for this workload, not the
maximum production capacity.

## Same-machine cloud comparison

Measured 2026-09-09 **11:40:44–12:10:46 UTC** (18:40–19:10 Bangkok). Backend Huawei
c6.xlarge.2 (4 vCPU / 8 GiB); separate generator c6.2xlarge.2 (8 vCPU / 16 GiB), over
private networking. API, PostgreSQL, PgBouncer, Redis, Kafka and workers share the
backend. Four generators each offered 88 RPS for 1,800 seconds: 800 shows × 300 seats,
8,000 logical viewers, 95% conditional reads / 5% unique-seat holds. No retries.

The change reuses serialized layout/availability bodies only after Redis validates
the ETag. Retention is bounded to 2,048 entries / 32 MiB per process. Admission
remains 8, hold TTL 120 seconds, map TTL 30 seconds and reconciliation interval
20 seconds; worker counts are unchanged. [ADR 0017](../../adr/0017-validated-browse-body-cache.md).

| Measurement | [Previous run](../huawei-isolated-reconciliation/README.md) | Validated bodies |
|---|---:|---:|
| Completed requests | 633,600 | **633,600** |
| Reads returning 200/304 | 601,920 | **601,920** |
| Successful holds | 31,597 | **31,680** |
| ADMISSION_FULL | 83 | **0** |
| SEATMAP_WARMING | 0 | **0** |
| Worst-worker read p95 | 54.340 ms | **13.217 ms** |
| Worst-worker hold p95, all outcomes | 130.332 ms | **43.976 ms** |
| Generator drops | 0 | **0** |
| Overall load gate | Failed | **Passed** |

Read p95 decreased **75.7%** and hold p95 **66.3%** in this comparison. Reads were
301,198 HTTP 200 and 300,722 HTTP 304; every hold returned HTTP 201. All four worker
exit codes were zero and all accounting checks passed. No transport/task errors.
Start skew was 1.688 ms. Percentiles are worst-worker values, not exact merged
percentiles. Failed-reservation latency cannot be measured in a run without rejections.

## Diagnostics

[Server logs](body-log-analysis.json) independently match all 31,680 hold successes
and zero request error codes. Dispatch p95 decreased from 45.218 to **5.592 ms**, DB
entry from 11.353 to **4.242 ms**, and transaction-body work from 31.514 to **21.004 ms**.
Dispatch includes validation/authentication and scheduling; it is not a pure thread
pool wait. Phase percentiles must not be added to estimate a transaction percentile.

[In-window CPU samples](cpu-comparison.json) show mean API CPU 84.27% to 60.76%,
Redis 18.53% to 13.23%, and PostgreSQL 85.39% to 84.97%. Docker CPU percentages are
relative to **one core**, not all four backend cores. These are unweighted sample
means, not continuous profiling, filtered to each run's measured window.
Reduced read work and lower dispatch delays are consistent with the optimization;
one sequential comparison cannot establish exclusive causality. Retained database
history, cloud variation, and 804 tracked events versus 808 previously are confounders.
The same 800 fixture shows were used.

[Cache counters](body-cache-metrics.txt): 245,821 reused bodies, **81.6%** of measured
HTTP 200 availability responses. The serialization counter 56,177 includes 800
bootstrap reads outside timing, leaving 55,377 measured serializations. Client-304
count matches the generator. Retained payload was 14,905,100 bytes across 800 entries
at collection. This is payload size, not total process RSS.

[Observer summary](body-observer-summary.json): 892 in-window samples, zero observer
errors or missing maps, peak 14 database connections, zero sampled lock waiters,
maximum reconciliation age **20.517 seconds**, and overdue cleanup delay **0.329 seconds**.
Brief lock waits may occur between samples. Raw observations include pre/post-run
samples; the summary uses only the measured window.

## Correctness and cleanup

[Full cloud suite](body-correctness.log): **84 passed**, two dependency deprecation
warnings, no skips. The [local suite](local-tests.log) also passed 84 tests with the
new API deployed. Coverage includes 100 concurrent requests for one seat with exactly
one winner, duplicate payment callbacks, expiry/fencing, and encoded-response
race/eviction/outage behavior. These tests are separate from the uniform load test.

[Durability verification](body-durability.json), at 12:12:45 UTC after the TTL drain,
matched **31,680 acknowledged holds** to distinct persisted holds, orders and
idempotency records, with zero broken links. No active/overdue run holds or pending
orders remained. Unpublished outbox, pending refresh and dead-letter counts were zero.

Runtime commit **6d1cde1**. [Configuration and hashes](body-config.json) for API, cache
and workers were verified against that commit using the archive's CRLF line endings.
Remote infrastructure configuration, generator scripts and workload parameters were
retained. Brief SSH disconnection did not stop the detached generator; completed
accounting and zero drops confirm continuous load.

[Cleanup](body-stopped.log): all benchmark services stopped after validation, then
the temporary API firewall rule was removed. Temporary manifests were deleted from
both hosts and the API container; private environment files and data volumes remain.
The observer was intentionally stopped with the API after evidence capture, before
its 35-minute limit. Its cleanup exit 137 is outside measurement, not a load failure.
The resource sampler was also stopped after collection.

Evidence: [generator summary](body-352/summary.json), workers
[0](body-352/worker-0.json), [1](body-352/worker-1.json),
[2](body-352/worker-2.json), [3](body-352/worker-3.json),
[Prometheus](body-prometheus.json), [observations](body-observations.json),
[container resources](body-resources.log), [vmstat](body-vmstat.log).

Reproduce with the existing private benchmark harness, fresh expiring manifests,
all 800 maps warm, and four workers at 88 RPS each for 1,800 seconds. Drain the hold
TTL before durability verification. Keep tests and cleanup outside measurement.
Higher RPS, 5–10× traffic, hot-show/simulcast skew, retry waves, checkout/payment
throughput and HA/failure recovery need separate qualification. Do not extrapolate
to 100,000 RPS or all 200 locations.

## Local component experiment

[Raw component results](component.json) compare browse + JSONResponse against the
encoded path using real Redis and one temporary 300-seat show. Each mode ran 2,000
sequential reads, repeated three times in alternating order. Half send a matching
validator; one seat changes every 20 reads outside timing. Identical version ranges
produce equal total response sizes and status counts.

| Mean across three runs | Previous path | Encoded reuse |
|---|---:|---:|
| Mean component latency | 1.515 ms | 0.410 ms |
| Python CPU per 2,000 reads | 2.366 s | 0.572 s |

Mean latency decreased 72.9% and measured Python CPU 75.8% in this component test.
It excludes HTTP, authentication, database work, concurrent workers, Redis CPU and
mutation time; it does not establish production RPS. The temporary Redis key was removed.
Run `scripts/benchmark_browse_body.py` in the tests image with
`--redis-url redis://redis:6379/1 --output /tmp/browse-body-component.json`.
The script also prints results; use a writable output path or mounted artifact directory.
