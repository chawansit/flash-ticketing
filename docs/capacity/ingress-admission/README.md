# Protocol ingress and admission experiment

**Decision: retain admission 8.** Admission 16 reduced overload rejections but did not improve the total latency of hot-seat contenders. Both settings passed the separate 400 HTTP RPS uniform workload. This experiment does not establish maximum production capacity.

## Measured results — 9 September 2026

| Workload / metric | Admission 8 | Admission 16 |
|---|---:|---:|
| Uniform load, each configuration | 400 RPS × 300s | 400 RPS × 300s |
| Measured HTTP requests | 120,000 | 120,000 |
| Successful distinct-seat holds | 6,000 | 6,000 |
| Availability read p95, worst worker | 15.79 ms | 9.98 ms |
| Hold p95, worst worker | 51.96 ms | 46.39 ms |
| Unexpected responses / generator drops | 0 / 0 | 0 / 0 |
| Hot seat, two waves of 1,000: 201 / 409 / 503 | 2 / 77 / 1,921 | 2 / 230 / 1,768 |
| Failed contender total p95, wave 1 / 2 | 611.18 / 560.23 ms | 649.27 / 680.72 ms |
| Protocol callback → ASGI p95, wave 1 / 2 | 41.39 / 29.87 ms | 38.33 / 38.71 ms |
| Generator lane wait p95, wave 1 / 2 | 573.59 / 507.82 ms | 597.89 / 627.78 ms |
| One live PostgreSQL owner per contested seat | Pass, both waves | Pass, both waves |

Admission 16 converted 153 of 2,000 responses from `503 ADMISSION_FULL` to seat conflicts, but the failed-contender total p95 was higher in both measured waves. Neither setting meets the **<=200 ms failed-reservation total-response target** under this burst. A conflict and an overload rejection are both included in failed-response latency. Individual percentile columns describe separate distributions and must not be added or subtracted.

Uniform requests were 95% availability reads and 5% fresh-seat holds (380 reads/s and 20 holds/s). Read percentiles include conditional 304 responses. There were 114,000 reads per setting. Uniform values are worst-worker percentiles, not pooled percentiles. Two burst repetitions and one five-minute uniform run per setting are insufficient to establish a statistically reliable performance improvement. Configuration 8 ran first; database/cache history can affect the second run.

## Scope and timing boundaries

Same Huawei backend (4 vCPU / 8 GiB), separate generator (8 vCPU / 16 GiB), private network and 800 shows × 300 seats. PostgreSQL, Redis, Kafka and background workers share the backend ECS. Same ownership code at base commit `6d1cde1`, pool maximum 12, hold TTL 120s, Uvicorn concurrency limit 256 and pinned Uvicorn 0.52.4 / httptools 0.8.0. Only the admission setting differs between the compared configurations; both use the same new opt-in timing adapter.

The adapter timestamps headers-complete before the ASGI task is scheduled. `protocol_queue` measures that callback to ASGI entry, including event-loop/pipelined scheduling. `asgi_headers` measures ASGI entry to the response-start hook. Existing `app` timing remains present. Network/kernel waits before headers-complete and response transmission after the hook are outside these server measurements. `outside_protocol_ms` is a per-request client residual, not a measurement of the network alone.

All 4,000 measured contenders had protocol timing. Full captured API logs correlated 123,269 request/ingress pairs at admission 8 and 123,235 at admission 16, with no missing ingress records, missing stamps or status mismatches. These log totals also include warm-up, manifest/bootstrap and health/metrics traffic; they are not the measured workload counts.

Each hot wave used four generator processes, 1,000 distinct viewers, 128 total persistent connection lanes and a fresh seat (S240–S243). Warm-up is excluded; waiting for a generator lane after release is included in total latency. There were no retries. Do not compare these timings directly with older uninstrumented or 1,000-connection runs.

## Database and background processing

The [operational summary](operational-summary.json) filters the 2-second observer to each uniform interval (149 samples each), and uses first/last Prometheus counter deltas for approximate scrape-aligned means:

| Uniform-load metric | Admission 8 | Admission 16 |
|---|---:|---:|
| API pool acquisition mean | 0.027 ms | 0.022 ms |
| API query mean | 1.10 ms | 0.67 ms |
| API transaction mean | 17.47 ms | 11.07 ms |
| Maximum sampled PostgreSQL connections, all observed clients | 14 | 14 |
| Maximum sampled lock waiters | 0 | 0 |
| Maximum reconciliation age | 20.65 s | 20.44 s |
| Maximum sampled expiry cleanup delay | 0.30 s | 0.38 s |

These means are not p95, and sampling can miss short locks. The API pool maximum is per process, whereas observed database connections include workers and the observer. No missing seat maps or maps without TTL were sampled. Brief overdue ACTIVE rows occurred during asynchronous cleanup; after drain none remained. Worker operation busy-time ratios are retained in the summary and raw metrics; overlapping operations must not be summed as CPU utilization. These observations do not prove linear scaling at 5–10× load.

## Correctness and recovery

Immediate PostgreSQL checks found exactly one active hold/order per contested seat matching its HTTP winner. After expiry, [all 24 worker run IDs](backend/ingress-final-durability.json), including workers with no successful holds, matched expected database records. **12,004 acknowledged holds** had matching idempotency records, holds and orders, with zero broken links, remaining active/overdue holds or pending orders. The final outbox, refresh and dead-letter queue snapshot was zero.

The existing cloud test image passed **84 tests in 12.97s**, including integration/concurrency and duplicate-payment processing checks, after load measurement. New adapter and harness tests ran locally; see [local validation](local-validation.md). This read/hold load is not a high-volume payment or Kafka capacity test.

Benchmark containers and the observer were stopped, temporary manifests on both ECSs deleted, and the temporary source firewall rule removed. ECS instances and synthetic volumes remain available. The default deployment stays at admission eight; the diagnostic adapter remains opt-in.

## Evidence and reproduction

- [ADR 0020](../../adr/0020-ingress-and-admission-experiment.md), [optional Compose override](../../../compose.ingress-benchmark.yaml).
- [Baseline hot wave](generator/ingress-8-hot-0/summary.json), [candidate hot wave](generator/ingress-16-hot-0/summary.json); sibling directories retain both waves, raw per-request timing and worker results.
- [Baseline uniform](generator/ingress-8-uniform/summary.json), [candidate uniform](generator/ingress-16-uniform/summary.json).
- [Baseline runtime hashes/settings](backend/ingress-8-config.json), [candidate settings](backend/ingress-16-config.json), [cloud tests](backend/ingress-cloud-tests.log), [cleanup](backend/ingress-cleanup.log).
- `backend/` retains all four live ownership checks, full observer samples, Prometheus exports and compact correlated-log analyses. Raw API logs remain on the backend ECS.
- `generator/` retains the exact executed generator scripts. Private credentials/manifests are excluded. Run `python docs/capacity/ingress-admission/summarize.py` to regenerate the operational summary.

Apply the opt-in Compose override with the base Compose file and private-network overrides, then set `EXPERIMENT_ADMISSION=8` or `16` explicitly. Use fresh output directories and seat IDs:

```sh
python contention_timing.py --manifest private-manifest.json --output ingress-8-hot-0 --mode warm --workers 4 --contenders 1000 --seat 240 --require-ingress
python parallel_cloud_load.py --manifest private-manifest.json --rate 400 --seconds 300 --seat-offset 0 --output ingress-8-uniform
```

Repeat with seat 241, then admission 16 with seats 242/243 and uniform offset 40. Check live ownership before 120s and persistence after expiry. Keep backend and generator code identical across settings. Preserve private-network restrictions when exposing the benchmark API.

The next useful experiment is to isolate the long admitted hot-seat requests and evaluate a bounded conflict path while preserving idempotent replay and PostgreSQL ownership. Raising the global admission limit alone is not supported by this evidence.

Follow-up: [per-request failed-hold attribution](../hold-attribution/README.md) identifies pre-handler dispatch as the next diagnostic target; authentication and worker queue time are not yet isolated.
