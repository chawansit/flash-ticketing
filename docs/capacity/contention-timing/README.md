# Contention connection and generator timing

Executed 2026-09-09 against unchanged backend source **6d1cde1**. Connection reuse and generator parallelism materially affect measured client latency. They do **not** establish a backend capacity improvement: admission rejections increased with more concentrated arrivals, and every 1,000-contender wave still exceeded the failed-response target below 100–200ms.

## Results

Values below are the range of two independently measured p95 values, not an average or a merged percentile across repetitions. Within each wave, percentiles merge actual request samples from all workers. Total latency starts at each worker's release barrier and includes dispatch, lane waiting and the HTTP request. Maximum measured worker start skew was below 1ms.

| Contenders | Connection mode | Processes | Failed total p95, ms | ADMISSION_FULL per wave |
|---|---|---:|---:|---:|
| 100 | cold | 1 | 202.3–202.8 | 92–92 |
| 100 | warm | 1 | 114.7–119.4 | 92–92 |
| 100 | cold | 4 | 106.1–107.8 | 92–92 |
| 100 | warm | 4 | 68.9–78.6 | 92–92 |
| 1,000 | cold | 1 | 1308.6–1390.1 | 851–873 |
| 1,000 | warm | 1 | 1220.5–1229.9 | 774–811 |
| 1,000 | cold | 4 | 512.1–537.0 | 952–957 |
| 1,000 | warm | 4 | 468.8–480.2 | 944–950 |

All **16 waves** produced exactly one HTTP winner and one verified live PostgreSQL owner. Across **8,800 requests**, responses were 16 successful holds, 936 expected seat conflicts and 7,848 `ADMISSION_FULL` responses. No transport errors occurred. The no-admission-rejection availability gate failed in every wave. Warm comparisons all verified the original local connection port and zero measured TCP connects.

## What the timing supports

For 1,000 warm contenders, one process had lane-wait p95 **1,045–1,051ms**, while four processes had **436–437ms**. Maximum worker event-loop lag was **67–73ms** versus **11–17ms**. The single process consumed approximately 1.24–1.25 CPU seconds in a roughly 1.25-second wave. Together these show a material generator scheduling/processing contribution under this bounded-lane experiment.

Cold TCP-connect trace p95 for 1,000 contenders was **38–64ms** with one process and **15–16ms** with four. Warm waves made zero new connects. TCP trace duration includes event-loop scheduling; it is not pure wire RTT. At 1,000 contenders, removing connection establishment alone did not remove the much larger client queue delay.

The warm four-process total p95 of **469–480ms** was lower than the one-process **1,220–1,230ms**, but it also rejected **944–950** requests instead of **774–811**. Response composition changed, so this cannot be interpreted as a pure backend speedup. Faster release concentrates traffic against the unchanged admission limit of eight.

`Server-Timing` matched server log duration exactly for all 8,800 request IDs, with no missing or mismatched statuses. Each sample retains app duration, TCP duration, time before first client I/O, send-to-response-headers duration, lane wait, dispatch time and total duration. The per-request client-minus-app residual includes networking, client scheduling, response handling and work before middleware. **Server ingress wait is not independently isolated** by that subtraction. Percentiles of different components must not be added or subtracted to infer a phase percentile.

## Controls and limitations

- Same Huawei backend: 4 vCPU / 8 GiB, with PostgreSQL, Redis, Kafka and workers on the ECS. Generator: separate 8 vCPU / 16 GiB ECS, private network.
- Same 800 shows × 300 seats and background reconciliation. Hold TTL 120s, admission eight, pool maximum 12 and Uvicorn limit 256 unchanged.
- Total lanes were `min(contenders,128)`: 100 or 128 for the two groups. Each lane owns one HTTP/1.1 client and connection, with explicit measured queueing. Cold lanes connect on their first request and may reuse the connection for later queued requests. Warm lanes call `/health/live` before the measured barrier.
- Compare cold/warm × one/four processes, then reverse order for repetition two. Distinct viewers within each wave, fresh seat per wave (S200–S207 and S220–S227), no retries. Warm-up and process creation are excluded; request waiting after release is included.
- This differs from the earlier 1,000-connection shared-pool workload. The earlier 3.64-second result is not an apples-to-apples baseline for the new bounded-lane result. Only two repetitions per case; no sustained capacity or statistical certification.
- The host TCP counters include non-benchmark/SSH activity and are in the host network namespace, not the API container namespace. They showed zero host listen drops but nonzero retransmit/timeout counters; these cannot establish whether the API ingress path lost packets or queued requests.

## Backend correctness and recovery

Immediate checks found exactly one active hold/order on every contested seat, matching its HTTP winner and idempotency result. After expiry, **all 40 worker run IDs**, including workers with zero successful responses, matched the expected persisted counts. All 16 acknowledged holds expired, with zero broken links, active/overdue holds or pending orders; outbox, refresh and dead-letter queues were zero.

The 2-second observer sampled 9 and 14 times during the 100- and 1,000-contender groups. Peak database connections were eight, with no sampled lock waiters or missing maps. Maximum reconciliation age was 20.16s. Short lock waits can be missed; these brief snapshots do not establish a database capacity limit.

## Evidence and reproduction

- [100-contender waves](timing-100/wave-0-cold-1p/summary.json), [1,000-contender waves](timing-1000/wave-0-cold-1p/summary.json): all eight wave directories per group retain per-worker request JSON and merged summaries.
- [Exact executed harness](contention_timing.py), [runtime/backend settings](timing-config.json), [client/server correlation](timing-correlation.json), [final durability](timing-final-durability.json), [observer summary](observation-summary.json), [TCP counter context](timing-tcp-delta.json).
- Immediate ownership results: `timing-100-ownership/` and `timing-1000-ownership/`. Prometheus exports and compact server log analyses are alongside this report; raw server logs remain on the ECS.
- [Two real-socket local tests passed](local-validation.md); Ruff passed. Full application suite not rerun because the backend was unchanged.
- [ADR 0019](../../adr/0019-contention-latency-attribution.md).

From the generator directory, using a private development manifest:

```sh
.venv/bin/python contention_timing.py --manifest timing-manifest.json --output timing-100 --contenders 100 --seat 200 --matrix
.venv/bin/python contention_timing.py --manifest timing-manifest.json --output timing-1000 --contenders 1000 --seat 220 --matrix
```

Use fresh output directories and seats; verify live owners before 120-second expiry and durable counts after drain. Credentials are not included in the repository. Benchmark containers/observer were stopped, private manifests deleted and the temporary firewall rule removed; ECS instances and synthetic volumes remain available.

## Next engineering step

Use the verified multi-process generator when investigating backend behavior, retain total latency and all admission rejections, and instrument the server/container ingress boundary before attributing the remaining outside-app time to a server queue. Then evaluate seat-conflict handling versus admission limits with a correctness-preserving change and a new ADR. No admission limit, booking algorithm or production sizing recommendation changed in this experiment.
