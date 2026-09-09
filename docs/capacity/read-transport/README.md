# Read transport diagnostics — failure not reproduced

**The earlier ReadError remains unexplained, not fixed.** Two baseline diagnostic runs on 9 September 2026 completed 240,000 requests without reproducing it. New bounded transport diagnostics are implemented and tested. The inline-service candidate from ADR 0022 remains unadopted.

## Measured outcomes

| Metric | First run | Repeat |
|---|---:|---:|
| Offered traffic | 400 HTTP RPS × 300s | 400 HTTP RPS × 300s |
| Attempted requests | 120,000 | 120,000 |
| Availability reads | 114,000 | 114,000 |
| Successful holds | 6,000 | 6,000 |
| Transport / unexpected HTTP errors | 0 / 0 | 0 / 0 |
| Generator drops | 0 | 0 |
| Read p95, worst worker | 14.53 ms | 18.40 ms |
| Hold p95, worst worker | 54.11 ms | 63.82 ms |
| Workload gate | Pass | Pass |

Each run used four generator workers at 100 RPS, 95% reads / 5% distinct-seat holds. Read timing includes conditional 304 responses; percentile values are worst-worker values, not pooled percentiles. Both runs had the diagnostic flag enabled in every worker. Their failure-example lists are empty because no HTTP transport exception was caught. No trace from the previous failed requests can be reconstructed retroactively.

The [previous service experiment](../service-dependency/README.md) still contains two failed uniform runs, one ReadError each. Neither failure is discarded or converted to a pass. Instrumentation changes timing, so these successful runs cannot prove a connection race has disappeared or establish the cause of those earlier failures.

## Diagnostics implemented

`--transport-diagnostics` propagates from the partitioned coordinator to the HTTP generator. HTTPX trace callbacks retain at most 32 phase/timestamp events per request. At most 20 failures per worker retain the request index, operation, UTC start, elapsed time, whether TCP connect was attempted, and a bounded exception class/errno chain.

The generator records no exception messages, URLs, headers, tokens or bodies. It does not retry, suppress errors, modify pooling/keep-alive/timeouts or alter arrival scheduling. All failures still fail the existing workload gate. Missing TCP-connect activity only means a new connect was not observed for that request; by itself it does not establish a particular connection-reuse failure. Trace execution adds overhead.

A real local reset-on-close socket test verified that an HTTP failure produces a failed transport phase. This proves the diagnostic mechanism works for that local test; it does not prove the cloud failures were resets. Separate tests verify bounded phase retention and removal of secret-bearing exception text.

## TCP evidence and its limits

Snapshots came from the API container network namespace and the generator host namespace. They cover the measured runs plus nearby bootstrap, health/metrics and other traffic. They are not per-request counters.

| API-container counter delta | First interval | Second interval |
|---|---:|---:|
| Established connection resets | 71 | 70 |
| Outgoing RST segments | 125 | 130 |
| Retransmitted segments | 2 | 4 |
| Listen drops | 0 | 0 |
| TCP timeouts | 0 | 0 |

Reset counters increased even though measured requests had no transport errors. Therefore a reset counter increase alone cannot identify or explain the old ReadError. Generator-host retransmission/timeout counters also include other traffic such as SSH and cannot be attributed to the benchmark without connection-level evidence. [Raw snapshots and derived deltas](summary.json) are retained for review.

## Controls, verification and cleanup

Same Huawei backend 4 vCPU / 8 GiB and separate generator 8 vCPU / 16 GiB over private IP. Same 800 shows × 300 seats, synchronous service dependency, admission 8, pool maximum 12, hold TTL 120s and optional server dispatcher/ingress instrumentation. No application code changed. Existing background services ran normally. Client timeout remained 10s, maximum connections 64 per worker and default keep-alive expiry 5s. The earlier runtime snapshot has a different burst-only request-cap guard; it is inactive in these fixed-rate runs.

Fresh uniform seat offsets were 160 and 200. First measurement began around 16:09:30 UTC; repeat around 16:15:49 UTC. The repeat was added because the first did not reproduce the failure. These are diagnostic samples, not a maximum-production-capacity or high-volume payment test.

After expiry, all **eight worker run IDs** matched expected database counts: **12,000 acknowledged holds**, idempotency records and orders, with zero broken links, active/overdue holds or pending orders. Final outbox, refresh and dead-letter counts were zero. This verifies persistence and expiry for distinct-seat holds; no new contention or payment test is claimed.

[Local validation](local-validation.md): 4 targeted tests passed in 5.53s; complete unit suite 45 passed in 14.23s. Ruff passed. The unchanged application integration suite was not rerun in this generator-only investigation. All benchmark services were stopped, temporary manifests on both hosts/API removed, and the temporary firewall rule removed. Server probe setting returned to off. ECSs and synthetic volumes remain available.

## Reproduction and evidence

```sh
python parallel_cloud_load.py --manifest private-manifest.json --rate 400 --seconds 300 --seat-offset 160 --output transport-uniform --transport-diagnostics
```

Use fresh seats and output directories on repetition. Do not publish the private manifest. The exact executed generator scripts are retained under `generator/`. Run `python docs/capacity/read-transport/summarize.py` to validate accounting and reproduce the summary.

- [ADR 0023](../../adr/0023-read-transport-diagnostics.md), [summary](summary.json).
- [First run](generator/transport-uniform/summary.json), [repeat](generator/transport-uniform-repeat/summary.json), with all worker outputs.
- [Generator versions/source hashes](generator/transport-generator-config.json), [backend identity](backend/transport-config.json).
- [Persistence/expiry verification](backend/transport-durability.json), [cleanup](backend/transport-cleanup.log).
- Full API logs remain on the backend ECS. Failure traces contain no cloud examples because this investigation did not reproduce an error.

## Remaining work

Root cause is unresolved. A focused connection-idle/keep-alive experiment or longer bounded read soak with this trace enabled can test the connection-reuse hypothesis. Correlate an actual failed request with transport/server evidence before changing pooling or accepting the retained async-service candidate. The current evidence does not justify calling the issue fixed.

Follow-up: the [idle-boundary experiment](../idle-boundary/README.md) reproduced controlled connection-reuse errors. It supports a mitigation candidate but does not establish the cause of the earlier failures.
