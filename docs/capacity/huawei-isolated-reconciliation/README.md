# Isolated reconciliation: 352 RPS for 30 minutes

**No SEATMAP_WARMING errors were observed, but the overall zero-error gate failed: 83 holds returned ADMISSION_FULL.** This is not a verified production maximum.

Measured 2026-09-09 **02:19:20–02:49:21 UTC** (09:19–09:49 Bangkok). Same Huawei machines: backend c6.xlarge.2 (4 vCPU / 8 GiB), separate generator c6.2xlarge.2 (8 vCPU / 16 GiB), private networking. API, database, Redis, Kafka and workers share the backend. Four generators each offered 88 RPS for 1,800 seconds: 800 shows × 300 seats, 8,000 logical viewers, 95% conditional reads / 5% unique-seat holds, no retries.

## Comparison

The [previous diagnostic run](../huawei-diagnostics/README.md) used the same machines and workload. This change isolates reconciliation from dirty refresh and hold expiry. Admission remains 8, map TTL 30 seconds, hold TTL 120 seconds and reconciliation interval 20 seconds.

| Measurement | Shared maintenance | Isolated reconciliation |
|---|---:|---:|
| Completed requests | 633,600 | 633,600 |
| SEATMAP_WARMING reads | 160 | **0** |
| Successful holds | 31,680 | 31,597 |
| ADMISSION_FULL holds | 0 | **83** |
| Worst-worker read p95 | 25.761 ms | **54.340 ms** |
| Worst-worker hold p95, all outcomes | 74.678 ms | **130.332 ms** |
| Maximum sampled reconciliation age | 33.694 s | **21.099 s** |
| Maximum sampled missing maps | 85 | **0** |
| Peak sampled DB connections | 13 | 14 |
| Generator drops | 0 | 0 |
| Overall zero-error gate | Failed | **Failed** |

All 601,920 reads succeeded: 300,756 HTTP 200 and 301,164 HTTP 304. Hold rejections were 0.2620% of hold attempts (0.0131% of all requests). Client and server error totals agree. All accounting checks passed, with no transport/task errors. Start skew was 26.581 ms. Percentiles are worst-worker values, not exact merged percentiles. Worker files include successful-only and rejected hold distributions.

Read and hold p95 remain below the requested 100–150 / 200–300 ms targets, but increased. Freshness improved in this measured run; overall performance did not uniformly improve. This is one sequential comparison with retained database history and 808 tracked events versus 807 previously. Cloud variation and extra process/connection pressure are confounders; no causal attribution or linear capacity extrapolation is justified.

## Diagnostics and correctness

[Observer summary](isolated-observer-summary.json): 890 in-window samples, zero observer errors, zero sampled missing maps or lock waiters. Brief lock waits may occur between two-second samples. Maximum overdue cleanup delay was 0.264 seconds. [Raw observations](isolated-observations.json) include pre/post-run samples; the table uses only the measured window.

[Server phase analysis](isolated-log-analysis.json) covers all 31,597 successful holds. Dispatch p95 increased from 9.883 to 45.218 ms, DB entry from 6.898 to 11.353 ms, and transaction body from 30.062 to 31.514 ms. Dispatch includes validation/authentication and scheduling, not just thread-pool wait. Phase percentiles cannot be added. Next investigate admission occupancy and dispatch spikes alongside CPU/worker activity before changing admission limits.

[Post-drain durability](isolated-durability.json) matched all 31,597 acknowledged successes to distinct persisted holds, orders and idempotency records, with zero broken links. No active/overdue run holds or pending run orders remained. Global unpublished outbox, pending refresh and dead-letter counts were zero. This workload does not qualify payment/Kafka throughput.

[Full cloud suite](isolated-correctness.log): **80 passed**, two dependency warnings. Rebuilt local suite also passed 80 tests. Coverage includes 100 concurrent requests for one seat with exactly one winner, duplicate payment callbacks, and a real reconciler process rebuilding an expired map while maintenance is absent. Correctness results are separate from the load gate.

## Deployment and evidence

Source commit: `a5fa4e7`; [ADR 0016](../../adr/0016-isolated-reconciliation-worker.md). Deployed workers.py hash in [configuration](isolated-config.json) was verified against that commit after normalizing to the archive's CRLF line endings. Existing remote infrastructure configuration was preserved; a Compose override adds the reconciler from the maintenance service definition with the new role command.

- [Generator summary](isolated-352/summary.json), workers [0](isolated-352/worker-0.json), [1](isolated-352/worker-1.json), [2](isolated-352/worker-2.json), [3](isolated-352/worker-3.json).
- [Prometheus](isolated-prometheus.json), [resource snapshots](isolated-resources.log), [vmstat](isolated-vmstat.log), [server summary](isolated-log-analysis.json).
- [Cleanup evidence](isolated-stopped.log): all benchmark services including reconciler stopped. Temporary private API firewall rule removed after stopping. Temporary manifests deleted from generator, backend and API container. Private environment files/data volumes retained; credentials excluded from this evidence.

To reproduce, use the private benchmark harness with the parameters above, deploy both maintenance and reconciler, issue fresh private manifests, start observers before load, and drain at least the 120-second hold TTL before durability checks. Production sizing still requires sustained zero-error runs and separate checkout, failure/recovery and HA qualification.
