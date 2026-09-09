# Huawei diagnostic rerun: 352 RPS for 30 minutes

**Completed; zero-error gate failed because 160 availability reads returned
SEATMAP_WARMING. All 31,680 holds succeeded.** No production maximum or performance
improvement is claimed.

## Result and comparison

Measured 2026-09-09 **01:29:11–01:59:12 UTC** (08:29–08:59 Asia/Bangkok).
The same 4-vCPU/8-GiB backend and separate 8-vCPU/16-GiB generator were used.
Four workers each offered 88 RPS for 1,800 seconds; the workload remained 800 shows
× 300 seats, 8,000 logical viewers, 95% conditional reads / 5% unique-seat holds.

| Measurement | Previous 30-minute run | Diagnostic rerun |
|---|---:|---:|
| Completed requests | 633,600 | 633,600 |
| Worst-worker read p95 | 24.657 ms | 25.761 ms |
| Worst-worker hold p95 | 71.163 ms | 74.678 ms |
| Successful holds | 31,660 | **31,680** |
| Hold 503 responses | 20 | **0** |
| Availability 503 responses | 0 | **160** |
| Generator drops | 0 | **0** |
| Zero-error gate | Failed | **Failed** |

Rerun reads: 319,838 HTTP 304, 281,922 HTTP 200 and 160 HTTP 503. Error rate is
0.0266% of availability reads (0.0253% of all requests). All worker accounting
checks passed; no transport/task errors. Start skew was 3.799 ms. Percentiles are
worst-worker values, not exact merged percentiles. No retries were generated.
The prior run's rejection cause remains an inference; this rerun directly captured
error codes and request IDs. A different failure appearing does not prove that
instrumentation fixed the old problem or caused the new one.

[Summary](diag-352/summary.json), [worker 0](diag-352/worker-0.json),
[worker 1](diag-352/worker-1.json), [worker 2](diag-352/worker-2.json),
[worker 3](diag-352/worker-3.json), [previous baseline](../huawei-private/README.md).

## What the new diagnostics established

Client error-code totals and [server log analysis](diag-log-analysis.json) both
report **160 SEATMAP_WARMING** errors. All measured server hold logs were HTTP 201;
there were no admission rejections. This does not reproduce the previous run's
hold failure, and is not evidence that increasing the limit would help.

The [in-window observer summary](diag-observer-summary.json) has 891 samples,
zero observer errors, maximum 13 database connections and zero sampled lock
waiters. Brief lock waits can occur between two-second samples.

| UTC observation | Missing fixture maps | Oldest reconciliation age |
|---|---:|---:|
| 01:31:30.481 | 62 | 30.505 s |
| 01:31:32.496 | 85 | 31.743 s |
| 01:31:34.513 | 76 | **33.694 s** |

These were the only missing-map samples during load. The configured cache TTL is
30 seconds. The error code and missing Redis maps establish a projection freshness
failure; they do not establish why the maintenance schedule fell behind. A shared
maintenance worker, transient host/DB delays and deadline distribution need separate
attribution before choosing the fix. Current raw data includes
[Prometheus](diag-prometheus.json), [resource snapshots](diag-resources.log), and
[vmstat](diag-vmstat.log). vmstat began around minute 8 and cannot explain the
minute-2 expiry incident retroactively.

All 31,680 successful holds emitted seven phase timings. These are server-log
samples rounded to milliseconds; the per-phase distributions must not be added.

| Hold phase | p95 | p99 |
|---|---:|---:|
| Dispatch, validation/auth/thread scheduling included | 9.883 ms | 49.174 ms |
| Redis rate limit | 1.977 ms | 4.685 ms |
| Redis shield acquisition | 1.936 ms | 4.206 ms |
| DB pool/transaction entry | 6.898 ms | 15.622 ms |
| Work inside DB transaction | **30.062 ms** | 47.653 ms |
| DB exit: commit/rollback and pool return | 6.326 ms | 9.620 ms |
| Redis shield release | 4.870 ms | 8.551 ms |

The slowest server hold was 275.61 ms, of which dispatch consumed 188.785 ms.
Dispatch is not a pure thread-pool wait metric. These numbers do not demonstrate a
commit bottleneck in this run. [Diagnostic definitions](../hold-diagnostics.md).

## Correctness and environment

[Durability verification](diag-durability.json), executed at approximately
02:01:32 UTC after the 120-second expiry drain, matched **31,680 acknowledged
holds with 31,680 distinct persisted holds and orders** and correct links/owners.
No active/overdue run holds or pending run orders remained. Global snapshots showed
zero unpublished outbox events, pending refresh requests and dead letters.
Maximum sampled in-run overdue cleanup delay was 0.724 seconds.

[Cloud validation](diag-correctness.log): **9 passed**, covering the seven diagnostic
unit cases plus two real end-to-end cases: 100 requests for one seat produce exactly
one durable hold/order; repeated payment callbacks yield one ticket. The full suite
was not rerun on cloud in this turn (78 passed locally before deployment). This
load measures read/hold throughput, not payment capacity or every failure scenario.

[Configuration](diag-config.json): admission 8, pool 12, hold TTL 120 s,
reconciliation interval 20 s, batch 8, budget 500 ms, lease 30 s and seed batch 200.
Application settings, private HTTP path and isolated Compose topology were retained.
807 events were tracked, including seven non-workload fixtures; the prior short-run
inventory also accumulated correctness fixtures. Synthetic database history is
retained, not reset. Instrumentation overhead, fixture history and host variability
mean this is a diagnostic comparison, not a controlled speedup claim.

Core runtime modules match commit `2a6126c` with CRLF line endings:
[hashes](diag-runtime-hashes.json), [verification](diag-source-verification.json).
The Python analysis script was run only after load. Its local test passed and Ruff
passed. Existing ADRs [0014](../../adr/0014-private-cloud-capacity-benchmark.md) and
[0015](../../adr/0015-hold-path-diagnostics.md) cover this run; no locking, TTL,
admission or messaging pattern was changed.

## Next engineering action

Prioritize reconciliation completing within the freshness deadline. Measure its
backlog/deadline distribution and transient DB/host delays from startup onward,
then compare one bounded scheduling or maintenance-isolation change at a time.
Write an ADR before implementation, retaining version fencing and periodic recovery.
Do not merely extend TTL without defining acceptable availability-data freshness.
Keep admission at 8 until evidence supports changing it. Repeat the same sustained
run after the selected fix; only then escalate rates and add simulcast/skew/retries.

## Cleanup and reproduction

The benchmark stack was stopped and the temporary source restriction removed only
after shutdown; [shutdown log](diag-stopped.log). Synthetic credentials were deleted
from both hosts and the API container; no manifests or secrets are in this report.
Synthetic volumes and private local configuration remain for future runs. The
observer was intentionally stopped after the measured window and expiry validation;
the saved snapshot covers the entire load window. No new cloud resources were bought.

Use `scripts/parallel_cloud_load.py` with a fresh private manifest, four workers,
rate 352, duration 1800 and an unused result directory, after all fixture maps are
ready. Export metrics for the exact window, then run
`scripts/summarize_hold_diagnostics.py` against that window's server log and
`scripts/verify_cloud_holds.py` after expiry. Retain worker results and failed gates.
