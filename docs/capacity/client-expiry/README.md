# Client keep-alive expiry: controlled 400 RPS comparison

Completed on 2026-09-09 UTC (2026-09-10 Bangkok). All four 400 RPS runs passed: **480,000 requests, zero HTTP/transport errors and zero drops**. The 2s client expiry stayed within SLO, but opened **2.22x as many TCP connections**. There is no demonstrated error reduction in this workload, so the default remains **5s**; 2s is an opt-in diagnostic setting.

## Results

| Run | Client expiry | Read p95 ms | Hold p95 ms | New TCP | API CPU (cores) | Host busy |
|---|---:|---:|---:|---:|---:|---:|
| A1 | 5s | 12.09 | 49.03 | 331 | 0.646 | 56.44% |
| B1 | 2s | 12.18 | 51.44 | 668 | 0.686 | 62.23% |
| B2 | 2s | 16.57 | 59.60 | 672 | 0.707 | 62.11% |
| A2 | 5s | 16.21 | 54.94 | 272 | 0.687 | 61.86% |

Every run completed 114,000 reads and 6,000 holds. All measured TCP attempts completed. The 5s runs opened 603 connections in total; the 2s runs opened 1,340. API CPU averaged about 0.666 core for 5s and 0.696 core for 2s. Generator process CPU totals were 463.23s versus 456.32s across the respective pairs; these small CPU differences are descriptive, not demonstrated causal effects.

A1 returned 46,291 full read responses (200), versus 54,775 / 54,990 / 55,063 for B1 / B2 / A2; the remainder were conditional 304s. This response-mix difference and time effects limit direct CPU/latency attribution to expiry. The repeat B2/A2 pair had similar full-response counts. No statistically supported error-rate reduction or production fix is claimed.

After expiry drain, all **24,000 acknowledged holds** across **16 worker run IDs** matched distinct holds, orders and idempotency records. Broken links, active/overdue holds and pending orders were zero. Final unpublished outbox, refresh and dead-letter counts were zero.

During measurement, sampled database connections peaked at 12 / 13 / 13 / 13; no lock waiters or missing seat maps were observed at the roughly 2s sampling cadence. Transient expired-but-still-ACTIVE rows peaked at 6 / 6 / 6 / 8, with maximum cleanup lag 0.371s across runs. These sampled observations do not rule out shorter unsampled waits. The first observer sample arrived about 16s after A1 began.

## Cleanup

All test containers were stopped at 17:29:26 UTC and the temporary firewall rule removed. Development manifests were removed from the backend host, API container and generator; generator processes exited. Both ECS VMs and synthetic data remain available. Evidence was transferred through the existing authenticated connection after fresh public SCP logins failed, and the received archive SHA-256 matched the backend.

## Method

Compare client expiry 5s (A) and 2s (B) in A1/B1/B2/A2 order. Each run offers 400 RPS for 300s through four coordinated workers, with 95% availability reads and 5% distinct-seat holds. Use seat offsets 140/180/220/260 on the same 800 shows x 300 seats. Each worker has a 64-connection pool and 10s request timeout. Server keep-alive remains 5s. Both policies use identical bounded transport tracing and no retries.

Hardware: Huawei backend 4 vCPU / 8 GiB, separate generator 8 vCPU / 16 GiB, private network. Backend retains the synchronous service dependency, admission 8, database pool 12 and hold TTL 120s. No application code changes. Production/browser connection policy is outside this experiment.

## Interpretation rules

Latency is the worst of four worker p95 values, not a merged percentile. All HTTP and transport errors, task failures and scheduling drops remain failures. TCP counts are trace events on measured requests only; bootstrap and idle socket closes are excluded. Generator CPU is summed process CPU during measurement. Docker API CPU 100% represents one core; host busy percentage is normalized across all cores and excludes idle/iowait. CPU samples are descriptive, not a causal attribution.

The database observer initially failed because its TEST_DATABASE_URL was absent. It was restarted with the existing configured database URL; the first run has a short observer coverage gap. The failure log and corrected observer output are retained. No load run was discarded or restarted for this issue.

Four sequential runs reduce simple order bias, but cannot isolate all time effects. Zero observed errors would not establish zero risk or the cause of previous ReadError. This is not a maximum production-capacity test, payment stress test or renewed oversell test.

## Validation

Targeted real-server/transport tests: 9 passed in 6.04s. Full unit suite: 52 passed in 18.23s (two existing dependency deprecation warnings). Ruff passed. Application integration tests were not rerun for this generator-only change.

## Evidence and reproduction

The default client expiry remains 5s. Override with `--keepalive-expiry 2 --transport-diagnostics` on the worker or coordinator. Run the coordinator from a directory containing both generator scripts and a private development manifest. Keep manifests out of source control and published evidence.

```sh
python parallel_cloud_load.py --manifest expiry-manifest.json --rate 400 --seconds 300 --seat-offset 140 --output expiry-a1 --transport-diagnostics --keepalive-expiry 5
python run_stages.py
python docs/capacity/client-expiry/summarize.py
```

The retained run_stages.py is experiment-specific: it waits for the separately started A1, then executes B1/B2/A2. Use fresh output paths. It preserves ordinary gate failures and stops on missing result files or unexpected task errors. Existing request timeouts and bounded workload remain active.

[ADR 0025](../../adr/0025-client-expiry-load-comparison.md) records the experiment decision. [ADR 0024 evidence](../idle-boundary/README.md) records the earlier controlled idle-boundary errors.

Retained evidence:

- [Derived summary](summary.json), [local validation](local-validation.md), [stage results](generator/expiry-stages.json).
- [Generator settings and source hashes](generator/expiry-config.json), [exact harness archive](generator/expiry-harness.tgz). Individual worker results and bounded diagnostics are under each `generator/expiry-*` run directory.
- [CPU samples](backend/expiry-cpu-snapshot.json), [database observer](backend/expiry-observer.json), [Prometheus samples](backend/expiry-prometheus.json).
- [Backend identity](backend/expiry-backend-config.json), [settings](backend/expiry-settings.json), [server timeout](backend/expiry-timeout.txt), [verification](verification.json).
- [Durability and expiry check](backend/expiry-durability.json), [cleanup](backend/expiry-cleanup.log).
