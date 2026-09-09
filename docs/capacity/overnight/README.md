# Contention optimization, mixed load and recovery validation

Status: all four engineering steps executed. Both 30-minute mixed soaks failed the strict error gate; no new sustained error-free production capacity is established. Recovery drills and final durable audit passed.

Two existing Huawei ECSs: backend 4 vCPU / 8 GiB, generator 8 vCPU / 16 GiB, private network. The backend runs API, PostgreSQL, PgBouncer, Redis, Kafka and background workers together. This is a measured single-stack envelope, not a verified production maximum or HA deployment.

## Fixed comparison conditions

800 synthetic shows, 300 seats each; 120s holds, process-local admission 8, database pool 12, client keep-alive expiry 5s and server timeout 5s. Optional protocol/dispatcher probes enabled for both middleware variants. Four generator workers. Warm contention uses 100 lanes for 100 contenders and 128 lanes for 1,000 contenders; failed-total latency includes client lane queueing.

## Contention measurements

| Variant | Contenders | HTTP 201 / 409 / 503 | Failed-total p95 ms |
| --- | ---: | --- | ---: |
| Baseline A | 100 | 1 / 7 / 92 | 85.812 |
| Baseline A | 1,000 | 1 / 55 / 944 | 556.334 |
| Direct ASGI | 100 | 1 / 7 / 92 | 71.461 |
| Direct ASGI | 1,000 | 1 / 44 / 955 | 464.843 |
| Direct ASGI repeat | 1,000 | 1 / 43 / 956 | 495.394 |
| Closing baseline | 1,000 | 1 / 51 / 948 | 575.859 |

Each listed wave has immediate PostgreSQL evidence of one current durable owner. Approximately 15% directional latency improvement is accompanied by slightly more admission rejections; the 200ms failed-response target remains unmet for 1,000 simultaneous contenders. Percentiles from different timing components must not be added. For the two 1,000-contender candidate waves, admission-rejected requests had application p95 about 0.10ms, while admitted 409 conflicts had application p95 215.05/335.11ms. Thus client queueing explains part, but not all, of the missed failed-response target. See evidence/contention-client-components.json.

## Mixed stage results

| Offered RPS | Seconds | Read p95 ms | Distinct hold p95 ms | Failed hot hold p95 ms | Gate |
| ---: | ---: | ---: | ---: | ---: | --- |
| 400 | 300 | 6.762 | 36.211 | 18.177 | Passed |
| 500 | 300 | 17.062 | 58.427 | 31.803 | Passed |
| 600 | 300 | 63.716 | 133.099 | 135.329 | Failed: admission rejections |

At 600 RPS, all 180,000 requests completed with zero generator drops or transport errors, but 84 ordinary holds and 11 hot attempts returned ADMISSION_FULL (95 total; 0.0528% of all requests, 1.056% of hold attempts). Latency gates passed; the no-unexpected-error gate failed. Escalation stopped and 800 RPS was not run. Initial soak rate: 500 RPS. New admission rejections were detected approximately 13 minutes into that soak, so its five-minute pass is not a sustained-capacity pass. The complete 500 RPS soak finished: 900,000 scheduled requests, zero generator drops, 13 ADMISSION_FULL responses (12 ordinary, one hot) and one read transport error. Read p95 was 17.396ms, ordinary hold p95 58.799ms and failed-hot p95 31.798ms. Latency gates passed but the error gate failed. The separate 400 RPS fallback soak is reported below. The ReadError trace used an existing connection and ended in ConnectionResetError errno 104 while receiving response headers; idle-timeout causality is not established.

Three successful hot holds per passing stage are consistent with expiry/reacquisition of the shared seat over five minutes; this is not three simultaneous owners. The final durable interval audit passed across all selected runs.

## Thirty-minute mixed soaks

| RPS | Requests | Read p95 ms | Distinct hold p95 ms | Failed hot p95 ms | Admission rejections | Read resets | Drops | Gate |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 500 | 900,000 | 17.396 | 58.799 | 31.798 | 13 | 1 | 0 | Failed |
| 400 | 720,000 | 10.349 | 42.960 | 22.948 | 0 | 3 | 0 | Failed |

The 400 RPS fallback ran 19:03:20-19:33:20 UTC on 9 September 2026. All 28,800 ordinary holds succeeded, with 15 additional hot holds as the shared seat expired/reopened. There were 7,185 expected hot conflicts. The three read failures occurred on reused connections while receiving response headers and ended in ConnectionResetError errno 104. Their cause is unresolved; no measured request was retried. Reducing load eliminated admission rejection in this finite run but did not establish an error-free operating point. Five-minute 400/500 passes must not be presented as 30-minute passes.

## Durable correctness and recovery

The final read-only audit covered 60 completed worker result files and all **94,748 acknowledged holds**, including candidate/control, failed stages and both soaks. All acknowledged holds had matching idempotency records, orders and hold ownership; all expired after drain. All 94,748 seat intervals were audited with **zero overlaps**. Outbox, refresh backlog and dead letters were zero at the final snapshot. This is measured coverage, not a proof of every possible distributed failure.

The first combined interval query exhausted the container's 64 MiB shared-memory allocation before producing an audit result. A transaction-local setting disables parallel query workers only for this diagnostic; its serial rerun passed. The original failure log is retained. No application memory, persistence or pool setting changed for this recovery.

After load and observers finished, separate bounded outages produced:

| Dependency | During outage | Recovery validation |
| --- | --- | --- |
| Redis | Hold returned 503 ADMISSION_UNAVAILABLE | Readiness in 1.05s after start; failed key had no durable record, retry/replay returned the same hold, competing key conflicted |
| PostgreSQL | Hold returned 503 DATABASE_UNAVAILABLE | Readiness in 13.06s after start; failed key had no durable record, retry/replay returned the same hold, competing key conflicted |
| Kafka | Payment reached PAID, zero tickets, one unpublished OrderPaid event | FULFILLED with one ticket in 10.10s after start; five deliveries of the same callback yielded one callback record, booking, ticket and OrderPaid event |

Post-drill expiry checks found two EXPIRED orders/holds and one FULFILLED order with CONSUMED hold, one booking and one ticket. Global duplicate-booking groups and pending queues were zero. All container automatic restart counters remained zero across the drill; explicit service stops/starts are not counted as automatic restarts. The Kafka drill does not test poison messages, every broker failure mode, or sustained payment throughput.

## Resource evidence

Comparable windows use the final four minutes of each five-minute mixed stage, excluding the first minute so trailing Prometheus rates do not mix stages.

| RPS | Max DB connections | Max sampled lock waiters | Mean host busy % | Mean API CPU % of one core | Max sampled overdue hold age s |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 400 | 13 | 0 | 53.68 | 57.13 | 0.196 |
| 500 | 12 | 0 | 63.14 | 70.33 | 0.320 |
| 600 | 13 | 0 | 68.25 | 83.43 | 2.156 |

API query p95 histogram estimates remained about 4.77–4.89ms; pool-acquisition p95 estimates stayed 4.75ms, close to the first 5ms bucket and therefore not sub-millisecond measurements. Sampled API pool-acquiring gauge stayed zero. Reconciliation batch busy time including I/O averaged 34.64%, 37.57%, and 40.20%; publishing averaged 9.62%, 12.97%, and 15.45%. Do not sum overlapping worker operations or interpret these as CPU percentages.

The short-stage rise in API CPU/admission rejection without proportional connection growth or sampled lock contention supports investigating API scheduling/worker scaling. During the longer 500 RPS soak, an admission-rejection episode coincided with a PostgreSQL CPU sample of 117.23% of one core and host busy 80.23%. A checkpoint was in progress and autovacuum activity was recorded, but these observations do not establish which operation caused the spike. Longer-run behavior also requires investigating shared-host database/background work. Retained server traces localize the slowest successful requests near 18:43:14 UTC to db_exit: roughly 497-507ms, within roughly 548ms application duration. This phase includes transaction commit/rollback and pool return, so it is not a direct fsync measurement. Across 36,003 successful holds, database_body p95 was 26.077ms and db_exit p95 4.511ms, demonstrating why rare tails can fill admission despite acceptable p95. It does not prove PostgreSQL can never become the bottleneck. Resource summaries are retained as evidence/mixed-400-window.json through mixed-600-window.json.

## Invalid comparisons retained

- The first candidate 100-contender wave returned 1 HTTP 500, 7 conflicts and 92 admission rejections, with no HTTP winner. The traceback identified a read-only database transaction. Diagnostics had issued persistent session settings through transaction-pooled PgBouncer. ADR 0027 replaces them with transaction-scoped settings; isolated pool connections were reset and later writes succeeded. This failed wave is retained, not counted as a candidate pass.
- The first uniform 400 RPS control ran immediately after all 800 synthetic sale windows expired. All 6,000 holds returned SALE_CLOSED. It is not reservation-capacity evidence. Only the explicit fixture IDs were reopened for six hours, with before/after evidence and a read-only preflight added.
- Initial DB observer output paths were unavailable/unwritable inside the API image. Failed observer logs are retained; the observer was restarted using /tmp. Control observer coverage is partial.

- The first combined durability report used an incorrect flat-status assumption for contention workers, falsely counting their six winners as zero. The raw report is retained; nested hold/hot-hold accounting is regression-tested and corrected before final verification.

## Uniform control

The reopened-fixture baseline completed 400 RPS for 300 seconds: 120,000 requests, 6,000 successful holds, zero dropped requests, worst-worker read p95 12.973ms and hold p95 60.770ms. Candidate also passed with 6,000 successful holds and zero drops: read p95 11.954ms, hold p95 48.846ms (approximately 20% lower reservation p95). A brief read-only verifier ran during the candidate window; observer coverage is partial for the baseline, so this is a directional comparison, not a statistical confidence interval.

## Validation executed

- Full candidate cloud suite: 114 passed, 2 dependency deprecation warnings (before mixed generator additions).
- Latest local unit suite: 67 passed, 2 dependency deprecation warnings; cache provider disabled.
- Changed code lint and preflight/fault script compilation passed. Corrected post-expiry verification passed across 36 worker results, with 6,006 acknowledged holds/orders and no active/overdue holds. The final all-run audit and recovery drills passed; both long mixed-load runs failed their strict error gate as detailed below.

See [the sequence](PLAN.md), [ADR 0026](../../adr/0026-direct-asgi-instrumentation.md), [ADR 0027](../../adr/0027-transaction-scoped-diagnostics.md), and [ADR 0028](../../adr/0028-mixed-load-and-failure-validation.md).

## Reproduce on the isolated test stack

Run the read-only `scripts/cloud_load_preflight.py` on the backend against an expiring development manifest before every stage group/soak. It verifies only the manifest's show IDs and records open-window/credential validity without tokens. Generate manifests under ignored `tmp/` and transfer them privately; never publish them.

On the separate generator, `scripts/cloud_mixed_stages.py` invokes four workers at 400/500/600/800 RPS for 300 seconds per step. It stops escalation on the first failed gate and falls back to 200 then 100 only if no higher stage passed. `stages.json` records the selected soak rate. Run the same mixed coordinator at that rate for 1,800 seconds with a new manifest and seat offset 180. All measured calls are single attempts; expected hot conflicts are counted separately.

After expiry drain, run `scripts/verify_cloud_holds.py` with the collected worker JSON files. Its interval audit must cover every acknowledged hold; missing coverage fails the gate. The bounded `scripts/cloud_failure_drill.py` then targets only the named `flash-cloud-bench` Compose project and restores each stopped dependency in a finally path. Run no load concurrently with these faults.

The Prometheus export contains histogram estimates and sampled worker/pool gauges. Database query timing includes client/network/pooler time; it is not PostgreSQL-only execution time. Worker busy time includes I/O and is not CPU utilization. Sampled zero lock waiters does not prove no short lock wait ever occurred. CPU samples and fixture reconciliation observations provide complementary evidence.

## Scope and next validation

- This workload uses 800 simultaneously open synthetic shows (240,000 seats), the lower end of 200 locations with four screens each. The 2,000-screen upper end and multiple active showtimes per screen were not measured here.
- Mixed load is 95% availability reads, 4% distinct-seat holds and 1% hot-seat attempts. Payment/outbox recovery is a separate functional drill, not sustained payment-throughput sizing.
- The staircase increased traffic from 400 to 600 RPS (1.5x) before its error gate failed; resilience at 5-10x traffic is not established.
- No 800 RPS stage was run after 600 failed. No 100,000 RPS or production maximum is established by these two ECSs. A passing finite run is an operating point for its measured configuration and workload.
- Follow-up diagnosis should separate database commit/pool-return tails and storage/WAL behavior, then compare API worker scaling with bounded total connections. Preserve durable commit semantics while testing.
- Investigate the reused-connection reset separately, including connection-lifetime settings. Its trace alone does not prove an idle-timeout cause, and retries were not used to hide measured failures.

## Final evidence

See evidence/night-final-load-durability.json, evidence/night-fault-results.json, evidence/night-postfault-durable-state.json and results/night-soak-400/. Request-ID correlation recovered server records for all 4,200 requests in the six valid contention waves; the failed first candidate has 99 correlated records for 100 requests because its unhandled 500 did not return a request ID. Probe snapshots showed at most seven borrowed thread tokens and zero queued thread-token waiters; this does not exclude event-loop/dispatch scheduling delays.


The final 29 minutes of the 400 RPS soak sampled at most 13 DB connections and zero lock waiters, with no missing maps. Mean host busy was 57.71% across four cores; API CPU averaged 60.04% of one core. Maximum sampled overdue hold age was 0.348s. These are sampled observations, not proof of no short lock waits.

## Cleanup completed

Both ECS VMs remain running. Only flash-cloud-bench containers were stopped; data volumes were retained. All five backend and four generator development credential manifests were deleted, including API copies. No temporary generator directories remained, the temporary firewall rule was removed after stopping services, and the opt-in diagnostic probe was disabled in the test override. Evidence: night-backend-cleanup.json and night-generator-cleanup.json.
