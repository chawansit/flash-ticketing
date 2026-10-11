# Reproducing the historical CCE paid-load experiment

This integration depends on backend PR #6. Merge that PR first, then retarget this stacked PR to main and rerun CI. Repository integration changes neither deployment nor capacity. Do not build a new image from main and label it as the historical benchmark image.

## Latest hourly measurement: ADR0283 (12 October 2026)

[Hourly evidence](observer-cost-hourly-result-2026-10-12.json): **302,298 unique paid-and-issued tickets inside one hour (83.9717/s)**; the conservative boundary count is 302,078. All 302,400 offered journeys dispatched and completed, with zero drops or final failures. Thirty-two initial payment 503 journeys recovered (0.01058%); journey p95 was **2.314 seconds**. The [independent full-cohort audit](observer-cost-hourly-terminal-recovery-2026-10-12.json) verified all 302,400 terminal paid tickets, zero payment loss, double-booking and relationship errors, post-TTL durability, both safety fixtures, empty queues and exact runtime restoration. Test pods, helpers and private inputs were removed; total bounded experiment time was 78.02 minutes.

**The target was measured; the original qualification remains FAILED_RESTORED.** Its canonical financial query timed out, and one 2.012074-second DB-statistics gap exceeded the unchanged two-second diagnostic bound. Independent recovery does not turn those original gates into passes. The hourly observer itself had zero SQL errors and 8.035-second maximum cohort age; API and worker CPU captures passed. The same four 1-vCPU/2-GiB APIs, fourteen 250m/512-MiB workers, 24 physical DB connections and pinned application image were used. This distributed 1,008-show simulator workload does not qualify a simultaneous concert opening or maximum capacity. [ADR0283](../../adr/0283-bound-hourly-observer-cost-and-preserve-diagnostics.md) records evidence and limits. Next: complete bounded financial-audit integration and independent statistics sampling; do not increase load from this failed qualification.

## Previous hourly result: ADR0281 (11 October, audited 12 October 2026)

The [hourly result](status-reuse-hourly-result-2026-10-11.json) is **FAILED_RESTORED**: **299,782 unique paid tickets within one hour**, 218 below target. The conservative guarded window contained 299,593. Total after drain was 300,228; that total does not qualify 300,000 inside the hour. Resources, image, connection budgets, gateway delays and recovery matched ADR0280.

Of 302,400 scheduled journeys, 2,172 were undispatched (0.7183%). All 302 journeys with initial errors recovered; final customer failures were zero. Worst-shard journey p95 was 5.568 seconds. Pipeline SQL timed out in 156 samples; hourly CPU/diagnostic coverage failed, so CPU attribution and a causal bottleneck remain unconfirmed.

The [independent terminal audit](status-reuse-hourly-terminal-recovery-2026-10-11.json) reconciled all 300,228 dispatched payments/bookings/tickets, with zero duplicate booking, payment loss or relationship errors. All queues drained and original runtime/cleanup were independently verified. [ADR0281](../../adr/0281-qualify-status-reuse-for-one-hour.md) and [ADR0282](../../adr/0282-bound-hourly-terminal-relationship-audit.md) distinguish failed qualification from successful recovery. Next: correct observer overhead/hourly compatibility and restore complete failure-time coverage, then repeat the same 84/s workload. Do not increase load or claim production qualification from this run.

## Latest measured result: ADR0280 committed status reuse (11 October 2026)

The [fixed-budget comparison](committed-status-reuse-result-2026-10-11.json) passed: **25,200 of 25,200 journeys dispatched and completed**, zero initial/final customer errors, zero double-booking or payment loss, and complete queue drain/restoration. It issued **25,097 unique paid tickets inside 300 seconds (83.6567/s)**; 103 more issued after the window. Journey p95 fell from 11.303 to 4.661 seconds. Reusing fresh committed snapshots avoided 23,133 redundant reads; consumer DB acquisitions fell 22.39%. Image, resources, connections, offered workload, gateway delays and recovery policy were unchanged. The earlier ADR0279 baseline failed quality, explicitly retained below.

Read [ADR0280](../../adr/0280-reuse-fresh-committed-order-status.md) for the exact setting and failure/freshness behavior. This is one passing five-minute run; **300,000 tickets in an hour remains unqualified**. The subsequent hourly attempt above failed; this earlier short pass remains valid within its five-minute scope. The original disabled runtime was restored after the experiment.

## Latest measured comparison — 11 October 2026

The [ADR0279 native database bridge comparison](native-database-bridge-result-2026-10-11.json) issued **24,558 unique paid tickets inside five minutes (81.86/s)**, versus 20,279 (67.60/s) in its retained ADR0278 baseline: **21.1% more in-window tickets**. The shared application image, four 1-vCPU/2-GiB APIs, fourteen 250m/512-MiB workers and 24 physical database connections were unchanged. Only the database TCP bridge implementation changed; the explicitly recorded generator-reporting correction changes no traffic behavior.

Three initial customer errors recovered, zero final customer failures, zero double-booking and zero payment loss were verified. **183 of 25,200 journeys were undispatched**, so quality qualification failed. All 25,017 dispatched journeys ultimately received paid tickets. Independent financial closure and queue drain passed, runtime restored, and no hourly qualification ran. Read [ADR0279](../../adr/0279-native-database-tcp-bridge.md) and the [terminal audit](native-database-bridge-terminal-recovery-2026-10-11.json) for evidence and limits. The historical profile below describes a different pinned experiment; its image/resources/retry settings must not be substituted for this result.

## What is pinned

`reproduction-lock-2026-10-09.json` records the CCE Linux platform image digest, eight historical role image identities, 21 API source hashes, runner/helper/contract input hashes and two fixed workloads. The adapter verifies registry index, platform manifest and Docker configuration separately. API startup verifies copied source, installed imports, loaded bytecode, explicit environment keys and command. ECS transition and recovery verify role image/configuration/start identity, pool budgets, ownership and restoration.

- Short profile: 84 offered journeys/s for 300 seconds, 25,200 scheduled journeys.
- Hourly profile: 84 offered journeys/s for 3,600 seconds, 302,400 scheduled journeys and at least 300,000 unique paid-and-issued tickets inside the exact hour.
- Four API pods, each 1 vCPU/1 GiB. PgBouncer 24 server connections; API pools remain 4 per pod, partitioned 2 general/2 payment. The hourly shared acquisition budget is 20; the retained adapter contract's baseline is 12.
- Two generator shards, concurrency ceiling 500 in total (250 per shard), eight HTTP clients per shard, one-second command polling and no customer retries. Offered journeys, dispatched journeys, HTTP attempts and unique paid tickets are separate measures.
- Historical simulation has no added bank-like latency, one callback delivery in the paid stage and three deliveries in the safety test. Financial confirmation is synchronous in this historical profile.

Frozen generator files are stored under `artifacts/cce-frozen-harness`, validated against the retained 78-file parent manifest, then receive only the hash-checked ADR0226 correction and qualified polling helper. Public historical source-export snapshots are also packaged as hash-addressed text, so parent contracts and migration input checks do not need an old Git object or an ignored temporary source export. Historical source paths are lookup keys only; no old run or scope is recreated. These text assets are inputs, not an alternate application implementation.

## Offline verification

Install the repository's locked dependencies using Python 3.12. From the repository root:

```text
python scripts/check_repository_names.py --all
python scripts/check_cce_reproducibility.py
python -m pytest -q tests/unit
python scripts/run_work_envelope.py
```

The first three commands perform repository/local checks. The last command reports the historical envelope; without `--execute` it makes no cloud calls. CI runs these checks and the Compose backend integration suite.

## Future authorized execution

Use the existing `scripts/run_work_envelope.py` entrypoint with the separately registered `cce_paid_comparison` or `cce_hourly_qualification` profile. Private config, image proof, diagnostic target, original ECS snapshot, SSH runtime, cluster kubeconfig and registry credentials are operator inputs. Do not commit them. Use the exact original platform/role images; missing assets or different topology are blockers, not permission to substitute.

The original unmodified short-control input is archived under `artifacts/cce-historical-reports`; the published report and its provenance remain unchanged. The retained envelope, ledger and compact checkpoint preserve consumed scopes. This PR grants no new cloud scope. Before any execution, record fresh authorization, bind exact configuration/image/manifest/source hashes and use a fresh generated ownership identity. The runner requires preflight and safety qualification before the paid stage; failed controls stop progression. It retains financial audits, zero double-booking, authorization/replay checks, mandatory post-TTL durability, full queue/Kafka drain, owned cleanup and exact restoration. Expired, consumed, paused or uncertain-recovery scopes must reject dispatch.

## What remains unqualified

The historical hour issued more than 300,000 tickets inside the window but remained `FAILED_RESTORED` because of a customer payment failure and observation gates. The trace-size ceiling and sampling coverage are unresolved; this promotion must not waive those gates. The 18,000-seat projection contention issue remains open. The combined backend source in PR #6 has a new identity and needs a fresh measured comparison before any performance claim. No cloud action, capacity improvement or production qualification is established by this PR.

ADR0283: [observer correction validation](hourly-observer-cost-validation-2026-10-12.json) and [same-budget five-minute paid result](observer-cost-paid-result-2026-10-12.json). All25,200 paid journeys completed,zero errors/drops,full diagnostic coverage and restoration;journey p952.58s. In-window issuance25,095;original separate conservative short-window flag retained false. Subsequent hourly measurement and qualification limits are recorded above.
