# Reproducing the historical CCE paid-load experiment

This integration depends on backend PR #6. Merge that PR first, then retarget this stacked PR to main and rerun CI. Repository integration changes neither deployment nor capacity. Do not build a new image from main and label it as the historical benchmark image.

## Latest measured result: ADR0280 committed status reuse (11 October 2026)

The [fixed-budget comparison](committed-status-reuse-result-2026-10-11.json) passed: **25,200 of 25,200 journeys dispatched and completed**, zero initial/final customer errors, zero double-booking or payment loss, and complete queue drain/restoration. It issued **25,097 unique paid tickets inside 300 seconds (83.6567/s)**; 103 more issued after the window. Journey p95 fell from 11.303 to 4.661 seconds. Reusing fresh committed snapshots avoided 23,133 redundant reads; consumer DB acquisitions fell 22.39%. Image, resources, connections, offered workload, gateway delays and recovery policy were unchanged. The earlier ADR0279 baseline failed quality, explicitly retained below.

Read [ADR0280](../../adr/0280-reuse-fresh-committed-order-status.md) for the exact setting and failure/freshness behavior. This is one passing five-minute run; **300,000 tickets in an hour remains unqualified**. Next is an hour at the same measured configuration, not increased load. The original disabled runtime was restored after the experiment.

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
