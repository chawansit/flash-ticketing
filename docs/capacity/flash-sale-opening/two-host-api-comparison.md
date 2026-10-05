# Two-host API comparison preparation

[ADR0147](../../adr/0147-fixed-budget-two-host-api-comparison.md) records the decision. [Local validation](two-host-scaling-preparation-2026-10-05.json) records executed checks. This is preparation; no multi-host paid runner or capacity result is claimed.

## Ready now

The offline preparation command creates a fresh bundle under ignored repository `tmp/`. It never connects to cloud, changes services or generates traffic:

```powershell
.venv/Scripts/python.exe scripts/prepare_two_host_scaling.py --output tmp/adr0147-new-preparation
```

The bundle contains a frozen plan, primary Compose override and standalone API-only secondary Compose. The secondary uses `pull_policy: never` with the exact exported baseline image. Both arms pin pool4/shared acquisitions12/payment2/cache0 per API, total16 API connections including eight payment connections, and one primary PgBouncer24/reserve0/client160. Primary API ports8101-8104 and PgBouncer5432 bind only to the verified private IP. Secondary API ports use the same private range. Background services stay on primary and the generator remains independent. No CCE/ELB/read-replica deployment is included.

Neither config is applied by this command. Do not run the old single-host paid runner for candidate qualification: it recreates four primary APIs and its observers omit secondary replicas.

## Infrastructure needed

Supply an additional API ECS matching the primary 4-vCPU/8-GiB class, its public/private addresses, and an SSH access method. Do not reuse the generator. Restrict private secondary-to-primary TCP5432 for the existing PgBouncer and primary-to-secondary TCP8101-8104 for upstream/metrics requests. Keep generator traffic directed at the existing primary private LB8000. Verify routing/security groups rather than changing them implicitly. Do not open these database/API ports publicly. Existing RDS TLS validation and DCS authority stay unchanged.

The private secondary environment is an export of the actual baseline API runtime settings. Do not copy the broad RDS environment file and expect variable names to match API runtime names: the API consumes DATABASE_URL/REDIS_URL. Rewrite only DATABASE_URL's network host to primary private PgBouncer, preserving user/password/database/connection options. The current API startup/readiness uses PostgreSQL and Redis; it does not require publishing another Kafka broker. Keep all other runtime settings and auth material identical. Never retain raw environment or credentials in public evidence. The supplied secondary now has the exact imported image and owner-only environment; installed runtime hashes and rendered environment parity passed. Owner-scoped password access used no temporary SSH key. See the executed checkpoint below. Private PgBouncer publication, two-host API deployment and exact restoration are now safety-qualified; matched paid capacity integration remains pending.

## Fresh observed inventory contract

With an independently collected private inventory, the tool also validates it and emits `nginx.control.conf` or `nginx.candidate.conf`:

```powershell
.venv/Scripts/python.exe scripts/prepare_two_host_scaling.py --inventory tmp/private-observed-inventory.json --output tmp/adr0147-verified-preparation
```

Required fields are defined in `validate_inventory` in the preparation script:

- schema1, arm control/candidate, timezone-aware captured_at within the last300seconds;
- hosts primary/secondary/generator, distinct machine_id_sha256 and RFC1918 private_ipv4; matching API-host vcpus/memory_bytes; secondary non_api_services empty; generator api_processes0;
- baseline_authorities fingerprints captured before the change, background settings, exactly one primary PgBouncer24/reserve0/client160, generator_idle and global_queues_zero;
- four apis with full container IDs, host role/private publication port, exact image/source revision verification, readiness, selected API_SETTINGS and shared baseline fingerprints for other settings, database credentials/name, Redis authority and auth material. Secondary DATABASE_URL must route to primary private PgBouncer; primary keeps its baseline pooler DNS route.

Inventories and generated route configs stay private under ignored `tmp/`. Fingerprints must be computed on the hosts without printing underlying credentials. Inventory validation checks supplied observations; it does not attest that a remote collection occurred. The additional host identity/resources are now verified; a fresh matched-budget four-endpoint runtime inventory still requires the deployment adapter. OS machine IDs are shared across the VM image: machine_id_sha256 must use the verified normalized DMI instance UUID fingerprint, with this provenance recorded.

Both Nginx arms use four explicit per-container endpoints, least_conn, identical keepalive/timeouts, and proxy_next_upstream off. Derive endpoints from actual container publications; do not assume port assignment order after scaling.

## Remaining integration before load

1. Verify host identity/resources, exact source/image/dependencies, environment parity and primary PgBouncer route. Save normal settings/LB config and define owned secondary resources before changing anything.
2. Adapt paid orchestration to split placement. Collect CPU on both hosts and metrics for all four APIs over the same offered interval, checking replica identity, request distribution and observer completeness. Keep frozen financial audits and global queue/Kafka checks. The endpoint-only wrapper and both-host CPU collector are now implemented and locally tested. The CPU collector passed a short passive idle smoke; full cross-host pipeline observation and integration into the paid runner remain unexecuted.
3. Prove 100 simultaneous attempts for one fresh seat across both hosts yield one accepted hold and one durable owner; prove replay/payment callback safety. Verify rollback to four primary APIs and the original LB before paid qualification.
4. Run matched control/candidate at60buyers/s300s, same source, fixture shape, generator and budgets, with at most one run per arm. Preserve the control result even if customer availability fails. Missing safety/source/restoration evidence stops the workflow; candidate qualification requires all22 original and10 additional gates. No automatic higher-rate stage.
5. Audit exact post-TTL payment/order/ticket accounting, zero double booking and complete global queue/Kafka drain after each arm. Restore primary settings/LB/four APIs, remove only owned secondary resources and temporary access, and verify recovery. Later refunds or retries cannot erase failed customer gates.

Higher-rate, concentrated single-concert and sustained-hour tests follow a successful comparison with separately sized fresh inventory; they are not executed or automatically scheduled here. 300000 tickets/hour remains unqualified.

## Supplied-host checkpoint

[Executed preparation](two-host-host-preparation-2026-10-05.json) verifies independent instances, matching Docker dependencies, immutable image, installed runtime modules and private rendered configuration. Primary still has four APIs; secondary has no API containers. No paid traffic was generated.

`scripts/observe_two_host_pipeline.py` wraps the unchanged frozen observer with a fresh validated inventory. Place it and `prepare_two_host_scaling.py` alongside each other; pass `--inventory` and `--frozen-observer` plus the original manifest/output/duration arguments. It preserves worker DNS and financial sampling, adds process-start and business counters from the existing scrape, and rejects restarts/counter resets. Its distribution evaluator requires complete samples bracketing the actual offered interval and traffic on every replica; it does not claim balanced routing or paid throughput.

`scripts/observe_two_host_cpu.py` consumes an owner-only per-host spec (schema1, arm, host_role, instance_uuid_sha256, observed containers with full id/role), a future common UTC `--start-at` within60seconds, fresh output and duration up to300seconds. Primary control expects four APIs; secondary control observes host CPU with no containers. Candidate expects two APIs on each host and no secondary background services. The future runner must launch both collectors concurrently and validate their samples against the actual generator offered window. A shared idle schedule alone cannot satisfy the offered-load gate. No deployment, SSH authentication or paid orchestration is performed by either adapter.

## Executed safety and restoration checkpoint

[Safety qualification](two-host-safety-qualification-2026-10-05.json) passed with100 requests split50 per API host: one accepted hold and99409 conflicts, one durable owner, cross-host hold/payment replay, customer authorization and one paid issued ticket. Three callback deliveries produced one deduplicated callback record. Post-expiry financial audit, zero double booking, global queues and Kafka lag passed. The normal primary runtime/counts were restored; secondary project resources and both runs' credential snapshots were removed.

The [first failed qualification](two-host-qualification-recovery-2026-10-05.json) remains recorded. The adapter now scales absent restore roles to zero, corrects the probe file for the API's non-root user and retains phase/wave/error evidence before teardown.118focused tests/Ruff passed. No matched capacity stage was executed, and no throughput gain or300000tickets/hour is established.

## Integrated runner and first paid attempt (2026-10-05)

The multi-host runner is implemented. It validates fresh live inventory, freezes78 harness files, coordinates shared offered windows, records owned supervisor identities/exit receipts, preserves original22 paid plus10 additional gates, and audits financial/TTL/global queues even when dispatched evidence collection fails. Exact-adapter dryc0dae8f43adc passed both stages, cross-host contention/replay, post-expiry financial checks and exact restoration. Earlier dry failures remain recorded.

[Failed control and recovery evidence](two-host-paid-comparison-2026-10-05.json):18,000 journeys dispatched at60/s for300s;17,999 customer confirmations, one payment503 (0.00556% of dispatched journeys), no generator drops.17,999 successful payments had17,999 durable tickets, one unpaid order expired, duplicate bookings0, pending orders0 and all DB/Redis/Kafka queues drained. Original primary runtime was restored and the generator was idle. Full-completion/financial gates remain failed; zero observed duplicate bookings does not make the whole financial gate pass.

Secondary SSH reset while the runner checked completed CPU jobs. Shared-window CPU evidence was recovered read-only: primary82.299%, aggregate API1.2967cores. Hold HTTP p95 was17.4ms, payment HTTP p9578.6ms, hold-to-ticket p952.212s (worst shard p95, not combined percentiles). Pipeline/Kafka traces were not copied before container teardown and are unavailable; original failed observer/private-cleanup gates remain recorded. Candidate was not started. No capacity improvement or sustained hourly throughput is established.

Follow-up harness corrections configure30-second keepalive on every pinned SSH client and retain raw observer traces in finally before teardown; no remote mutation or customer retry is introduced.155 focused tests and Ruff passed. A[330-second passive SSH idle qualification](two-host-ssh-idle-qualification-2026-10-05.json) passed for all three clients with generator idle and no customer load. This does not qualify a paid run with the corrected harness. A replacement control/candidate pair requires explicit authorization because the original one-control attempt is consumed; do not reset its ledger or relax any gate automatically.

## Completed replacement comparison (2026-10-05)

The user explicitly authorized one replacement pair; the earlier failed attempt and consumed ledger remain archived. Corrected-adapter dry qualification193bc294b81f passed before execution. [Replacement comparison evidence](two-host-paid-replacement-comparison-2026-10-05.json) records run9738b5fd822b. Both arms used the frozen application/generator and identical budgets:60 buyers/s for300s,18000 journeys each, zero customer retries. All32 gates passed per arm.

| Measured result | Four APIs on primary | Two APIs per host |
| --- | ---: | ---: |
| Confirmed durable paid tickets |18000|18000|
| Customer journey errors / drops / double-bookings |0 /0 /0|0 /0 /0|
| Primary offered-window CPU |83.107%|69.562%|
| Secondary offered-window CPU |0.191%|17.573%|
| Aggregate API CPU cores |1.302|1.384|
| Hold HTTP p95 |17.310ms|11.643ms|
| Payment HTTP p95 |62.129ms|59.922ms|
| Hold-to-ticket p95 |2147.031ms|2127.276ms|

Primary CPU fell13.545 percentage points and hold-response p95 improved32.743%; end-to-end p95 improved only0.920%. This is one ordered matched pair, with worst-shard customer p95. Higher throughput and sustained hourly capacity remain unmeasured;300000 tickets/hour requires83.333 paid tickets/s, above the tested60/s. Total API CPU increased about6.27%; more compute improved primary headroom without proving better total efficiency.

The complete observer-lifetime API pool-acquire mean increased2.340 to4.314ms; these means include preparation/audit tail and are not offered-window p95. Internal simulator delivery-error phase calls17/22 and one consumer55P03 per arm were retained despite full payment/ticket completion and empty final queues. Their cause is not established. Zero customer errors must not be reported as zero errors throughout all workers.

Final restoration passed: exact original primary configuration/API4/background placement, secondary APIs removed, generator idle, owned credential snapshots removed, DB/Redis queues and Kafka lag0. No SSH reset interrupted this pair. All capacity authorization is consumed; no further rate or hour stage ran. Next propose a separately authorized bounded rate probe toward83.333+ paid tickets/s, then long steady and flash-opening validation if it passes. No GitHub push or main merge was performed.

## Prepared next rate probe (not executed)

[ADR0148](../../adr/0148-bounded-two-host-84-buyer-rate-probe.md) and [preparation evidence](two-host-rate-probe-preparation-2026-10-05.json) define one two-host probe at84 offered buyers/s for300s:25200 journeys,84 shows of300 seats. The frozen coordinator requires even integer rates, so the proposed85/s cannot run unchanged.84/s gives42/s per shard and12600 unique seats/viewers per shard. Inventory expands from60 to84 shows; this is a capacity probe, not another exact matched latency comparison or synchronized hot-seat sale.203 focused tests/Ruff passed; no new cloud load or configuration change occurred.

After explicit authorization, `scripts/run_two_host_rate_probe.py --execute` first qualifies exact adapters if needed, then conditionally dispatches one paid candidate stage, retains all32 gates plus bounded worker exception evidence, audits/drains/cleans and restores. Dry and paid protocols may each create one separate safety ticket with three duplicate callbacks (maximum two safety tickets total). The separate ADR0148 ledger refuses unauthorized or consumed attempts and preserves ADR0147 history. No automatic replacement, higher rate, hour stage, push or merge.
