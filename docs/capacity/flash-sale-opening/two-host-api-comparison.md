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

Next implement the fresh observed inventory and bounded paid runner integration using the qualified deploy/restore and observer adapters. Preserve the frozen application/generator and all22 paid plus10 two-host gates. Run at most one60buyers/s300s control and one candidate after observer/inventory prerequisites pass; do not use the old single-host runner unchanged.
