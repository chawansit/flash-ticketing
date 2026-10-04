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

The private secondary environment is an export of the actual baseline API runtime settings. Do not copy the broad RDS environment file and expect variable names to match API runtime names: the API consumes DATABASE_URL/REDIS_URL. Rewrite only DATABASE_URL's network host to primary private PgBouncer, preserving user/password/database/connection options. The current API startup/readiness uses PostgreSQL and Redis; it does not require publishing another Kafka broker. Keep all other runtime settings and auth material identical. Never retain raw environment or credentials in public evidence. Image export/import, private env transfer, owner-scoped access and cleanup are still live prerequisites.

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

Inventories and generated route configs stay private under ignored `tmp/`. Fingerprints must be computed on the hosts without printing underlying credentials. Inventory validation checks supplied observations; it does not attest that a remote collection occurred. The current checkpoint has no verified additional host or remote inventory.

Both Nginx arms use four explicit per-container endpoints, least_conn, identical keepalive/timeouts, and proxy_next_upstream off. Derive endpoints from actual container publications; do not assume port assignment order after scaling.

## Remaining integration before load

1. Verify host identity/resources, exact source/image/dependencies, environment parity and primary PgBouncer route. Save normal settings/LB config and define owned secondary resources before changing anything.
2. Adapt paid orchestration to split placement. Collect CPU on both hosts and metrics for all four APIs over the same offered interval, checking replica identity, request distribution and observer completeness. Keep frozen financial audits and global queue/Kafka checks. These adapters are not yet implemented.
3. Prove 100 simultaneous attempts for one fresh seat across both hosts yield one accepted hold and one durable owner; prove replay/payment callback safety. Verify rollback to four primary APIs and the original LB before paid qualification.
4. Run matched control/candidate at60buyers/s300s, same source, fixture shape, generator and budgets, with at most one run per arm. Preserve the control result even if customer availability fails. Missing safety/source/restoration evidence stops the workflow; candidate qualification requires all22 original and10 additional gates. No automatic higher-rate stage.
5. Audit exact post-TTL payment/order/ticket accounting, zero double booking and complete global queue/Kafka drain after each arm. Restore primary settings/LB/four APIs, remove only owned secondary resources and temporary access, and verify recovery. Later refunds or retries cannot erase failed customer gates.

Higher-rate, concentrated single-concert and sustained-hour tests follow a successful comparison with separately sized fresh inventory; they are not executed or automatically scheduled here. 300000 tickets/hour remains unqualified.
