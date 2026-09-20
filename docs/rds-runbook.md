# Huawei RDS PostgreSQL validation runbook

This runbook implements ADR 0035. It keeps PgBouncer on the backend ECS, moves only PostgreSQL to RDS and keeps aggregate application pool/admission budgets fixed. Never commit `.env.rds`, the CA bundle, database URLs, passwords or load manifests.

## Required RDS information

- Private hostname and port in the backend ECS region/VPC.
- Database name, application/migration username and password.
- PostgreSQL major version, instance class and connection limit.
- Huawei CA bundle for hostname verification.
- Security-group access from the backend ECS only.

Place the CA bundle at `secrets/rds-ca.pem`, copy `.env.rds.example` to `.env.rds`, URL-encode reserved password characters in both URLs, and restrict both files to the deployment operator. Use an RDS hostname, not a raw IP, with `verify-full`.

## Preflight before migration

On the backend ECS, run the preflight in the migration image. Compose reads the private file without sourcing its values into the operator shell:

```sh
mkdir -p tmp
docker compose --env-file .env.rds -f compose.yaml -f compose.rds.yaml \
  run --rm --no-deps --user root -v "$PWD/tmp:/evidence" migrate sh -c \
  'RDS_DATABASE_URL="$DATABASE_URL" python scripts/rds_preflight.py --output /evidence/rds-infrastructure-preflight.json'
```

Require `pass=true`, TLS enabled, a primary/read-write server, UTF-8 and sufficient connections. Review DNS, TCP, connect and transaction timing. The output redacts the target and never contains the DSN.

## Migration and schema verification

Render the deployment first and verify that `postgres` is absent unless profile `local-database` is explicitly enabled:

```sh
docker compose --env-file .env.rds -f compose.yaml -f compose.rds.yaml config --services
```

Run migration as a bounded one-off task. It connects directly to RDS and serializes migration work with the existing PostgreSQL advisory lock:

```sh
docker compose --env-file .env.rds -f compose.yaml -f compose.rds.yaml \
  run --rm --no-deps migrate
docker compose --env-file .env.rds -f compose.yaml -f compose.rds.yaml \
  run --rm --no-deps --user root -v "$PWD/tmp:/evidence" migrate sh -c \
  'RDS_DATABASE_URL="$DATABASE_URL" python scripts/rds_preflight.py --require-schema --output /evidence/rds-schema-preflight.json'
```

Require exact migration checksums and all required tables/indexes. Seed only the isolated development fixture after verification.

## Idle baseline comparison

Capture the same statistics and representative plans used for the local control:

```sh
docker compose --env-file .env.rds -f compose.yaml -f compose.rds.yaml \
  run --rm --no-deps --user root -v "$PWD/tmp:/evidence" migrate sh -c \
  'PROFILE_DATABASE_URL="$DATABASE_URL" python scripts/postgres_baseline.py --output /evidence/rds-postgres-baseline.json'
docker compose --env-file .env.rds -f compose.yaml -f compose.rds.yaml \
  run --rm --no-deps --user root \
  -v "$PWD/tmp:/evidence" -v "$PWD/docs:/evidence-docs:ro" migrate \
  python scripts/compare_postgres_baselines.py \
  --local /evidence-docs/capacity/huawei-rds/local-baseline/postgres-baseline.json \
  --candidate /evidence/rds-postgres-baseline.json \
  --output /evidence/rds-idle-comparison.json
```

The profiler executes only SELECT/EXPLAIN statements and rolls back its row locks. Run it while no load is active. Idle plans diagnose indexes and network overhead; they do not establish capacity.

## Deployment

Start the measured four-replica topology with a pool of three per API and twelve
connections in aggregate. Set hold admission to the value accepted by the latest
ADR; validate the rendered per-process value and aggregate budget before traffic.
Include the existing private-network, reconciliation, keep-alive and horizontal
overrides used by ADRs 0034--0038. Use `compose.rds.yaml` after `compose.yaml` so
local PostgreSQL is excluded and PgBouncer points to RDS. Do not print rendered
Compose JSON because it contains interpolated secrets.

Verify every API, PgBouncer and worker is healthy. Confirm no `postgres` container is running in this Compose project before issuing traffic.

## Matched capacity stages

Use a fresh private development manifest for each long stage and delete it after evidence collection. Keep 800 shows, 300 seats, 8,000 viewers, 95% conditional reads, 5% unique holds, four generator workers, no retries and client/server keep-alive 5/10 seconds.

1. 400 RPS control.
2. 500 RPS for 30 minutes.
3. 750 RPS for ten minutes as a safety control, followed by a fresh 30-minute
   confirmation. A previous clean result does not waive the repeatability gate.
4. If both 750 RPS gates pass, run 800 RPS for ten minutes and then 30 minutes.
5. If every gate passes, 1,000 RPS for 30 minutes.
6. Stop at the first failed gate; do not rerun a failed rate until it passes by chance.

For every stage require zero unexpected responses, transport errors and generator drops; seat-map p95 below 150 ms; hold p95 below 300 ms; exact acknowledged persistence; zero broken links and overlapping seat intervals; and drained outbox, refresh and dead-letter queues. Record per-API CPU, PgBouncer queues, pool acquisition, query/body/commit timings, RDS CPU, connections, disk latency/IOPS, WAL and checkpoints.

## Failure validation

Run faults serially after load and expiry drain. Never overlap faults.

- Block API-to-RDS connectivity or stop local PgBouncer, require readiness failure and controlled hold failure, then restore and replay the same idempotency key.
- Force an RDS primary/standby failover through Huawei controls and measure detection/recovery; do not describe the local PgBouncer drill as RDS failover evidence.
- Terminate one API container during a deliberately delayed transaction and verify rollback, Redis shield release and successful retry.
- Drop the response after a committed hold and require the same key to return the stored response.
- Stop Kafka, accept one payment, require one unpublished outbox event, restart Kafka and require one eventual ticket despite duplicate callbacks.
- Interrupt Redis, verify controlled failure, recovery and reconciliation backlog drain.

After each case audit seat ownership, idempotency records, hold/order links, bookings/tickets, outbox and dead letters. Restore services in a guaranteed cleanup path.

## Rollback

Stop traffic before switching database authority. Removing `compose.rds.yaml` and starting profile `local-database` restores the isolated local test stack and its own dataset. Never route new traffic to the old local database after RDS has accepted writes; that requires an explicit data migration and cutover plan.


## Unattended distributed stage

ADR 0040 provides a single operator command for a complete stage. It deploys
the bounded admission candidate, prepares a fresh isolated fixture, transfers
the credential-bearing manifest through private temporary directories, then
runs preflight, warmup, no-retry traffic, post-TTL durability checks, rollback
and cleanup. It
retains compact evidence under the requested local output directory.

Before running it:

- Install the same committed revision on the backend and generator ECSs so both
  hosts contain the helper scripts.
- Configure OpenSSH aliases and keys for both hosts; batch-mode SSH must succeed
  without a password prompt.
- Keep the RDS environment file and credentials only on the backend ECS.
- Use a fresh, Git-ignored output path. Do not place a manifest in that directory.
- Confirm no other benchmark or fixture job is active.

Example safety stage:

```sh
python scripts/unattended_capacity_stage.py \
  --backend-host flash-api \
  --generator-host flash-generator \
  --backend-dir /root/flash-ticketing-rds \
  --generator-dir /root/flash-generator \
  --origin http://BACKEND_PRIVATE_ADDRESS:8000 \
  --output tmp/capacity/750-safety \
  --rate 750 \
  --seconds 600 \
  --admission-candidate 5 \
  --admission-rollback 4 \
  --identity-file tmp/capacity-auth/id_ed25519
```

Run `--dry-run` first to inspect the fixed phase order without contacting either
ECS. The live command verifies that both checkouts resolve to the same Git
commit before changing admission. Under ADR 0043, the generator starts one
detached, bounded job; the operator checks its short status response every
ten seconds and stops its process group before private cleanup. A missing or
malformed job status fails the stage and cannot promote a higher rate.
A temporary identity file may be supplied with `--identity-file`; remove
its authorization from both ECSs and delete the local key after the stage.
The generator helper prefers `/root/http-load-venv/bin/python` when present
and otherwise uses `python3`; set `FLASH_TICKETING_LOAD_PYTHON` on the
generator only when its managed environment is elsewhere.

A nonzero exit means at least one safety gate or cleanup step failed. Read
`stage-result.json` first, then the redacted phase logs. Do not start a higher
rate from a failed result. Verify `gates.rollback=true` before any later stage.
Raw high-volume observer output stays on the backend ECS; compact load, admission,
durability and rollback evidence is copied locally. Private manifests are removed
from the operator and both ECSs in the cleanup path.
