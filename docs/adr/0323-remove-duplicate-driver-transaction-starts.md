# ADR0323: Remove duplicate driver transaction starts

## Status

Implemented, locally validated and immutably published as a default-off bounded correction; no new cloud paid comparison or capacity claim. Supersedes the implicit driver start in the selected experimental Postgres adapter only. Preserve the accepted explicit BEGIN, transaction-local timeouts, atomic COMMIT/ROLLBACK and synchronous durability from the accepted adapter selected after ADR0242; other architectural decisions remain unchanged.

## Context

ADR0322 captured payment-request/callback COMMIT stalls and general BEGIN waits. Retained pg_stat_statements deltas show598056 BEGIN executions for299027 transaction-timeout setup calls, consistent with approximately two BEGIN commands per application transaction. The adapter configures Psycopg autocommit=False and also executes explicit BEGIN. Psycopg can automatically start the transaction before that command. Reproduce with actual protocol traces before accepting this interpretation. This does not establish the underlying WAL/storage or early OperationalError cause.

Read-only live diagnosis confirmed physical PgBouncer budget24, query_wait_timeout1second, transaction pooling, synchronous_commit=on, no synchronous_standby_names, and driver idle-session timeout1hour. Retained snapshots show nine warm server connections before traffic and24after opening; max sampled wait20.663ms does not establish a one-second query-wait timeout. Do not change queue timeouts, add connection retries or claim stale connections caused the errors. Historical tables contain millions of retained rows; preserve them and compare against the same database.

ADR0239/ADR0242 already investigated redundant BEGIN and rejected its API-only remove-BEGIN candidate after customer failures. That result remains rejected. This proposal revisits the cost with a different driver mode that retains explicit BEGIN and applies to the current shared API/worker runtime; it is a new bounded experiment, not a claim that the older optimization was never considered or that either implementation fixes COMMIT stalls.

## Decision

Introduce DB_EXPLICIT_TRANSACTION_START default off. In the opted-in adapter, configure driver autocommit=True so the existing explicit BEGIN is the only transaction start. This driver setting must never mean autocommitting financial statements: every production adapter operation continues to enter Postgres.transaction(), execute explicit BEGIN and local timeouts, and commit only after the whole operation succeeds. Existing connection and transaction contexts, savepoints, locks, write pipelines and measured commit/rollback boundaries stay intact. Reject non-boolean selection.

Wire the setting into both API pools and worker adapters without changing connection limits. Verify all production uses of the adapter connection remain inside its transaction method. Build from the exact ADR0322 shared image; change only postgres adapter, config and API/worker constructor wiring. Keep notification suppression as in the reference. No changes to business SQL, identifiers, Redis atomic holds, TTL, idempotency, Kafka offsets, payment simulator or retries.

Require real PostgreSQL protocol evidence proving two starts in the reference and one in the candidate, explicit transaction-local timeout enforcement, cross-connection invisibility before commit, rollback after a body failure, deferred commit failure, clean pool reuse and pipeline/savepoint behavior. Run financial/replay regression checks before immutable publication. After local gates, a separately sealed comparison keeps102 offered journeys/s300seconds, four APIs2+2slots, four writers/batch4, shared24/admission20, nineteen pods and concurrency500. Keep customer errors/recovery/drops separate; zero drops and financial/drain/restoration gates remain mandatory. A failed short run does not authorize hourly or higher load.

## Alternatives

Remove explicit BEGIN and rely on implicit startup: changes the accepted boundary and makes provenance less clear. Use undocumented driver internals: rejected. Broaden pipeline mode: ADR0320 rejected nested savepoint behavior; not part of this correction. Increase connection budgets or raise query-wait timeouts: hides/confounds queue contention. Disable synchronous commit or ACK payments before durable commit: rejected. Delete historical data or rebuild large tables: changes the workload and requires a separate decision.

## Consequences

Expected benefit is one fewer transaction-start command and protocol wait per opted-in transaction, reducing connection occupancy and database work. Actual throughput benefit remains unmeasured. This cannot remove a storage/network COMMIT stall directly. Driver autocommit changes raw-connection behavior, so raw connections must not become an unguarded financial write API. Activation stays default-off until correctness and comparison gates pass.

## Failure and recovery behavior

Body/setup/commit errors must propagate; rollback the explicit transaction before returning a healthy connection, or discard a broken connection. COMMIT ambiguity is resolved through the existing idempotent operation lookup and replay behavior. Never create duplicate charges/tickets or extend seat expiry. Kafka advances only after durable processing; cache publication follows commit. Rollback disables the flag or restores the exact parent image and runtime; reconcile the actual cohort and drain queues after every failed experiment.

## Validation evidence

ADR recorded before implementation. Local reproduction, correctness tests, image publication and cloud comparison are pending. [Psycopg transaction management](https://www.psycopg.org/psycopg3/docs/basic/transactions.html) describes automatic transaction starts and explicit atomic blocks on autocommit connections. [ADR0322 measured reference](../capacity/cce/ticket-notification-dedup-comparison-2026-10-14.json) retains failures and independent recovery.


## Executed local and live read-only validation

PostgreSQL17.6 statement counters confirmed two BEGIN executions in the reference and one in the candidate, with exactly one COMMIT in each. Eight real tests across both modes passed: pre-commit cross-connection invisibility, body rollback, deferred commit failure, local timeout settings, clean connection reuse, pipeline and savepoints. Initial libpq client tracing was unavailable on Windows; the final test uses server statement counters rather than skipping the assertion.

The exact candidate source then passed56 PostgreSQL/Redis integration checks with one existing Kafka-dependent test skipped. These include atomic holds, authorized retrieval, duplicate payment/callback replay, ticket issuance and notification handling. One lock-contention test initially used an implicitly transactional raw connection; it now opens a deliberate adapter transaction and preserves the same lock-timeout/recovery assertion. Forty-seven API/pool/default unit tests passed after preserving disabled-path constructor calling conventions. Initial failures remain retained privately.

The candidate preserves all eighteen other module hashes and dependency inputs from ADR0322. All twenty-two copied/installed/imported modules and eight role defaults were verified. SWR rejected the first OCI-format upload; the corrected single-platform Docker-format image was published and remotely pulled/verified without changing application source or the original runtime. Feature default remains off.

Twenty alternating warm read-only pairs through the existing live PgBouncer/RDS path measured mean connection hold6.8256ms reference versus5.6916ms candidate (16.6% lower); mean commit1.0763ms versus1.0738ms was essentially unchanged. Each mode used one client connection inside the existing physical budget. The helper and its temporary credentials were removed, original runtime remained unchanged and the generator remained idle. This is transaction-overhead evidence, not customer throughput or a claim that the underlying WAL/storage stall is solved.

The separately sealed lifecycle uses the selected mode on bootstrap/native APIs and native workers; ECS bootstrap workers remain off until native activation. Original runtime restoration returns all original settings. Exact source, flag, connection-budget, baseline and namespace ownership gates must pass before dispatch. No new cloud paid load or capacity qualification is claimed at this checkpoint.

[Exact immutable image](../capacity/cce/explicit-transaction-start-image-2026-10-14.json), [executed local and read-only RDS evidence](../capacity/cce/explicit-transaction-start-local-2026-10-14.json). Public image preparation reproduced the same22-module source identity;140 affected runner/admission tests passed.
