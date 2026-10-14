# ADR0323: Remove duplicate driver transaction starts

## Status

Experimental, default off. Implemented, locally validated and immutably published; the controlled cloud comparison failed the zero-generator-drop gate and was restored and independently reconciled. Not adopted or capacity-qualified. Supersedes the implicit driver start in the selected experimental Postgres adapter only. Preserve the accepted explicit BEGIN, transaction-local timeouts, atomic COMMIT/ROLLBACK and synchronous durability from the accepted adapter selected after ADR0242; other architectural decisions remain unchanged.

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

ADR recorded before implementation. Executed validation and the failed cloud comparison are recorded below. [Psycopg transaction management](https://www.psycopg.org/psycopg3/docs/basic/transactions.html) describes automatic transaction starts and explicit atomic blocks on autocommit connections. [ADR0322 measured reference](../capacity/cce/ticket-notification-dedup-comparison-2026-10-14.json) retains failures and independent recovery; it is not a passing control.


## Executed local and live read-only validation

PostgreSQL17.6 statement counters confirmed two BEGIN executions in the reference and one in the candidate, with exactly one COMMIT in each. Eight real tests across both modes passed: pre-commit cross-connection invisibility, body rollback, deferred commit failure, local timeout settings, clean connection reuse, pipeline and savepoints. Initial libpq client tracing was unavailable on Windows; the final test uses server statement counters rather than skipping the assertion.

The exact candidate source then passed56 PostgreSQL/Redis integration checks with one existing Kafka-dependent test skipped. These include atomic holds, authorized retrieval, duplicate payment/callback replay, ticket issuance and notification handling. One lock-contention test initially used an implicitly transactional raw connection; it now opens a deliberate adapter transaction and preserves the same lock-timeout/recovery assertion. Forty-seven API/pool/default unit tests passed after preserving disabled-path constructor calling conventions. Initial failures remain retained privately.

The candidate preserves all eighteen other module hashes and dependency inputs from ADR0322. All twenty-two copied/installed/imported modules and eight role defaults were verified. SWR rejected the first OCI-format upload; the corrected single-platform Docker-format image was published and remotely pulled/verified without changing application source or the original runtime. Feature default remains off.

Twenty alternating warm read-only pairs through the existing live PgBouncer/RDS path measured mean connection hold6.8256ms reference versus5.6916ms candidate (16.6% lower); mean commit1.0763ms versus1.0738ms was essentially unchanged. Each mode used one client connection inside the existing physical budget. The helper and its temporary credentials were removed, original runtime remained unchanged and the generator remained idle. This is transaction-overhead evidence, not customer throughput or a claim that the underlying WAL/storage stall is solved.

The separately sealed lifecycle uses the selected mode on bootstrap/native APIs and native workers; ECS bootstrap workers remain off until native activation. Original runtime restoration returns all original settings. Exact source, flag, connection-budget, baseline and namespace ownership gates must pass before dispatch. No new cloud paid load or capacity qualification is claimed at this checkpoint.

[Exact immutable image](../capacity/cce/explicit-transaction-start-image-2026-10-14.json), [executed local and read-only RDS evidence](../capacity/cce/explicit-transaction-start-local-2026-10-14.json). Public image preparation reproduced the same22-module source identity;140 affected runner/admission tests passed.

## Controlled cloud comparison and recovery

Run adr0151-c54f25a7ba82 offered 102 journeys/s for 300 seconds on 14 October 2026, 17:18:48–17:23:48 Bangkok time. Resources, connection budgets, notification suppression, gateway delay and recovery settings stayed unchanged from ADR0322. The raw result remains failed: 30,600 journeys scheduled, 30,587 dispatched and fulfilled, 13 generator drops, 74 initial-error journeys all recovered with 84 retry attempts, and 0 final customer failures. The drops occurred late in the offered window when a generator shard reached its configured 250 active journeys; they are not hidden or counted as successful customers.

Worst-shard total journey p95 improved from 4,518.84 ms to 3,060.68 ms, payment HTTP p95 from 261.67 ms to 188.94 ms, and ticket-wait p95 from 2,753.83 ms to 1,623.52 ms. API process CPU averaged 1.475 cores versus 1.711; background process CPU 1.409 versus 1.503. These are observations from one failed comparison, not a qualified capacity increase or node CPU/throttling measurement. Unique paid tickets issued inside the timestamp window were 30,463; the unchanged conservative inner-window check counted 30,258, or 100.86 tickets/s over the 300 second denominator. The numerical ticket target passed, but zero drops remains mandatory. No hourly or higher-load stage followed.

The original financial gates expected all 30,600 scheduled orders and therefore remained failed. Independent post-TTL reconciliation verified the actual 30,587-order cohort: 30,587 unique paid-and-issued tickets, no duplicate seats, payment loss or relationship errors, and both independent safety payments passed. Automatic restoration completed; a fresh independent check confirmed the original four ECS APIs stable, test namespace and helpers absent, generator idle, secondary empty and all queues zero. Forty-two recovery-validator tests passed, including rejection of missing payments, hidden drops, incomplete shard evidence and a false capacity claim. The failed raw result is preserved rather than reclassified as a passing load test.

[Sanitized comparison and recovery evidence](../capacity/cce/explicit-transaction-start-comparison-2026-10-14.json) records the raw and recovery evidence hashes. The setting stays default off and the prior ADR0242 rejection remains in force.

## Remaining failure evidence

Complete failure captures recorded 115 slot rejections: 58 role-limit, 41 native-timeout and 16 global-limit events. Every captured event occurred after 200 offered seconds; captured COMMIT holds reached 537.414 ms. Removing duplicate starts reduced BEGIN work, but the underlying late COMMIT stalls remain unconfirmed. Cluster WAL I/O timing was unavailable; sampled WAL waits do not establish storage or checkpoint causality. Diagnose these late stalls before another correction and matched load.

A separate early connection issue affected all six fulfillment consumers after more than seven minutes since startup. The database bridge uses an inactivity timeout of 300 seconds. [Nginx stream proxy documentation](https://nginx.org/en/docs/stream/ngx_stream_proxy_module.html#proxy_timeout) describes closing the stream when no reads or writes occur within that interval. A local reproduction with the same pinned Nginx image and a scaled 2 second idle timeout produced a stale pooled OperationalError without SQLSTATE. This is a plausible mechanism for the early errors, not proof that it caused the late payment-slot rejections. A naive checkout-recycling callback also failed its bounded local acquisition test with PoolTimeout; it was not adopted. No idle-connection correction or production timeout change was implemented, and all local reproduction resources were removed.
