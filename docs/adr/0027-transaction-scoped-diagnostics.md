# ADR 0027: Transaction-scoped diagnostic database settings

Status: Accepted; cloud recovery validated.

## Context

The first ASGI candidate contention wave returned one 500 and no winner. Its server traceback was ReadOnlySqlTransaction at INSERT. The preceding ownership verifiers connected through transaction-pooled PgBouncer and issued SET default_transaction_read_only=on with autocommit. Session SET is not isolated to that logical client in transaction pooling; a later application transaction can receive the contaminated server connection. This invalidates that wave as a middleware performance comparison, but the failure is retained. Baseline HTTP waves completed before the verifiers ran.

## Decision

Use an explicit read-only transaction for ownership/durability verifiers: autocommit=False, SET TRANSACTION READ ONLY before reads, and SET LOCAL for statement timeouts. Commit/rollback and close on exit. Do not issue persistent session settings through the pooler. Restart only the isolated benchmark pooler/API to discard contaminated connections, then validate successful writes after verification. Keep the failed trace and run outputs. PostgreSQL remains ownership authority; no reservation algorithm changes.

## Alternatives

Direct PostgreSQL-only diagnostics avoid pool reuse but do not make the utility safe if later pointed at PgBouncer. Resetting a session afterward is fragile on exceptions and can target another backend under transaction pooling. Forcing global pooler resets is unnecessary for correctly transaction-scoped settings.

## Consequences

Verifier reads share one read-only READ COMMITTED transaction and may still observe committed changes between statements. Immediate ownership checks remain time-bounded before hold expiry. Statement timeout is local to the verifier transaction. No prior booking or idempotency ADR is superseded; this supersedes the verifier's unsafe session-setting implementation.

## Failure and recovery

Rollback/close on error; preserve evidence. Clear currently contaminated benchmark pooler connections, verify the failed wave created no durable owner, rerun fresh-seat comparisons and verify a later write after the read-only checks. Do not label the failed wave as a pass or change admission behavior to hide it.

## Validation evidence

After pool reset and transaction-scoped diagnostics, three candidate waves and the closing baseline each created exactly one durable owner. A later 400 RPS control created 6,000 holds without read-only errors. Post-expiry verification of 36 worker results passed with 6,006 acknowledged holds/orders and no active or overdue holds. The failed read-only wave created zero durable records. An initial verifier accounting bug incorrectly treated contention statuses as flat; its failed report is retained, corrected nested accounting has a regression test, and a separately named verification passed. See docs/capacity/overnight for evidence.

## Final audit memory bound

The combined 60-worker interval audit failed before producing a result because a parallel PostgreSQL query exhausted the container shared-memory allocation (16 MiB segment resize). Use SET LOCAL max_parallel_workers_per_gather = 0 within this read-only audit transaction. This preserves all-row coverage without changing application settings or enlarging production resources. The alternative is increasing container shared memory, which would change the measured stack; a serial diagnostic is slower but bounded and its setting ends with the transaction. Retain the failed log and rerun after hold expiry.

Serial final audit validation: 94,748 holds/seat intervals across 60 workers passed with zero overlaps. The final unit suite (including assertions for all three transaction-local diagnostic statements) passed 67 tests, with two dependency warnings.
