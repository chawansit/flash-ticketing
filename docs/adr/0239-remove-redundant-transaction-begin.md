# ADR0239: Remove redundant transaction BEGIN

## Status
Accepted for isolated local implementation and validation. Cloud comparison and hourly qualification are not executed. This is a candidate hypothesis, not confirmed attribution of the historical payment 503.

## Context
The hourly run adr0151-0f735e978cba produced one customer payment 503 and three callback 503s, all PoolTimeout. Native pools have four connections per API: two general and two payment; total PgBouncer connections remain 24. Sampling after the failure cannot establish which operation occupied payment slots. Mean connection holding time was 28.7586 ms; the adapter's BEGIN call averaged 4.6325 ms. Three owned API log reads hit 8 MiB ceilings, retaining no failure snapshots.

The installed, locked Psycopg 3.3.5 implementation starts a transaction automatically before executing the first query on a non-autocommit idle connection. Our adapter then executes an explicit BEGIN, adding a redundant command and server warning. Driver source inspection verifies the duplicate startup path. These means motivate reducing one round trip; they do not predict a guaranteed percentage improvement or explain the intermittent timeout.

## Decision
For the normal non-autocommit pool, rely on Psycopg's transaction startup when executing the existing local timeout setup query. Make autocommit=False explicit in the native pool connection configuration. Retain explicit BEGIN for a borrowed autocommit connection. Mark startup before timeout setup so cancellation/setup failure always invokes rollback. Preserve the existing transaction body, commit, rollback, connection-return ordering, local timeouts, isolation, and financial/idempotency/locking semantics.

This is the single application performance correction in the next candidate. Do not change pool sizes, admission/wait limits, retries, payment confirmation mode, worker placement, workload or customer gates. The historical control image remains immutable. Candidate images require exact copied/installed/import/source/configuration proof and a new identity. Improved diagnostics must be common to both arms and qualified separately; historical failed evidence and consumed scopes remain unchanged.

## Alternatives
Increase pool sizes or waiting time; add customer retries; enable asynchronous confirmation; change multiple queries. These either change budgets, obscure the failure, or prevent one-factor attribution. Keep duplicate BEGIN: preserves unnecessary network work while offering no stronger transaction boundary.

## Consequences
Normal transactions send one fewer redundant SQL command. Automatic driver startup remains a real PostgreSQL transaction; this does not make financial writes asynchronous. BEGIN telemetry shifts to the first measured setup query, so before/after comparisons use total hold, transaction, commit and acquisition durations rather than treating a missing BEGIN counter as missing transactions. The actual capacity and error effect remain unmeasured.

## Failure and recovery behavior
Body/setup failures rollback before returning a connection. Commit failures preserve the existing rollback/replay contract; a lost response does not justify creating a second payment. Retain a fallback BEGIN for autocommit adapters. Revert this isolated change if real-server transaction, concurrency, replay, durability or customer gates fail. Failed controls stop candidate progression. Do not run hourly until the new short comparison passes every required gate.

## Validation evidence
Executed before implementation: locked driver source inspection shows _start_query() issuing BEGIN for non-autocommit IDLE connections. The historical evidence establishes PoolTimeout, not the occupant or causal operation. All 77 sampling gaps over 2 s have paid_cohort as the largest observer phase; 273 cohort scans exceeded 1 s. Executed: 1,827 unit/runner tests passed; two Windows symlink cases skipped. Nine transaction regressions passed on an isolated PostgreSQL 17.6 server, including automatic startup without duplicate-BEGIN warnings, borrowed autocommit commit/rollback, setup failure, cancellation, lock timeout and connection reuse. The owned test container was verified absent after cleanup. PostgreSQL 17.6 is local correctness evidence, not RDS 17.11 performance qualification. Ruff and the offline historical reproduction contract passed. Cloud comparison and failure-time slot ownership attribution remain pending. No cloud actions, capacity benefit or production qualification are claimed.
