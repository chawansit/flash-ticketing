# ADR0173: Bounded database wait and WAL diagnostics

## Status
Accepted under ADR0172. Locally qualified; managed PostgreSQL read-only capability check passed. Paid control executed; financial/customer gates passed, diagnostic completeness failed.

## Context
ADR0171 completed 18,000 paid-and-issued tickets at 60 journeys/s for 300 seconds, with complete financial reconciliation and queue drain. Its longest commits finished together across four APIs on two hosts. A shared downstream stall is plausible, but neither sparse PgBouncer samples nor low aggregate RDS utilization prove the cause. The 84/s target remains unqualified.

## Decision
Add a separate control-only diagnostic profile with the exact ADR0171 application images, workload, machine sizes, connection budgets and customer gates. Reuse the pipeline observer's existing autocommit database connection. Collect allowlisted PostgreSQL activity counts, WAL and checkpointer counters and timing-availability settings once per existing sample. No new database connections, extensions, privileges or server setting changes are authorized by this decision.

Validate PostgreSQL capabilities before creating a paid fixture using one transient connection that closes before the regular observer starts; no concurrent observer connection is added. Each diagnostics query runs inside a read-only transaction with a local 100 ms statement timeout. A failure is retained as a bounded error classification and fails diagnostic completeness; existing financial audits, queue drain and restoration remain mandatory. Capture query duration, sample timestamps and statistics reset epochs. A zero timing counter when timing collection is disabled is unavailable evidence, not proof that WAL writes are fast.

One fresh standing reservation permits safety qualification followed by one 60 journeys/s, 300-second control, only if qualification passes. This is the shortest existing qualified matched window; a shorter invented workload would not provide a comparable control. One-second activity snapshots can miss shorter waits, and cumulative statistics lag activity. Correlation narrows hypotheses but does not establish causality. Do not increase load or adopt a performance correction from this diagnostic alone.

## Alternatives
- Another observer connection with continuous high-frequency sampling: better temporal coverage but changes connection pressure and observer cost.
- Enable server-wide WAL timing or install extensions: requires privileges and changes the experiment; defer until necessary and supported.
- Tune WAL or enlarge pools now: rejected because the bottleneck is not attributed.
- Reuse sparse old metrics alone: cannot distinguish brief database waits from upstream transport or assignment delays.

## Consequences
Two bounded read-only diagnostic transactions per sample add measured observer work. Keep the existing sample-gap gate and report overhead. Unsupported views, permission-restricted activity or counter resets invalidate the corresponding evidence. Cluster WAL/checkpoint counters include other database activity and cannot be attributed exclusively to ticketing.

No application persistence, locking, payment idempotency, hold TTL, messaging semantics or scaling decision is superseded. ADR0172's single-profile restriction is extended only after local qualification to include this exact control profile; its spending, publication, stop and cleanup boundaries remain unchanged.

## Failure and recovery
Diagnostic errors must not hide customer or correctness failures. Missing coverage prevents a diagnostic pass; teardown still collects retained traces, audits payment durability and zero double-booking, drains all queues and restores the original topology. Consume failed scope identities and use a new reservation for any rerun. Preserve explicit pauses and unresolved recovery blockers.

## Validation evidence
Executed: 297 focused tests passed in 51.59 seconds across nine suites, including query privacy/bounds, unsupported capabilities, reset handling, collector integration, unchanged profile contracts, standing authorization, transport recovery and cleanup. Ruff passed; canonical naming passed for 1,488 documents. Read-only RDS check: PostgreSQL 17.11, complete activity visibility, WAL and checkpointer views available; track_wal_io_timing disabled and track_io_timing enabled. Collector wall time 13.89Ã¢â‚¬â€œ31.08 ms in two preflight samples. One unchanged cloud control remains pending. No capacity improvement claimed.

References: [PostgreSQL statistics](https://www.postgresql.org/docs/17/monitoring-stats.html) and [statistics settings](https://www.postgresql.org/docs/17/runtime-config-statistics.html).

### Executed control and diagnostic follow-up
The fresh standing reservation completed safety qualification and one paid control with exact restoration. All 18,000 customers obtained unique paid-and-issued tickets, no drops or customer failures, and complete queues. Only database_wait_evidence_complete failed: 10 of 478 samples returned ValueError. Valid samples showed WALWrite wait peaks of 20 sessions and WalSync waits near shared slow commits; WAL timing remained disabled. This is evidence of WAL-related contention, not a complete causal attribution or a capacity improvement.

A subsequent 65-second read-only visibility check found no hidden sessions and did not reproduce the errors. The account lacks pg_read_all_stats membership. The specific validation cause is unresolved. Add allowlisted failure codes/phases and bounded backend category counts while retaining the strict visibility gate. Do not exclude hidden sessions or declare the issue fixed without evidence. Preserve the original failed result and consume its reservation; any paid rerun needs a fresh identity and safety qualification.

Executed follow-up validation: 111 targeted tests passed in 18.55 seconds; Ruff passed. The expanded SQL passed two read-only managed PostgreSQL samples (20.96 ms and 13.67 ms). This validates query compatibility, not reproduction of the load-dependent failure or a paid-load pass for the classifier. See [the control report](../capacity/flash-sale-opening/database-wait-diagnostic-control-2026-10-06.json).
