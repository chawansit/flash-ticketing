# ADR0247: Batch paid-cohort observer lookups

## Status
Accepted for isolated implementation and query-plan validation. Paid-runner integration and capacity qualification are pending. ADR0245 remains failed; this decision does not authorize candidate progression from that control.

## Context
The same accepted API image passed one 84 journeys/s control and failed the later control. Matched 300-second trace windows show longer database connection holds across consumer, writer, simulator and maintenance workers. This does not attribute the regression to one application query.

A bounded read-only inspection of the restored RDS completed in 14.562 seconds without changing runtime identity or starting traffic. The sampled order-status query used its order/bookings/ticket indexes and executed in 0.083 ms while idle. The observer's paid-cohort query executed in 283.072 ms and touched 208,770 shared buffers. It performed 23,390 payment index probes and 23,377 ticket probes. In the latest offered window, the observer's cohort phase averaged 156.189 ms versus 129.110 ms in the earlier passing control. RDS statistics estimate 4.89 million historical orders and 1.73 million bookings. These observations establish measurement overhead, not the root cause of all customer failures. The public.pg_stat_statements view was absent; availability in other schemas was not inspected; WAL timing is unavailable in retained samples.

## Decision
Implement an isolated read-only observer query that collects the selected cohort's order IDs once and requests payment rows using one ANY(array) lookup. Collect booking IDs for the selected shows and count their existing tickets with another ANY(array) lookup. Retain one SQL statement and the same nine result fields, payment delivery predicates, global unpublished-outbox count and lock-wait count. Never replace ticket existence with booking/order counts.

Keep the historical query and paid generator unchanged. First compare result equality and EXPLAIN ANALYZE on the exact retired cohort, with per-query timeouts and no customer traffic. Verify mixed statuses, empty cohorts, multiple seats, partial callback delivery and unrelated shows on an isolated PostgreSQL database. A lower isolated query cost does not establish customer throughput improvement. Integrating this candidate into the frozen paid runner requires an explicit identity-bound profile extension and a fresh unchanged control; no old reservation may be reopened.

## Alternatives
Add more API/payment slots: the latest control also rejected general acquisitions and cannot justify reducing general capacity. Change the order-status join: the sampled query is already indexed and cheap while idle. Reduce observer frequency: loses failure-time resolution. Omit payment/ticket verification or approximate counts: weakens evidence. Force session-wide planner settings: risks unrelated transactions and hides plan changes.

## Consequences
Only measurement SQL changes in the isolated candidate. No indexes, schema, financial transactions, customer authorization, atomic holds, TTL, retries, machine sizes or connection budgets change. The planner may still choose an expensive plan for large cohorts; inspect buffer cost and preserve bounded statements before admission.

## Failure and recovery behavior
Reject count differences, failed plans or a regression in query cost. Keep the original failed control and diagnostic completeness failure. Read-only profiling may warm cache, so compare alternating queries and report the scope rather than claiming a cold-cache or production benchmark. Full post-TTL durability, zero-double-booking, queue-drain and restoration audits remain mandatory and unchanged in future paid runs.

## Validation evidence
Executed diagnosis only: the restored runtime identity was unchanged after bounded read-only profiling; query plans and matched trace comparisons are retained privately. Candidate correctness, performance and integration will be recorded after execution. No paid traffic, stable 84 tickets/s or hourly capacity is claimed by this decision.

Executed candidate validation: three real PostgreSQL 17.6 integration tests passed; the owned local database was removed. All nine live-cohort counts matched in a read-only repeatable-read snapshot. Alternating warm-cache RDS plans were baseline 240.593/255.017 ms and candidate 199.380/202.068 ms: mean 247.805 -> 200.724 ms (19.0% lower), shared buffer hits 208,770 -> 117,353 (43.8% fewer). Both read-only inspections preserved runtime identity and took 30.812 seconds combined. Ruff passed. These are query-level improvements only; no new customer load, paid-runner integration or hourly qualification occurred. [Sanitized measured query evidence](../capacity/cce/cohort-query-profile-2026-10-09.json).
