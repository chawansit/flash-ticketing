# ADR 0080: Batch read-only durability audit by load run

Status: Accepted for read-only audit behavior; sustained-load validation pending

## Context

The 1,000 RPS, 30-second Huawei stages at revisions `c8b3963` and `8020a05` passed all fixed post-TTL correctness gates, but their durability verifier spent several minutes auditing just 1,800 acknowledged holds. A sustained stage would create many more holds and risks exceeding the runner's five-minute audit timeout even when the application is healthy.

Read-only PostgreSQL `EXPLAIN (FORMAT JSON)` on the RDS dataset estimated approximately 2.41 million idempotency records and 677,000 reservation commands. The current verifier issues one idempotency/hold/order count and one reservation-command count per generator run (eight runs), followed by one global overlap query. The reservation-command count uses a sequential scan, and the overlap query also scans idempotency records. These repeated scans are audit overhead, not booking throughput. A read-only rerun of the grouped SQL against the same completed stage preserved every count and took 160.015 seconds overall: idempotency links 2.168 seconds, reservation commands 0.676 seconds, overlap 2.104 seconds, and the remaining 155.037 seconds in a sequential Redis `SCAN` plus one `XLEN` and `XPENDING` round trip per historical stream. The Redis phase is the dominant remaining audit cost.

## Decision

Group all worker run IDs in one read-only count query over idempotency records and one over reservation commands, retaining exact `run_id + '-'` prefix matching. Generator run IDs in one stage must have equal length; reject malformed or mixed-width identifiers rather than silently misattribute rows. Treat an absent group as zero, so missing durability remains a failure. Keep the existing full interval-overlap query and global queue check, but apply the same exact prefix set. Do not add a production database index or mutate application tables for this diagnostic workload. Use a complete Redis keyspace `SCAN` with bounded batches of at most 256 stream keys and a non-transactional pipeline of `XLEN` and `XPENDING`, following the existing capacity preflight pattern. Do not trust the advisory stream registry as a complete audit source. Missing consumer groups on nonempty streams count as pending work and fail the gate; unexpected Redis errors fail the verifier. Record elapsed time for each audit phase and the number of scanned stream keys without changing pass criteria.

This refines the verifier implementation of ADR 0029's mandatory post-run durability gate. It does not weaken the zero-overlap, exact durable-command, broken-link, expiry or queue requirements.

## Alternatives considered

- Increase the five-minute audit timeout without changing queries: rejected because it hides repeated full-table work and makes long stages expensive to validate.
- Add persistent indexes on synthetic idempotency prefixes: rejected for now because they would increase production write amplification to speed a test-only query.
- Sample holds instead of counting every acknowledged command: rejected because exact durability and zero double-booking require complete validation.
- Omit the overlap window query: rejected because count equality does not prove that two holds did not own the same seat at overlapping times.
- Check only the advisory reservation-stream registry: rejected because registry updates can fail and a full keyspace scan is the recovery path; this could silently miss orphaned pending commands.

## Consequences

The verifier performs one grouped scan of each relevant table rather than one scan per generator run. Grouped results require explicit zero-fill for absent runs. The separate overlap query remains a significant scan. Redis remains a complete scan of historical stream keys, but bounded pipelines remove per-key network round trips; the total may not fall to milliseconds. The normal application request path and database schema are unchanged.

## Failure and recovery behavior

The verifier runs in a read-only transaction with a bounded statement timeout. Any SQL error, malformed run identifier, missing run count, broken link, expired-but-active hold, pending order, non-durable acknowledged command, overlapping seat interval or undrained queue fails the stage. A failed audit does not alter bookings or reservations; stage rollback still restores the service topology. The previous per-run verifier remains available in Git history for comparison.

## Validation evidence

Before implementation, the 30-second cloud stage's final verifier ran for several minutes, although 30,000/30,000 requests and 1,800/1,800 holds passed. Read-only RDS plans showed per-run idempotency index scans, per-run reservation-command sequential scans and a further idempotency sequential scan for overlap. The grouped SQL passed 20 focused tests and Ruff. A read-only RDS rerun against the last passed stage matched every per-run count, 1,800 audited holds, overlap zero and all queue counts. Its measured SQL phases totaled about five seconds, while sequential Redis stream inspection took 155.037 seconds. The focused grouped-audit and stage tests passed (23 tests), including empty streams, orphaned nonempty streams with no consumer group, and pending messages. Ruff passed for the changed Python files; the complete unit suite passed 177 tests. A second read-only comparison against the same completed Huawei run matched every per-run count, all 1,800 audited holds, zero overlaps and every PostgreSQL/Redis queue count. It scanned all 32,311 historical reservation-stream keys and took 8.619 seconds total: idempotency links 2.259 seconds, reservation commands 0.650 seconds, overlap 2.028 seconds and pipelined Redis streams 3.652 seconds. The prior grouped-SQL/sequential-Redis run took 160.015 seconds total, of which 155.037 seconds were Redis inspection. This is an 18.6x reduction for the same read-only dataset. No load was generated during either comparison. A sustained capacity stage using this verifier remains unverified. The compact comparison is recorded in [`docs/capacity/huawei-rds/2026-09-29-audit-batching`](../capacity/huawei-rds/2026-09-29-audit-batching/README.md).
