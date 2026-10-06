# ADR0189: Atomic payment dispatch claim

## Status
Accepted for an isolated application candidate and local PostgreSQL qualification under ADR0172. No cloud deployment or adoption, profile change, new resource or SLO relaxation. Historical recovery remains unresolved. Refines the existing simulator claim implementation; its lease, delivery and financial-authority decisions remain in force.

## Context
The retained 84/s run measured simulator due-to-claim delay averaging about 4 seconds and callback backlog peaking at 404, alongside CPU pressure and API admission failures. These measurements do not prove claim SQL is the sole bottleneck. The existing simulator claim uses one locked SELECT plus a separate lease UPDATE before releasing its transaction. Each successful delivery therefore uses three simulator SQL statements across two transactions, including acknowledgement.

## Decision
Select the next due eligible payment with FOR UPDATE OF p SKIP LOCKED in a materialized CTE, then UPDATE its 15-second lease and fresh lease token and RETURN the callback fields in the same statement. Retain the current eligibility predicates, due ordering, returned order amount/currency and claim-observed timestamp. Keep the claim transaction committed before HTTP delivery, keep HTTP outside database ownership, and keep acknowledgement fenced by payment ID and lease token. Preserve duplicate delivery targets and the existing retry-after-lease behavior. Add no inline retry, database connections, worker slots or altered timeout.

Compare the original two-statement claim with the candidate against the same local PostgreSQL fixture using alternating rollback-only trials. Count statements independently of elapsed time. This diagnostic isolates claim round trips; it does not measure customer throughput, production CPU, successful tickets or network/RDS latency. Run real database contention, rollback, lost-delivery and stale-token tests before publishing the candidate.

## Alternatives
Increase simulator concurrency: changes database and HTTP pressure without reducing work. Move services first: may help shared CPU but requires unresolved deployment/recovery prerequisites. Change payment durability or acknowledge callbacks early: unnecessary for this isolated correction. General orchestration improvements: defer until needed to test this candidate.

## Consequences
Claim statements fall from two to one; successful simulator delivery statements fall from three to two. Two durable transaction boundaries and callback idempotency remain unchanged. Real performance improvement and reduction of the observed backlog must be measured later in a fixed-workload cloud comparison; local speed is not extrapolated to 300000 tickets/hour.

## Failure and recovery behavior
A failed claim or commit sends no HTTP request. Locked or actively leased payments remain unavailable to competing claims. Failed or ambiguous HTTP leaves the lease for existing expiry-based redelivery; a stale owner cannot acknowledge a replacement token. Duplicate callbacks continue to create at most one booking and ticket. Failed local evidence is retained; historical scopes are never replayed.

## Validation evidence
Executed: 36 unit/native PostgreSQL integration tests passed in 7.96 seconds; Ruff passed. Tests verify 24 concurrent claims with one dispatch, durable claim recovery, token fencing, duplicate callback and paid-event replay safety. Alternating 200 local rollback-only samples per arm measured claim median 2.830 ms before versus 2.297 ms after (18.83 percent lower), with p95 4.272 ms versus 3.493 ms. This is not customer throughput or production capacity. See [sanitized local evidence](../capacity/flash-sale-opening/atomic-payment-claim-local-2026-10-06.json). No cloud call or deployment; recovery and original profile gates unchanged.
