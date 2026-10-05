# ADR0159: Index the safety audit through the probe actors

- Status: Accepted for local harness correction; fresh cloud safety unqualified
- Date: 2026-10-05

## Context

ADR0158 replacement safety6a6446959501 verified pinned runtime sources and returned one accepted hold plus99 expected conflicts across two hosts without transport errors. The control stopped with QueryCanceled during its read-only durable-owner audit, before payment. The candidate was not launched. Original topology, credentials and global DB/Redis queues and Kafka lag were restored/cleared.

Read-only RDS EXPLAIN shows the idempotency query searching operation/key in the primary key(actor,operation,key) without the leading actor. The table has approximately4587504 rows. A bounded read-only comparison after restoration returned the same ownership tuple(1,1,1,1): original1.7016s, actor-scoped0.03183s including connection setup. This supports an expensive audit-access-path diagnosis; it does not reproduce the earlier timeout or measure application capacity. No schema or RDS parameter changes were made.

## Decision

Restrict the idempotency-record audit to the100 known probe actors, derived from the same fresh run namespace used to mint their JWT subjects. Keep operation='hold' and the run's key prefix, counting every matching key for every participating actor, including unexpected extra keys. Use the existing actor-leading primary key; keep the read-only transaction and3s statement timeout. Validate the run namespace before connecting.

Keep the global event/seat active-hold/order count and seat-pointer check unchanged: an extra active owner from any actor still fails. Keep cross-host replay, payment duplication, authorization, post-TTL financial audit, global zero-double-booking and queue/restoration gates unchanged. The idempotency namespace audit now explicitly covers the participating100 actors, not arbitrary nonparticipant actors using the same text prefix; global seat ownership remains unscoped.

Bind the uploaded probe's source hash into shared adapter identity so this correctness-query change invalidates old approvals/qualifications. No image rebuild is needed: the probe is uploaded by the driver. This supersedes only the original unscoped idempotency prefix query in ADR0147's probe and its omission from adapter identity, while retaining accepted safety and restoration requirements.

## Alternatives

Raise timeout: allows history-dependent audit cost and masks the access path. Add a production key-leading index solely for this probe: adds schema/write/storage cost and changes the fixed baseline. Query only the winner: misses extra records among losers. Query exact expected keys only: misses additional keys from a participating actor. Restrict participants while retaining prefix and global owner checks uses existing indexing and preserves the concurrency assertion.

## Consequences

Audit cost should follow the participating actor records rather than unrelated historical actors. The participant boundary is explicit. No change to application persistence/locking, messaging, idempotency, seat TTL or scaling; no connection-budget, cache-age or SLO changes. The failed control and consumed allowance remain preserved; no automatic replacement or capacity stage.

## Failure and recovery

Malformed run IDs fail before DB access. Missing/extra participant records, invalid hold/order linkage, actor mismatch, wrong seat pointer, expired or multiple active owners fail closed. Database errors are not retried. Existing driver stops after failed control and restores original topology and queues. A fresh bound safety-only scope requires separate human authorization.

## Validation evidence

Recorded before implementation. Failed safety evidence:tmp/adr0151-6a6446959501/comparison-summary.json. Query plans/comparison:tmp/adr0158-owner-diagnostic-0e2187517586/query-plans.json. Native local PostgreSQL correctness tests and harness regressions pending. No payment requested, no ticket issued, no candidate or capacity stage executed in this failed control.


After implementation, **7 native PostgreSQL17.6 tests passed** against an owned isolated local container: valid ownership with unrelated history; missing/extra participant records; actor mismatch; wrong seat pointer; expired hold; second active owner from a nonparticipant. The owned container was removed. **287 harness tests passed**, including existing safety/32-gate/restoration regressions, malformed actor namespaces rejected before DB connection, and uploaded-probe hash drift changing the approval binding. Changed-file Ruff and diff checks passed. All19 application runtime modules remain unchanged; no image rebuild or further cloud customer traffic occurred.

[Safety failure, restoration and corrective evidence](../capacity/flash-sale-opening/order-status-dedup-safety-audit-correction-2026-10-05.json). The fresh two-ticket, zero-capacity replacement proposal remains inactive. Cloud safety, deduplication performance and hourly capacity remain unqualified.
