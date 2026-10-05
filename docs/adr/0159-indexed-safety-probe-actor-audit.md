# ADR0159: Index the safety audit through the probe actors

- Status: Accepted; corrected safety pair passed; subsequent measured control failed and candidate was skipped. No capacity promotion.
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


## Corrected cloud safety qualification (2026-10-05)

Run **adr0151-a6cdb46f8cff** passed both dedup-off/on arms after ADR0158/0159 harness corrections. Each arm verified source/images/factor,100 cross-host requests with exactly1accepted hold and99expected conflicts,one durable owner,hold/payment replay,other-actor denial and one customer-confirmed paid ticket. After hold expiry each retained1successful payment,1booking,1ticket and3callback deliveries,with0duplicate seats/orders and0pending financial work. Full global queues/Kafka lag returned to0;observer qualification,private cleanup and original topology restoration passed after each arm. Total2simulated safety tickets and0capacity stages; no customer retry hid failures.

[Safety qualification evidence](../capacity/flash-sale-opening/order-status-refresh-dedup-safety-qualification-2026-10-05.json). This qualifies the tested bounded safety configuration only. Deduplication CPU/DB cost and capacity improvement remain unmeasured; a separate fixed-load off/on comparison is proposed but not authorized. Earlier failed reports and consumed scopes remain preserved.

## Measured control failure (2026-10-05)

The subsequently authorized fixed-load pair **adr0151-f550731d5714** stopped after the dedup-off control failed. It offered 60 purchase journeys/s for 300 seconds: 18,000 scheduled, 14,921 dispatched, 14,192 customer-confirmed, 729 customer failures (4.886% of dispatched) and 3,079 generator drops. No customer retries were used. Worst-shard hold-to-ticket p95 was 12,796.71 ms. The dedup-on candidate was correctly skipped; no capacity improvement is established.

After hold expiry, 14,907 successful payments matched 14,907 bookings and tickets; 14 unpaid orders expired. Observed duplicate seats and multiple bookings per order were zero. Full-expected financial/zero-double-booking aggregate gates remain failed closed against 18,000 scheduled paid journeys; zero observed duplicates do not replace those gates. All other 29 of 32 gates passed, including source/budgets/cache age, observers, complete queues/Kafka drain and normal-runtime restoration. One measured stage and one additional isolated safety protocol executed; no automatic replacement follows the failed control.

Callback due-to-claim mean was 6,769.57 ms, backlog peaked at 525, status polling reached 7.97 checks per dispatched journey and offered-window primary CPU averaged 92.08%. This supports a delay/polling feedback-loop hypothesis. The root trigger remains unproven; the previous passing reference has different image revisions and is diagnostic only. Next is regression attribution before an architectural change or new measured scope.

[Measured control, retained failed gates and restoration evidence](../capacity/flash-sale-opening/order-status-dedup-measured-control-failure-2026-10-05.json). Original failed/dry evidence and consumption counters remain intact.
