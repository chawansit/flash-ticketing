# ADR0156: Deduplicate fresh fulfilled snapshot refreshes

- Status: Accepted for local implementation; disabled by default; cloud unqualified
- Date: 2026-10-05

## Context

Matched ADR0151/0155 off/on60-buyer/s300s comparison passed all32 gates and18000 confirmed paid tickets each. Event refresh cut API cache misses49.887% and API CPU4.091%, but consumer DB acquisitions rose53.134%. OrderPaid fulfillment emits TicketsIssued; both invoke a separate committed snapshot read. Candidate recorded36007 event fills. Primary CPU and ticket latency barely changed. Higher capacity is unmeasured.

## Decision

Add default-disabled ORDER_STATUS_EVENT_REFRESH_DEDUP, requiring event refresh and its existing bounded cache. A consumer-thread-owned OrderedDict holds at most1024 hints: order UUID, database-derived actor, canonical fulfilled-snapshot digest and a monotonic deadline anchored before its snapshot read. Hints exist only after OrderPaid successfully publishes a validated FULFILLED snapshot with tickets. They expire within the unchanged cache-age bound and are discarded on capacity eviction/process restart.

For TicketsIssued only, an unexpired hint permits the existing actor-scoped Redis lookup. Skip the advisory DB snapshot read only when lookup validates schema, owner, order, absolute freshness/TTL and returns the exact same canonical snapshot. Never renew Redis TTL or the hint on a skip. Missing/stale/invalid/mismatched/errored cache or hint triggers the existing committed DB read and publication. OrderPaid (including replay) always reads; RefundRequested invalidates its hint and always reads. TicketsIssued without a matching hint always reads, including after restart/rebalance. Publication must return explicit success; raced/stale/invalid/failed writes do not establish a hint.

Supersedes ADR0149's one-read-per-relevant-event policy only while this additional option is enabled. All financial and inbox transaction code, post-commit boundary, actor authorization, absolute TTL and default-off behavior remain. Successful relevant replays still repair a missing or expired projection. No new pool, queue, thread, distributed lock or persistence authority.

## Alternatives

Skip all TicketsIssued refreshes: loses a useful repair path. Refresh only TicketsIssued: delays first projection and changes OrderPaid replay repair. Batch-only coalescing: consecutive events may arrive in different polls. Redis distributed memo: adds keys and lifecycle complexity. Read snapshot inside financial transaction: changes protected transaction boundaries. Increase TTL: confounds the measured cache-age factor.

## Consequences

Eligible normal same-process OrderPaid/TicketsIssued pairs use one snapshot DB read instead of two, plus one Redis lookup. Eviction/rebalance/delayed events may conservatively retain two reads. Memory is bounded; the hint is never booking/payment authority. Exact-content/freshness checks preserve bounded advisory staleness, not monotonic database-state visibility. A concurrent refund can remain hidden only within the pre-existing absolute cache deadline; refund processing invalidates the hint and refreshes current committed state. Local query savings do not prove production CPU or capacity improvement.

## Failure and recovery

Commit failure prevents projector invocation. Redis publication failure leaves no hint; retry/replay reads and repairs. Redis lookup failure does not suppress a DB read. Cache deletion, corruption, actor/content changes or TTL expiry invalidate eligibility. No TTL extension on skips. Refund/restart/eviction fall back to existing behavior. Advisory errors never roll back committed payments or force financial replay. Preserve zero-double-booking, payment/ticket uniqueness and full queue checks.

## Persistence, locking, messaging, idempotency, TTL and scaling

PostgreSQL/Redis financial locking and persistence unchanged; Kafka/outbox delivery unchanged; inbox/payment idempotency unchanged; TTL unchanged. Each existing consumer process owns its bounded hint table; no cross-host correctness dependency. Use existing ADR0152 isolated-source/artifact workflow for a separate candidate, retaining historical source/image receipts. No cloud deployment, higher-rate test, publication or merge is authorized by local work.

## Validation evidence

Decision recorded before implementation. Owned PostgreSQL 17.6 / Redis 7.4.5 validation executed **182 focused** and **690 full isolated unit/integration tests**, all passed with no skips (the counts overlap). Source and locked dependencies were verified. Host projector/cache/coalescing/artifact/Compose checks executed **89 tests**, all passed. Changed-file Ruff and diff checks passed.

Tests cover eligible one-read pairs, disabled two-read behavior, exact owner/content matching, expiration without renewal, bounded eviction, refund invalidation, explicit publication success, deletion/corruption/outage, replay/restart and financial rollback. Integration tests confirm unique durable tickets, owner authorization, replay repair and drained owned outbox. Existing real database/Redis 100-request seat contention and payment recovery tests passed. The financial transaction AST and six protected API/reservation/database files match the qualified isolated parent.

The final exporter verifies 207 files and 19 runtime modules. Final Compose rendering caught and corrected an indentation defect after native tests; an added host regression and both workspace/isolated Compose renders passed. All runtime and test bytes remain identical to the native-tested candidate. Earlier permission/line-ending failures stopped before tests, were cleaned up and retained. Original native evidence was not rewritten. The separate patch inherits the existing Git LF/patch-context attributes, verified by a host regression.

[Local validation report](../capacity/flash-sale-opening/order-status-refresh-dedup-local-validation-2026-10-05.json) records counts, source identity, attempt history and raw evidence hashes. Native tests use an acknowledged outbox producer mock, not a real Kafka cluster. That native validation stage built no images and made no cloud calls/customer load; no production CPU/RPS improvement is claimed. The cloud comparison harness still needs adaptation and fresh safety qualification for this factor; the previous matched-run allowance remains consumed.

Subsequent [ADR0157](0157-isolated-deduplication-comparison-profile.md) preparation built and verified six offline images and the separate comparison profile. Cloud staging/safety and performance measurement remain pending.


## Corrected cloud safety qualification (2026-10-05)

Run **adr0151-a6cdb46f8cff** passed both dedup-off/on arms after ADR0158/0159 harness corrections. Each arm verified source/images/factor,100 cross-host requests with exactly1accepted hold and99expected conflicts,one durable owner,hold/payment replay,other-actor denial and one customer-confirmed paid ticket. After hold expiry each retained1successful payment,1booking,1ticket and3callback deliveries,with0duplicate seats/orders and0pending financial work. Full global queues/Kafka lag returned to0;observer qualification,private cleanup and original topology restoration passed after each arm. Total2simulated safety tickets and0capacity stages; no customer retry hid failures.

[Safety qualification evidence](../capacity/flash-sale-opening/order-status-refresh-dedup-safety-qualification-2026-10-05.json). This qualifies the tested bounded safety configuration only. Deduplication CPU/DB cost and capacity improvement remain unmeasured; a separate fixed-load off/on comparison is proposed but not authorized. Earlier failed reports and consumed scopes remain preserved.
