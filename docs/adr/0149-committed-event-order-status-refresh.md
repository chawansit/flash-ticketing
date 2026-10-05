# ADR0149: Refresh advisory order status after committed events

- Status: Implemented;isolated matched60-buyer/s cloud comparison passed;disabled by default,higher capacity unqualified
- Date: 2026-10-05

## Context

ADR0148 failed84 offered buyers/s:301 status-read503,23 payment503,22 webhook503,120628 status checks for21778 dispatched journeys, and callback backlog peak404. Status reads and financial work compete for bounded resources. Existing ADR0097/0143 advisory cache/coalescing and ADR0145 callback reservation are local candidates; the measured cloud cache and callback reserve were disabled. The cache uses NX refill, so a pending snapshot can obscure committed fulfillment until its absolute deadline.

## Decision

Add default-disabled ORDER_STATUS_EVENT_REFRESH, valid only with a positive existing ORDER_STATUS_CACHE_MS (maximum3000ms). For a consumer handling OrderPaid, TicketsIssued or RefundRequested, refresh a display snapshot only after its existing PostgreSQL/inbox transaction has exited successfully. Replayed inbox events may refresh the advisory snapshot without redoing financial effects. Query current committed order/tickets with one internal single-statement read in the existing consumer pool; derive owner from that row, never from an untrusted event actor. This adds at most one snapshot read per relevant delivered event. No connection, thread, queue or polling change is added.

Capture Redis server time before the snapshot query. Atomically replace an older cached snapshot only if the new start is strictly newer, preserving the maximum absolute freshness deadline and validating the schema/owner/order/tickets before publication. Equal timestamps retain the existing entry; correctness requires bounded freshness rather than instant visibility. Ordinary read-through fills retain NX, so a delayed pending refill cannot overwrite a newer live event snapshot. Hits never extend TTL. Out-of-order events publish current database state, not the event payload. Events older than the snapshot deadline cannot publish; a cache hit cannot authorize any mutation.

Treat publication as best effort, with fixed metric outcomes. Redis or snapshot-read failure after financial commit must not cause fulfillment rollback, Kafka replay or dead-lettering merely to repair this cache. Cache miss/expiry retains existing authorized, bounded DB fallback and ADR0143 coalescing where already present. The isolated frozen-base candidate retains its original read-through behavior without bundling coalescing. This is an advisory cache, not a durable projection; existing outbox/inbox plus eventual expiry provide recovery. No extra durable retry queue.

## Alternatives

- Enable read-through cache alone: avoids some reads but may display cached pending state after fulfillment.
- Invalidate only: improves visibility but still requires each final customer read to reach the API DB pool.
- Publish before commit: can expose tickets from a rolled-back transaction; rejected.
- Add a durable projection queue/version column: stronger delivery/version guarantees at additional writes and lifecycle complexity, unnecessary for bounded advisory freshness.
- Bundle callback reserve, client polling/jitter, more connections or worker movement: confounds the next comparison. Keep these separate factors.

## Consequences

Publication adds Redis operations and snapshot reads to consumers and could worsen CPU/DB pressure. It targets repeated API reads, not reservation-writer delay directly. No RPS gain is claimed without measurement. It may miss an update on a crash/cache outage or equal timestamp; old snapshots expire within the existing bound. Owners remain immutable in this model. A fresh query on a replay may create a new deadline because it is a new DB snapshot, not TTL renewal of cached data.

## Persistence, locking, messaging, idempotency, TTL and scaling

PostgreSQL remains payment/booking/ticket authority. Keep existing transaction SQL, seat ownership, Redis atomic holds, outbox/inbox/Kafka acknowledgment/retry semantics, payment idempotency and hold/command TTLs. Add only a read-only snapshot adapter after commit. Retain existing pool sizes and total acquisition budget. This extends ADR0097/0143 snapshot publication; no accepted financial, persistence, locking or scaling decision is superseded. ADR0145 stays default0 and is not enabled by this change.

## Failure and recovery

A failed/ambiguous transaction must never publish. Successful transaction followed by publication failure retains committed tickets/inbox and allows broker acknowledgment. Later replay can repair the cache without duplicate tickets, or expiry causes authorized DB read-through. Lost Redis data affects display only. The Redis client retains bounded timeouts. No exception swallowing around financial work; only the explicit post-commit advisory refresh boundary catches ordinary publication failures. Disabling the flag restores the prior consumer path.

## Validation evidence

Recorded before implementation. Execute unit checks for settings, post-commit-only invocation, duplicate-event repair, bounded/error publication and unchanged default. Real PostgreSQL/Redis integration must verify pending-to-fulfilled refresh, delayed fills/out-of-order publication, owner isolation, Redis failure after commit, transaction rollback, exact ticket/payment replay,100-way hold exclusivity and drained outbox. Validate owned local containers/source/dependencies, retain test logs and remove only owned resources. All results pending. No cloud access/load, benchmark gain, GitHub push or main merge is authorized by this implementation.

## Executed local qualification

79 Windows focused unit tests and changed-source/test Ruff passed. Real owned PostgreSQL/Redis development-branch checks passed211 focused tests; broader native run passed1003 with9 Git-dependent setup errors because its container lacks Git.96 host checks in the two runner modules subsequently passed, including those9 cases. Do not call that native development-branch run fully passing. Initial unit decorator-signature/lint and omitted-new-file staging failures are retained in the evidence record.

An isolated candidate was prepared on frozen deb330ec91e553640d1d0ba10e92aa8f29cd86dc. The old consume_event transaction body and new _consume_event_transaction body have identical ASTs. Six protected API/application/seat/payment/locking/pool files match that base after LF normalization; unrelated local coalescing, callback reserve and seat projection changes are excluded. Native source/locked dependencies verified;155 focused and all663 unit/integration tests passed, zero skipped. Pending-to-ticket refresh, post-commit Redis outage and replay repair, rollback safety, owner isolation, delayed-fill fencing, equal timestamp/absolute TTL and current-state out-of-order event checks passed. Existing100-way hold, payment recovery and bounded shared-pool tests passed. Test outbox drained with a mocked acknowledged broker; real Kafka/cloud delivery was not exercised.

Owned containers/network/copied test contexts were removed and absence verified. Cloud unchanged, no load, image deployment, RPS improvement, GitHub push or merge. Snapshot-start fencing orders read freshness rather than database versions: bounded staleness is guaranteed by validation/expiry, not strictly monotonic display state. Fresh snapshot reads add consumer work; measure net benefit before scaling. [Local qualification evidence](../capacity/flash-sale-opening/order-status-event-refresh-local-validation-2026-10-05.json).

Next prepare isolated image/import-source identity and observer/SSH recovery, then a separately authorized comparison with identical enabled-cache age/budgets/workload in both arms and only event refresh toggled. Cache age selection, callback reserve activation and client polling/jitter are separate factors; no automatic84/s retry follows these local results.

## Matched cloud comparison

The separately approved ADR0151/0155 comparison001831458f5a passed off/on60 buyer journeys/s300s each at identical1000ms cache,2+2API placement,machines/images/budgets.Each18000/18000 confirmed paid issued tickets,0 errors/drops/doubles,post-TTL durability/full queues/restoration.All32 gates passed.See the [report](../capacity/flash-sale-opening/status-refresh-paid-replacement-comparison-2026-10-05.json).Status-cache misses-49.887%,API CPU-4.091%;consumer acquisitions+53.134%,API+consumer acquisitions+12.220% over observer lifetimes.PrimaryCPU69.029->69.952% and ticketp952141.06->2126.49ms showed little net change.No higher sustainable/hourly capacity is established;normal runtime was restored and the option remains disabled by default.Next review repeated committed-snapshot reads;no refresh-policy change is chosen/implemented by this result.
