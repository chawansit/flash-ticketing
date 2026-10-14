# ADR0324: Bound fulfillment transaction batches

## Status

Experimental, default batch size one. Recorded before implementation; implemented, locally verified and immutably published. The controlled five-minute cloud comparison passed all gates at a conservative 100.90 paid-and-issued tickets/s. No new hourly or production qualification; normal runtime restored. Retain ADR0323 as an experimental, failed reference, not an accepted optimization. This decision changes only the opted-in fulfillment transaction grouping; all existing durable financial and replay guarantees remain.

## Context

ADR0323 reduced duplicate transaction starts and improved customer latency, but 13 undispatched journeys prevented qualification. Its 115 captured slot rejections occurred in two bursts approximately 232.51–232.66 and 234.40–239.21 seconds after opening. Independent database samples during that interval showed up to 17 sessions waiting on LWLock/WALWrite alongside one IO/WalSync waiter and concurrent DataFileRead waits. Payment requests and callbacks occupied slots in COMMIT while status reads waited. This supports reducing write/commit pressure; it does not establish storage saturation, a checkpoint cause or that batching will remove every stall.

The fulfillment lane currently commits its inbox receipt and business effect separately for every event, even when Kafka already delivered a bounded partition batch. Raising reservation-writer batch size previously worsened results under ADR0316; that rejected change is not repeated here. Static backend partitioning was also rejected under ADR0317/0318. Preserve the current shared connection budget.

## Decision

Add FULFILLMENT_TRANSACTION_BATCH_SIZE, default 1 and bounded to integer values 1–4. Values above one require separated event consumers and apply only to the fulfillment lane. Process already fetched messages without waiting for a batch to fill. Keep Kafka partition order. Split a group at the size bound or before a second event for the same order, so a committed snapshot cannot precede a later state change for that order in the same group.

For a selected group, capture Redis freshness timestamps before acquiring SQL. Acquire mutating order locks in stable UUID order. In one explicit SQL transaction apply the existing per-event inbox deduplication, ticket/refund effects and outbox writes in message order. Publish captured order status or perform existing authorized refreshes only after successful commit and connection release. Never share a transaction across Kafka partitions. Preserve the batch-one code path, existing pool/statement/lock timeouts, synchronous commit, TTL, payment/callback semantics, customer authorization and current notification suppression.

A local proof must verify fewer actual COMMITs for multiple paid events, visibility only after commit, whole-group rollback, deferred commit failure, replay after a committed response/publication failure and a simulated worker restart, stable locks and repeated-order splitting. Existing payment, atomic-hold and notification regressions remain required. A later controlled load must bind the exact image/configuration and keep resources, 24 physical backends, admission 20, four writers/batch four, six fulfillment consumers, gateway delay, recovery and 102 offered journeys/s for five minutes unchanged. Do not claim throughput improvement from local commit counts.

## Alternatives

Increase replicas or database slots: does not address measured WAL contention and changes the budget. Raise acquisition timeouts or generator concurrency: can conceal or move overload. Delay commits deliberately with commit_delay: PostgreSQL already groups followers behind WAL flushes; effectiveness on this SSD workload is unconfirmed. Disable synchronous commit or acknowledge before durability: rejected. Batch financial HTTP requests/callback acknowledgements: a separate persistence change outside this experiment. Change the idle bridge timeout: addresses a different early connection issue, not the late bursts.

## Consequences

Groups can amortize durable commits, but hold locks longer and couple up to four event outcomes. No forced collection delay is added. Existing poison-event isolation retries individual events after bounded group retries; replay of a committed group remains idempotent. The effect depends on actual fetched group sizes and must be measured. Default one preserves accepted behavior and remains the rollback setting.

## Failure and recovery

Any event or COMMIT failure rolls back the whole group; no captured cache state escapes. Kafka offsets advance only after durable outcomes or the existing explicit dead-letter handling. Failure after durable commit replays inbox identities without duplicating tickets, refunds or outbox effects. Repeated-order groups are separated. Preserve missing-event/error/drop evidence; require zero drops, approved final error and latency gates, zero double-booking/payment loss, actual-cohort reconciliation, complete drain and exact restoration. A failed short run cannot progress to higher or hourly load.

## Validation evidence

Executed 67 focused unit checks and 65 real PostgreSQL/Redis integration checks against all 22 exact candidate modules; one existing integration check was skipped. The tests are not cloud-capacity evidence. [ADR0323 retained comparison](../capacity/cce/explicit-transaction-start-comparison-2026-10-14.json) preserves raw result hash 0063b73eb56e44b535b703573637ede4faf3d15a66dfee74483c11c92f2dcaff. [PostgreSQL 17 WAL configuration](https://www.postgresql.org/docs/17/wal-configuration.html) explains group commit; it does not promise a benefit from this application change.


## Executed local verification

The tested three-paid-event case used three actual PostgreSQL COMMITs at batch one and one at batch four, in both driver modes. Separate tests verified pre-commit invisibility, full rollback after a middle-event error and a real deferred COMMIT failure, no captured cache publication while a connection is owned, replay after a lost post-commit publication, a new adapter/projector simulating restart, authorized retrieval, opposite-order concurrent replay without duplicate tickets and Kafka partition/offset boundaries. Repeated order identities split groups. Malformed default-path events still fail before acquiring SQL. The initial notification fixture omitted its required deduplication prerequisite; the corrected fixture passed without weakening the production validator.

The image derives from the exact ADR0323 digest and changes only workers.py and config.py; the other 20 modules and dependency inputs remain identical. Copied, installed and imported source hashes and eight role defaults were verified. A final pre-acquisition validation correction generated a fresh source identity and immutable tag; the earlier registry image was not deployed and is unselected. Do not deploy by mutable tag. Default batch size remains one; no original ECS, RDS or CCE runtime changed. Owned local PostgreSQL/Redis containers and temporary publication credentials were removed.

[Image, diagnostic correlation and executed local evidence](../capacity/cce/fulfillment-transaction-batching-local-2026-10-14.json). Next bind this single factor into the existing bounded comparison and repeat the same 102/s five-minute workload with unchanged budgets and quality gates. No new cloud performance result or hourly qualification is claimed.


## Runner integration verification

The existing bounded runner now selects the exact ADR0324 image and preserves the ADR0323 profile and identity files unchanged. API and ECS bootstrap batch size is explicitly one; only six native fulfillment consumers activate four, with all other workers remaining at one. Eighty-one focused runner/admission/recovery checks passed, including old and new profile activation. A full offline binding comparison passed against the retained ADR0323 failure and its independent recovery, unchanged resources/budgets, role environments, frozen helpers and customer gates. The first full binding rejected a missing receipt declaration; the exact public receipt was added to the frozen source list before deployment. No validator was weakened. The separately bounded cloud comparison is next; no result is claimed at this checkpoint.


## Executed controlled cloud comparison

Run adr0151-01b0b010f053 / ledger bounded_cce_ticket_target__5bd6f0e59156 passed and restored. Raw SHA 311ce962b75be217eda88b6811f0272d4e1e74b3b4a386fffc12ce806f5a1e8d. The 14 October 2026 Bangkok window was 19:27:50–19:32:50. At unchanged 102 offered journeys/s, all 30,600 scheduled journeys dispatched and fulfilled. No generator drops or final customer failures occurred. Five initial payment503 journeys recovered with five retries and five operation lookups; initial error rate 0.01634%. No payment loss, duplicate bookings or ticket relationship errors; both safety scopes passed, all queues drained, credentials cleared and original runtime restored.

30,477 unique paid tickets were issued within the timestamp window; the unchanged conservative inner-window check counted 30,269, or 100.8967 tickets/s over the 300second denominator. This qualifies this five-minute point, not an hourly capacity ceiling or a real concert opening.

Compared with ADR0323's failed but independently reconciled reference, generator drops fell from 13 to zero, initial-error journeys from 74 to five, and captured slot rejections from 115 to eight. Worst-shard total journey p95 improved from 3,060.68ms to 2,614.88ms, durable p95 from 2,022.00ms to 1,107.11ms and ticket-wait p95 from 1,623.52ms to 1,061.09ms. Actual COMMIT statement deltas fell from 305,381 to 280,522 (8.14%) across the recorded dispatch/completion scope. These are one-trial observations; the fixed offered rate does not identify a higher maximum throughput.

Do not claim CPU or WAL-byte reductions: measured API process CPU increased from 1.475 to 1.537 cores and worker CPU from 1.409 to 1.510. Attributed WAL bytes increased from 2.959GB to 3.531GB; the candidate observed one timed checkpoint while the reference observed none. Five recoverable payment errors and captured COMMIT holds up to 354.443ms remain. The underlying storage/fsync cause is unconfirmed, and the separate idle bridge issue has not been corrected. Default one remains; sustained qualification is required before adopting batch four.

[Full sanitized comparison, financial and restoration evidence](../capacity/cce/fulfillment-transaction-batching-comparison-2026-10-14.json). No higher-load or hourly stage followed this comparison.
