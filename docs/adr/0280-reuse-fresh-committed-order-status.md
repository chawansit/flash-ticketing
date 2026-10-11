# ADR0280 Reuse fresh committed order status in fulfillment

## Status

Implemented as an explicit candidate for one isolated fixed-budget comparison. No new cloud capacity measurement yet.

## Context

ADR0279 issued 24,558 unique paid tickets within 300 seconds (81.86/s); 183 scheduled journeys were not dispatched. All 25,017 dispatched journeys completed after drain, without payment loss or double-booking. Payment-to-ticket worst-shard p95 was 10.08 seconds. Fulfillment lag reached 1,590 records while unpublished outbox averaged 12.36 records and peaked at 40. API CPU remained about 1.97 of four cores. This supports investigating fulfillment work, not adding API capacity.

The exact tested worker source matches the local source. It sequentially handles business events and refreshes committed order status after both OrderPaid and TicketsIssued. The run recorded 50,004 event cache fills and 100,068 consumer database acquisitions. The tested image already implements bounded snapshot reuse behind ORDER_STATUS_EVENT_REFRESH_DEDUP, disabled in the baseline. These observations identify redundant work; they do not prove that reuse alone will remove the fulfillment bottleneck.

## Decision

Enable only ORDER_STATUS_EVENT_REFRESH_DEDUP=1 on the six fulfillment consumers in one fresh 84 offered journeys/s, 300-second candidate. Retain ADR0279's native database bridge, exact application image, four API pods, fourteen worker pods, CPU/memory allocations, physical PostgreSQL connection cap 24, admission 20, consumer pool budget 48, generator concurrency 500, payment simulator delays and recovery policy. Compare against the retained ADR0279 result; it is a failed quality baseline, not a passing control.

Reuse applies only to TicketsIssued after a successful committed OrderPaid refresh that produced a fulfilled order with tickets. It requires the same customer-owned snapshot fingerprint to still be available in Redis within the original one-second freshness deadline. Reuse does not republish, extend TTL or bypass the consumer inbox transaction. OrderPaid replay and refunds always invalidate the hint and read committed state. The hint table is process-local and capped at 1,024 entries. Keep the default disabled outside the explicit ADR0280 candidate. Bind the setting, actual worker environments and decision hash before load; reject missing or inconsistent flags.

## Alternatives

- Parallel fulfillment within a partition: potentially higher throughput, but requires new ordering, executor lifecycle and replay validation. Defer until this existing bounded optimization is measured.
- Increase API or worker resources: remaining API CPU headroom and repeated reads favor reducing work first; unchanged resources preserve comparability.
- Longer cache freshness or unconditional skipping: reject because that would weaken freshness or fail to repair missing snapshots.
- Publisher batch tuning: the measured outbox remains small; it is not the best first correction for the remaining lag.

## Consequences

Eligible event pairs save one database read transaction and redundant Redis publication, at the cost of a Redis lookup and bounded hint state. Pairs delayed more than one second still perform the original read; improvement may therefore be limited on congested partitions. Kafka partition order, manual offset commits, atomic holds, financial writes, uniqueness, customer authorization and expiry remain unchanged.

## Failure and recovery behavior

Cache loss, changed snapshots, failed lookup/publication, expired hints and worker restart fall back to the existing committed read. Financial commit failure still propagates before advisory publication. No offset is acknowledged solely because a cache hint exists. Existing lost-response/idempotent customer recovery remains unchanged. Preserve failed evidence; stop load escalation on any failed gate. Drain both event lanes, audit the actual dispatched cohort after expiry, remove owned resources and restore the original disabled setting. Rollback is the same image with the flag set to 0.

## Supersession

Supersedes the disabled consumer refresh-reuse setting only for the explicitly selected ADR0280 comparison. Extends ADR0279 and ADR0271; does not supersede their retained evidence, financial guarantees, TTL or messaging semantics.

## Validation evidence

Executed offline: 110 focused regressions passed, including candidate flag/source/budget rejection and the existing reuse regressions. Ruff passed and the sealed overlay verified all 535 historical inputs without changing them. Existing reuse regressions cover fingerprint changes, ownership, expiry, lookup errors, refund invalidation, replay and restart. New candidate binding tests must reject settings/source/budget drift. The cloud comparison must report actual deduplicated reads, event cache fills, DB acquisitions, consumer lag, customer latency, first-attempt and final errors, undispatched journeys, inside-window unique paid tickets, payment durability, double-booking and complete queue drain separately. No hourly qualification from a failed short gate.
