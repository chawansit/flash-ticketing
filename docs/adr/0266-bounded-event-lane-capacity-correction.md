# ADR0266: Bounded event-lane capacity correction

## Status

Accepted for the authorized bounded correction; cloud validation pending. Applies ADR0265 to one isolated paid fixture. Historical ADR0263 evidence remains failed and unchanged. Does not authorize main merge or assert capacity improvement.

## Context

The previous 84 offered journeys/s, five-minute run had 478 undispatched journeys, payment-to-ticket p95 6.14 seconds and 24 seat-refresh lock failures. All 24,722 dispatched journeys ultimately produced unique paid tickets after recovery. Shared event-group progress is a measured dependency; its share of the latency remains uncertain.

## Decision

Use the existing proven CCE paid runner for one fresh 84 offered journeys/s, 300-second correction. Deploy the immutable ADR0265 shared image with separation enabled for six fulfillment consumers and one projection consumer on the existing primary ECS. Rebalance local consumer pools from 6 x 8 = 48 to 6 x 7 + 1 x 6 = 48. Keep PgBouncer at 24 physical server connections, API admission at 20, four CCE APIs at 1 vCPU/2 GiB each with four connections (two payment/two general), simulator delivery concurrency 16 and SQL pool 10, the existing gateway latency, two generator shards and 500 active-journey slots unchanged. Other worker counts/settings remain unchanged. This adds one process on the existing host, not another machine; report its CPU cost and duplicated Kafka decoding as consequences of isolation.

Retain actual immutable sources, commands, explicit separation/pool settings and both group identities in inventory. Use a separately named read-only observer for ticketing-seat-projection-v1 so its supervised job cannot collide with the fulfillment observer. Require startup ownership of all six partitions, six fulfillment members and one projection member, complete measurement coverage and zero final lag in both groups. Mandatory queue drain also checks durable refresh work, reservation streams, outbox, callbacks, refunds and payment receipts. A historical structural adapter may remove the new role only after validating the complete actual sealed inventory; retained evidence remains unmodified.

Reuse current customer latency/error/drop gates, bounded recovery, zero double-booking and zero payment loss. Report first-attempt errors, retries, final errors, offered/dispatched journeys, unique tickets and completion tail separately. Stop escalation on any failed gate. A 100/s test remains conditional on all short-stage gates passing and requires its own bounded experiment identity.

## Alternatives

Reuse the one-group runner unchanged: rejected because it could hide projection lag. Add unbounded pools/replicas: rejected because it invalidates the comparison and may overload PostgreSQL. Replace the runner: rejected because the proven financial and restoration lifecycle already exists. Use fewer fulfillment consumers: changes partition parallelism alongside the correction.

## Consequences

Independent group progress may lower fulfillment latency; shared CPU/Redis/PostgreSQL contention can remain. A single projection consumer owns all six partitions and may itself bottleneck freshness. The failed historical baseline permits directional comparison but is not a passing qualification control. No improvement or production capacity is inferred before measurement.

## Failure and recovery behavior

Verify the projection worker and both groups before customer dispatch. Earliest replay is safe for the isolated fixture through the existing inbox. If either group is absent, has unknown ownership, fails observation or cannot drain, the experiment fails. Preserve failed evidence and owned fixture identity. Keep workers until acknowledged payments, tickets and projection work are reconciled. Drain both lanes before removing projection and restoring default mixed consumers and exact original routing/images. Restore checks inspect the normal one-member group independently; a dormant historical projection group does not qualify active split-mode drain. Never reset offsets or discard acknowledged financial work to obtain a pass.

## Validation evidence

ADR0265 executed 34 focused unit and 138 isolated integration tests, including real Kafka independent progress under a PostgreSQL refresh lock. This runner integration and cloud correction have not yet been tested. Record executed checks and the fresh cloud result here when available; do not overwrite ADR0263 failure.

Executed local validation: 131 affected runner/source/observer unit tests passed, one existing test skipped; all 63 historical transaction/profile/overlay unit tests passed. The new correction suite comprises 20 passing tests. Ruff and git diff whitespace checks passed. The 535-file historical reproduction contract and all 20 current overlay files verified. The immutable SWR configuration digest was checked directly, and the exact image was cached/source-verified on the primary ECS; generator idle confirmed. No buyer dispatch at this checkpoint.
