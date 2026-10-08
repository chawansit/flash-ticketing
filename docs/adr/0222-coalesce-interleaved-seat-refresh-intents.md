# ADR0222: Coalesce interleaved seat refresh intents

Status: Proposed

## Context

The failed ADR0219 84/s probe recorded 128,618 consumer database transactions in a 300.884-second bracketing window. Consumers spent 474.817 seconds across six lanes in 32,849 SeatsChanged handling transactions, while paid-unfulfilled orders peaked at 374. Each consumer partition spent almost the full window processing. The writer is independently saturated: three lanes spent 898.510 seconds busy and PostgreSQL batches averaged 125.266 ms. This change addresses redundant consumer work; it does not claim to solve writer throughput or qualify 84/s.

Current consume_events flushes accumulated seat refresh intents before every business event. Interleaved OrderPaid and TicketsIssued events prevent useful coalescing, although refresh intents describe the latest committed inventory and are not authoritative seat ownership or payment state.

## Decision

Coalesce all SeatsChanged intents within the existing bounded Kafka partition batch, including intents separated by business events. Process business events in their original relative order with their existing individual inbox/handler transactions. Persist the combined refresh intents once at batch end, before returning to the Kafka offset commit. Keep every refresh event identity in the existing inbox, union changed seats by show, and retain full-refresh precedence and generation fencing. Acquire per-show refresh row locks in deterministic show-ID order to prevent cross-partition batches with opposite input orders from deadlocking.

This supersedes ADR0070's consecutive-refresh-only implementation restriction. It retains ADR0070's at-least-once delivery and individual business-event transaction boundaries. ADR0084's OrderPaid batching proposal remains unimplemented. There is no change to hold authority, locking, payment idempotency, TTL, total connection budget or worker count.

## Alternatives

- Add consumers: existing six partitions already have six consumers, with added database concurrency risks.
- Batch business transactions: potentially useful later, but expands locking and poison-record scope; isolate the simpler redundant-refresh correction first.
- Increase generator concurrency: does not remove backend work or reduce the measured waits.
- Relax latency or diagnostic gates: rejected; original failed evidence remains failed.

## Consequences

A mixed partition batch uses one refresh transaction instead of one per consecutive refresh group. Refresh scheduling can be delayed until the current bounded batch finishes; customer freshness gates must be measured unchanged. There is no claim of a fixed speedup. Writer saturation may remain the next bottleneck.

## Failure and recovery behavior

Kafka offsets are committed only after business handlers and deferred refresh persistence finish. If a business handler or refresh transaction fails, the batch is retried. Already committed business inbox rows make replay safe; unpersisted refresh identities remain replayable. Existing bounded retries and poison-message isolation remain unchanged. A process stop before offset commit replays deferred intents. Full refresh wins over partial refresh, and stale projections cannot overwrite newer versions. Roll back by restoring the prior consumer image; audit tickets, payments and full queues before new load.

## Validation evidence

Implemented locally. Final current-code suite: 131 passed in 14.72 seconds. Exact frozen consumer candidate: 50 passed in 8.36 seconds, with its source import independently verified. Both suites used isolated local PostgreSQL 17.6 databases, which were removed afterward. Tests cover interleaving, business order, replay, refresh failure after committed ticket work, full-refresh precedence and two simultaneous overlapping batches with opposite show orders (40 batches/80 retained event identities, both workers progressed). The exact patch changes only workers.py against ADR0163; all other runtime source hashes match the parent. See [local evidence](../capacity/flash-sale-opening/interleaved-seat-refresh-local-2026-10-08.json). Candidate cloud comparison must use exactly one consumer-source change against the frozen image, the same 84/s workload, budgets and durations, plus unchanged correctness/freshness/queue gates. The existing SWR candidate includes unrelated simulator changes and must not be used silently as a matched candidate. No cloud improvement or hourly capacity is claimed.
