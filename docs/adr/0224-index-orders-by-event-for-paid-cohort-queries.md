# ADR0224: Index orders by event for paid cohort queries

Status: Proposed

## Context

ADR0223 attributes the diagnostic sampling gap to the existing paid-cohort query, peaking at 2,110.82 ms under paid traffic. Its idle read-only plan scans 4.32 million orders to find 25,168 owned orders. The orders scan costs 668.699 ms of 906.289 ms execution. The catalog confirms no orders event-id index. Existing payment/order and ticket/booking indexes already support the other joins. The retained 84/s reference dropped 32 of 25,200 offered journeys, completed every dispatched journey, restored runtime and drained queues. Its independent recovery receipt does not pass its failed qualification.

## Decision

Add one nonunique B-tree index, orders_event_id on public.orders(event_id). Keep queries, application images, consumer correction, transaction boundaries, locking, payment idempotency, TTL, polling, machine sizes and aggregate connection budgets unchanged. Do not include mutable status in the index: status changes should not require index updates solely for this diagnostic access path.

Register an orders_event_index_probe by reusing the proven paid workflow and the exact ADR0222 application artifact set. Compare a fresh 84 offered journeys/s, 300-second candidate against the retained ADR0223 timed reference. Bind the index specification, migration, immutable artifact maps and baseline/recovery hashes to the reservation. Deploy the index concurrently before safety and paid dispatch, with no paid work during index construction. Verify its exact table, columns, B-tree/nonunique semantics, valid/ready flags and lack of predicate/expressions before dispatch and after the stage. Reject an incompatible pre-existing index. No DDL is performed by the one-second observer. Preserve all qualification gates.

Provide the ordinary idempotent index migration for new/local databases. Use the explicitly bound concurrent form for the existing cloud database, outside a transaction, with a two-second lock timeout and 120-second statement timeout. Verify the inherited runtime and generator idle state. This uses existing storage and the single diagnostic action budget, before observers start; no extra connection budget or new infrastructure is introduced.

## Alternatives

- Relax coverage or reduce observation frequency: rejected; hides the measured delay or weakens the agreed gates.
- Rewrite cohort counts or infer them from cached customer results: rejected for this correction; index the unchanged authoritative query first.
- Cover id/status in the index: unnecessary for the measured scan; mutable status increases write amplification and prevents some HOT updates.
- Add API replicas or CCE capacity: does not address scanning unrelated historical orders and would change the comparison topology.
- Delete historical orders: rejected; financial history must not be removed for benchmark performance.

## Consequences

Event-scoped reads should avoid the full-history scan. The index consumes storage and adds maintenance to order insertion/event-id changes; measure write latency and paid outcomes unchanged. No fixed ticket-throughput gain is claimed. Writer saturation or periodic WAL contention may remain.

## Failure and recovery behavior

Persist an owned index intent before DDL and retain its outcome. An invalid/incompatible or ambiguous index stops load; verify ownership before any cleanup, never drop an unrelated object or retry ambiguous construction automatically. Preserve failed evidence, financial audits, queues and application restoration. The valid index is an intentional persistent schema correction, separately reported from restored application containers. If evidence shows a material write regression, prepare a separately bound rollback using DROP INDEX CONCURRENTLY only for the exact owned index, without weakening financial gates. The original baseline stays failed and consumed. Hourly qualification remains blocked until the short candidate passes every gate.

## Validation evidence

Implemented the additive migration and bound concurrent deployment through the existing paid runner. Executed 127 runner/observer/envelope/recovery tests and six additional strict index-proof tests. A fresh PostgreSQL 17.6 fixture with 50,000 historical and 100 cohort orders preserved every cohort count, replaced the orders full scan with an index scan, verified idempotent migration and status updates, and removed its owned database. These local results are [recorded here](https://github.com/chawansit/flash-ticketing/blob/8c244f033523a2d24aa16717a8c15f321abd113f/docs/adr/https:/github.com/chawansit/flash-ticketing/blob/8c244f033523a2d24aa16717a8c15f321abd113f/docs/capacity/flash-sale-opening/orders-event-index-local-2026-10-08.json). Cloud comparison remains pending. The existing sanitized attribution and independent recovery are [recorded here](../capacity/flash-sale-opening/paid-observer-timing-probe-2026-10-08.json). No index deployment or capacity improvement under this decision has been measured yet.

The ordinary migration uses the caller search path, matching the existing migration convention and isolated test schemas. The concurrent cloud operation remains explicitly bound to public.orders. This portability correction does not change the deployed cloud index or replay the consumed experiment.

Executed cloud result: safety passed. The fresh unchanged 84/s, 300-second paid candidate completed 24,423 dispatched journeys with zero customer failures or duplicate bookings; 777 offered journeys were undispatched. Peak cohort-query time fell from 2,110.82 to 314.21 ms and sampling gaps fell to 1.005413 s; every monitoring gate passed. Customer/count gates failed, and hold-to-ticket p95 worsened from 5,548.98 to 9,648.02 ms. There is no paid-throughput improvement claim. Index validity/identity and full original runtime restoration passed. The independently verified recovery reconciled all 24,423 payments/bookings/tickets and relationships, global duplicates/queues/Kafka and the persistent index; original failed gates remain failed. [Cloud comparison](../capacity/flash-sale-opening/orders-event-index-probe-2026-10-08.json), [recovery receipt](../capacity/flash-sale-opening/orders-event-index-recovery-2026-10-08.json).
