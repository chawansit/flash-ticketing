# ADR0236: Stage the paid backend schema before runtime promotion

Status: Accepted for repository integration; no cloud deployment authorized.

## Context

Documentation PR #4 records measured results from a frozen experimental runtime. Main does not contain that runtime's later schema or orchestration. Merging the entire experimental branch would introduce hundreds of commits and unrelated changes. Current working source also differs from the tested image, so its Git revision alone is insufficient runtime provenance.

## Decision

Promote four unchanged additive migrations, 007 through 010, in a standalone prerequisite PR based on main after PR #4. Keep all existing application source, default configuration, connection budgets, Redis holds, TTL, financial transactions and Kafka delivery behavior unchanged. Introduce nonunique booking/order lookup indexes, an unfinished-payment partial index, and the durable webhook-receipt schema. The receipt schema alone does not enable asynchronous callback acknowledgement or start a confirmation worker.

Retain PostgreSQL's unique(event_id, seat_id) booking constraint and unique ticket booking_id. The payment-dispatch predicate is deliveries < target_deliveries, including successful payments with duplicate callbacks still outstanding. The receipt identity is (provider, callback_id); payload collision handling, lease recovery and capacity admission belong to the subsequent runtime PR. Do not claim these algorithms are implemented by tables alone.

Review subsequent changes in dependency order: (1) this schema foundation, (2) source-pinned API and workers with their correctness tests and default/feature-flag review, (3) CCE deployment and bounded-runner integration. Resolve each runtime file against retained image/source hashes, not current checkout similarity. Main merge of each implementation PR requires separate approval.

This integration decision adopts existing experimental schema choices for main and does not supersede seat authority, outbox, payment idempotency or Kafka semantics. Historical decisions and evidence remain at the pinned source revision linked below.

## Alternatives

- Merge the experimental branch wholesale: rejected because unrelated runtime, benchmark and infrastructure changes would be difficult to review together.
- Copy current source as the measured runtime: rejected because retained source hashes differ.
- Promote runtime before schema: rejected because queries could reference absent objects.
- Ship schema and all deployment tooling in one PR: rejected because schema compatibility can be validated independently.

## Consequences

Existing code can run against the expanded schema without activating new features. Indexes incur storage and write maintenance. The receipt tables remain unused until a reviewed runtime explicitly enables admission and processing. No new capacity result follows from this repository integration.

## Failure and recovery behavior

Test both a fresh database and an upgrade from migrations 001â€“006 with existing paid/unfinished rows. Run ordinary migrations only during local/fresh-database setup or an appropriately planned maintenance window: ordinary CREATE INDEX can block writers. A populated production database requires separately reviewed concurrent index construction, exact index-definition verification and bounded lock/statement timeouts; this PR does not deploy or implement that online operation.

Application rollback to pre-promotion code can retain these additive objects. Never drop durable receipt rows as an automatic rollback; once enabled they may contain financially significant unfinished work. IF NOT EXISTS permits migration replay but does not validate incompatible pre-existing objects. Unknown schema drift blocks a future deployment pending catalog verification. Tests must preserve existing rows, seat uniqueness and ticket uniqueness, and prove transactional receipt rollback.

## Validation evidence

Pending before implementation: run the main-based backend suite with isolated local PostgreSQL and Redis, plus fresh/upgrade/replay, index-definition, duplicate-callback eligibility, multi-seat lookup, receipt identity/state and rollback checks. No cloud traffic, cloud DDL or new performance measurement is authorized by this PR. Executed results will be appended before publication.

Historical decisions: [unfinished payment index](https://github.com/chawansit/flash-ticketing/blob/750ad8a4bf680a8440c001f2300b7fa8d56d8e77/docs/adr/0098-index-unfinished-payment-deliveries.md), [booking lookup index](https://github.com/chawansit/flash-ticketing/blob/750ad8a4bf680a8440c001f2300b7fa8d56d8e77/docs/adr/0099-proposed-booking-order-lookup-index.md), [durable confirmation](https://github.com/chawansit/flash-ticketing/blob/750ad8a4bf680a8440c001f2300b7fa8d56d8e77/docs/adr/0160-durable-asynchronous-payment-confirmation.md), [event order index](https://github.com/chawansit/flash-ticketing/blob/750ad8a4bf680a8440c001f2300b7fa8d56d8e77/docs/adr/0224-index-orders-by-event-for-paid-cohort-queries.md).

Executed local validation: 230 tests passed and two full-stack HTTP/Kafka tests were skipped because no API/Kafka stack was started. The 18 new schema checks passed against fresh and populated upgrade schemas on native Linux PostgreSQL 17.6; the existing Redis, concurrent-hold, payment durability and duplicate-fulfillment regressions also ran with Redis 7.4.5. Ruff passed. Local containers and network were removed. Full-stack GitHub CI remains required before main integration. No cloud DDL, traffic or capacity measurement occurred. [Sanitized validation and exact migration provenance](../capacity/paid-schema-foundation-2026-10-09.json).
