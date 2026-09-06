# ADR 0001: PostgreSQL authority and Redis admission

Date: 2026-09-06
Status: Accepted (records the existing implementation)

## Context

A Redis lease cannot atomically commit SQL records, and can expire while a process is still running.

## Decision

PostgreSQL owns durable events, inventory and holds. Lock sorted seat rows with FOR UPDATE NOWAIT and validate expiry using database time after acquiring locks. Commit hold, pending order, inventory, idempotency response and outbox together. Redis uses an owner-token lease only to reduce contention.

## Alternatives considered

Redis-only locking and distributed two-phase commit were rejected: correctness must survive Redis loss without a cross-store transaction.

## Consequences and failure handling

Redis acquisition is not a successful hold. On SQL failure the transaction rolls back and the shield context releases only its own token. If release fails or the process dies, its 2-second lease expires. Never recreate SQL ownership from Redis. If commit acknowledgement is lost, the outcome is unknown: retry the same actor, operation and idempotency key to discover/replay the committed result. Do not assume rollback or blindly create a new hold. Seat maps are derived only from committed SQL through event-driven and periodic snapshots; version checks reject older snapshots. Cache state may lag; SQL always revalidates. A cache miss returns 503 until rebuilt.

## Evidence

See [reservation persistence](../../src/ticketing/infrastructure/reservations.py), [database transactions](../../src/ticketing/infrastructure/postgres.py), [Redis adapter](../../src/ticketing/infrastructure/cache.py), [workers](../../src/ticketing/workers.py), [schema](../../migrations/001_initial.sql) and [integration tests](../../tests/integration).


## Validation on 2026-09-06

- 16 unit tests passed (existing dependency deprecation warnings and a local pytest cache permission warning).
- 3 PostgreSQL/Redis integration cases passed: both SQL rollback cases in test_persistence_failure.py and the existing versioned-cache test. No integration skips.
- Injected a real SQL error after hold/order/inventory writes with Redis admission already acquired. Verified rollback of holds, orders, items, idempotency and outbox, unchanged inventory/cache, lease release or expiry after injected release failure, and successful same-key retry/replay.
- Ruff passed for the new test. This does not simulate ambiguous COMMIT acknowledgement or process termination; those recovery paths are design reasoning, not newly tested claims.
