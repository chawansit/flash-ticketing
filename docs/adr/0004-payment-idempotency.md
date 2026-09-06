# ADR 0004: Payment and callback idempotency

Date: 2026-09-06
Status: Accepted (records the existing implementation)

## Context

Client retries, gateway retries and distinct callback IDs can describe the same payment.

## Decision

Persist actor/operation/key plus request hash and response in PostgreSQL. Reject changed requests using the same key. Keep one payment attempt per order; deduplicate callback IDs and lock/check payment and order state. Record successful booking or refund intent and outbox atomically.

## Alternatives considered

In-memory deduplication or deduplication by callback ID alone was rejected because replicas, restarts and new callback IDs bypass it.

## Consequences and failure handling

Previously successful payments cannot regress or create another booking. Late capture requests a refund rather than reclaiming inventory. Provider calls remain outside SQL locks. Current payments/refunds are simulations; real gateway reconciliation remains future work.

## Evidence

See [reservation persistence](../../src/ticketing/infrastructure/reservations.py), [database transactions](../../src/ticketing/infrastructure/postgres.py), [Redis adapter](../../src/ticketing/infrastructure/cache.py), [workers](../../src/ticketing/workers.py), [schema](../../migrations/001_initial.sql) and [integration tests](../../tests/integration).

