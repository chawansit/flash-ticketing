# ADR 0003: Kafka at-least-once delivery

Date: 2026-09-06
Status: Accepted (records the existing implementation)

## Context

Kafka offsets and PostgreSQL business effects cannot share the current application transaction.

## Decision

Use at-least-once delivery. Commit the consumer inbox and transactional effects together before committing the Kafka offset. Stable event IDs and uniqueness constraints deduplicate effects.

## Alternatives considered

End-to-end exactly-once claims and offset-first processing were rejected. Kafka transactions alone do not atomically include PostgreSQL.

## Consequences and failure handling

Duplicates are expected. After five failed attempts persist a dead letter before advancing the offset. Redis refreshes are retryable projections, outside SQL atomicity. Partition keys do not guarantee publication order across publishers; rebuild current snapshots and reject older versions. Operators must monitor and replay dead letters.

## Evidence

See [reservation persistence](../../src/ticketing/infrastructure/reservations.py), [database transactions](../../src/ticketing/infrastructure/postgres.py), [Redis adapter](../../src/ticketing/infrastructure/cache.py), [workers](../../src/ticketing/workers.py), [schema](../../migrations/001_initial.sql) and [integration tests](../../tests/integration).

