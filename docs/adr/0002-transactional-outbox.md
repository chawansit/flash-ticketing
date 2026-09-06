# ADR 0002: Transactional outbox

Date: 2026-09-06
Status: Accepted (records the existing implementation)

## Context

Committing payment or seat state and independently sending Kafka messages creates a dual-write failure window.

## Decision

Persist business mutations and outbox rows in one PostgreSQL transaction. Publishers lease rows, commit the lease, publish stable event IDs and mark publication only after broker acknowledgement.

## Alternatives considered

Direct publishing inside or after the business transaction was rejected because either side can succeed independently.

## Consequences and failure handling

Broker outages accumulate durable work. A crash after acknowledgement can publish duplicates. Lease fencing prevents stale workers from acknowledging another lease. Monitor backlog and retain/replay failed work. This pattern already supports later-sprint payments; it does not imply new Sprint 1 payment requirements.

## Evidence

See [reservation persistence](../../src/ticketing/infrastructure/reservations.py), [database transactions](../../src/ticketing/infrastructure/postgres.py), [Redis adapter](../../src/ticketing/infrastructure/cache.py), [workers](../../src/ticketing/workers.py), [schema](../../migrations/001_initial.sql) and [integration tests](../../tests/integration).

