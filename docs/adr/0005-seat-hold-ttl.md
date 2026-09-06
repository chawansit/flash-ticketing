# ADR 0005: Seat hold expiry

Date: 2026-09-06
Status: Accepted (records the existing implementation)

## Context

Inventory must become available after abandonment even if cleanup workers stop.

## Decision

Persist a default 120-second hold deadline in PostgreSQL. Use clock_timestamp after locks. Reclaim logically expired inventory during reservation; cleanup is housekeeping. The independent Redis contention lease lasts 2 seconds and cache snapshots last 30 seconds.

## Alternatives considered

Redis key expiry as business ownership and cleanup-only expiry were rejected because caches/workers may fail or lag.

## Consequences and failure handling

Payment initiation and idempotent replay do not extend the deadline. Cleanup and callbacks clear only their own hold ownership. Late successful payment follows refund processing. Cached availability may lag logical expiry; clients receive the deadline and SQL arbitrates. Changing TTL requires a new or superseding ADR and boundary/race tests.

## Evidence

See [reservation persistence](../../src/ticketing/infrastructure/reservations.py), [database transactions](../../src/ticketing/infrastructure/postgres.py), [Redis adapter](../../src/ticketing/infrastructure/cache.py), [workers](../../src/ticketing/workers.py), [schema](../../migrations/001_initial.sql) and [integration tests](../../tests/integration).

