# ADR 0006: Horizontal scaling with shared database authority

Date: 2026-09-06
Status: Accepted (records the existing implementation)

## Context

Process-local locks and deduplication cannot protect seats across API replicas.

## Decision

Keep correctness in shared PostgreSQL row locks and unique constraints. Use shared Redis admission and durable idempotency. Bound each process admission and connection pools; PgBouncer bounds backend connections. Lease work in SQL and deduplicate consumer effects.

## Alternatives considered

Process-local seat locks and unbounded queues/connections were rejected.

## Consequences and failure handling

Replica counts multiply per-process admission and pool demand: tune aggregate limits against measured database capacity. Scaling replicas does not establish a throughput guarantee. Current Compose uses single infrastructure instances and is not HA. Multi-process contention tests exercise shared ownership; production load, failover and capacity validation remain necessary.

## Evidence

See [reservation persistence](../../src/ticketing/infrastructure/reservations.py), [database transactions](../../src/ticketing/infrastructure/postgres.py), [Redis adapter](../../src/ticketing/infrastructure/cache.py), [workers](../../src/ticketing/workers.py), [schema](../../migrations/001_initial.sql) and [integration tests](../../tests/integration).

