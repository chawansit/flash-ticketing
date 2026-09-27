# Redis-first Huawei cloud smoke — 2026-09-27

This isolated correctness check exercised the opt-in ADR 0058 path on the existing Huawei
topology. It was not a capacity test. The deployment used four API replicas, local PgBouncer
with verified TLS to Huawei RDS, Huawei DCS Redis 7 master/standby and one temporary reservation
writer.

## Preconditions

- PgBouncer used `verify-full`; a query through the application pool reported TLS 1.3,
  `TLS_AES_256_GCM_SHA384` and 256-bit encryption.
- All four API replicas were healthy.
- DCS reported master role, one connected replica, AOF enabled and `noeviction` in the earlier
  same-day preflight.
- A temporary write received one replica acknowledgement within the configured 100 ms window.
- Fresh development-only fixtures and one-hour credentials were generated inside the API
  container and were never copied into this report.

## Results

The single-command smoke returned HTTP 202/PENDING, reached DURABLE and replayed with the same
command, hold and order IDs. PostgreSQL contained exactly one matching command, hold, order,
order item, seat owner and transactional outbox event. Redis stream length and pending count
were zero after persistence.

The 100-actor race used one fresh seat and no retries:

| Check | Result |
|---|---:|
| HTTP 202 winners | 1 |
| HTTP 409 `SEAT_UNAVAILABLE` | 99 |
| Transport/unexpected errors | 0 |
| Winner persistence state | DURABLE |
| Matching command/hold/order/seat rows | 1/1/1/1 |
| Stream length after drain | 0 |
| Pending entries after drain | 0 |
| Slowest response | 337.204 ms |

The slowest-response value came from a short ECS-local contention probe and must not be treated
as a production percentile or capacity result.

## Rollback and limitations

All registered reservation streams were empty before rollback. Runtime returned to
`RESERVATION_MODE=postgres`, the reservation writer stopped, all four API replicas were healthy
with zero restarts and five consecutive readiness checks passed. Runtime logs inspected after
rollback contained no error-level entries, and temporary credential manifests were removed.

This run did not force a DCS primary failover, restart the writer with pending work, exercise the
distributed generator, or measure 1,000 RPS. Production activation and any higher-capacity claim
remain blocked on those gates.
