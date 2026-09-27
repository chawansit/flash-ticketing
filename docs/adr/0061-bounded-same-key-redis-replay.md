# 0061: Resolve ambiguous Redis intake by bounded same-key replay

## Status

Accepted for implementation on 27 September 2026. This decision refines the unknown-outcome contract in [ADR 0058](0058-redis-first-durable-reservation-intake.md), the retry rules in [ADR 0049](0049-bounded-idempotent-retry-diagnostic.md), and the failover recovery work in [ADR 0060](0060-bounded-redis-writer-failover-recovery.md). Redis-first production activation remains blocked until the managed-DCS repeat passes the gates below.

## Context

The bounded-writer DCS switchover repeat persisted all 45 Redis commands within the hold lifetime, but two requests returned HTTP 503 while their atomic Lua commands survived failover and later became durable in PostgreSQL. The API classified pipeline connection failures as `ADMISSION_UNAVAILABLE`, and the load client did not recognize `RESERVATION_DURABILITY_UNKNOWN` as a known retryable result. Consequently the client could stop after a response that did not say whether the command had committed.

The idempotency record and immutable response are written in the same Redis Lua operation as provisional seat ownership and the stream command. Replaying the same actor, event, payload and idempotency key therefore returns the original command if it exists, creates it once if the first attempt never executed, or rejects a changed payload. Replaying with a new key is unsafe.

## Decision

Any Redis error raised while dispatching the atomic reservation intake is reported as HTTP 503 `RESERVATION_DURABILITY_UNKNOWN`. The response retains the existing `Retry-After: 1` header.

Clients may make a bounded replay of the identical hold request using the same actor token and idempotency key. The load-validation client will recognize the unknown-outcome code and include it in its existing bounded exponential-backoff policy. Retry remains opt-in and reports first-attempt failures, physical attempts, recovered outcomes and exhausted outcomes separately.

The API will not blindly execute an internal write retry and will not return HTTP 202 merely because the Lua call may have executed. Redis Lua idempotency resolves the replay. The existing command-status endpoint remains available after the client receives a command ID; same-key replay is the recovery path when the original response did not contain that ID.

## Alternatives considered

- **Return HTTP 202 after a connection error.** Rejected because neither execution nor replica acknowledgement is proven.
- **Automatically rerun the Lua write inside the API request.** Rejected because it hides additional attempts, extends admission occupancy during failover and couples server latency to the outage duration.
- **Look up the idempotency key repeatedly inside the API.** Deferred because it creates synchronous waiters during a shared Redis outage. A bounded client replay honors backpressure and the existing `Retry-After` contract.
- **Retry with a new idempotency key.** Rejected because an earlier committed command could then reserve another seat or create a conflicting ownership attempt.
- **Treat the committed 503 records as harmless audit extras.** Rejected because an unresolved client can make an incorrect business decision even when no seat is double-booked.

## Consequences

A managed failover can add a small number of physical requests above offered logical load. Every replay remains observable and uses the same immutable identity. Clients that ignore the recovery contract may still receive an unresolved 503, so SDK and frontend work must preserve the same key until a terminal result is known.

The distinction between pre-intake unavailability and post-dispatch ambiguity becomes explicit. Some connection failures that occurred before Redis executed anything will conservatively use the unknown-outcome code; same-key replay is safe in that case.

## Failure and recovery behavior

After an unknown result, the client waits according to the bounded backoff and replays the identical request and key. If the first Lua operation committed, Redis returns its stored command and response. If it did not commit, one new command is created. If the payload differs, Redis returns `IDEMPOTENCY_MISMATCH`. If all attempts are exhausted, the client keeps the result unresolved and must not create a replacement key. Writers persist at least once; PostgreSQL command and idempotency uniqueness collapse replay.

## Validation evidence

Implementation must add unit coverage proving that a Redis pipeline failure is classified as `RESERVATION_DURABILITY_UNKNOWN`, that the load client recognizes and retries that code, and that the retry reuses the exact key. The existing Redis-first integration suite must still show one winner for 100 concurrent requests and safe same-key replay.

Cloud validation starts with a low-rate real DCS switchover using bounded same-key retries. It passes only if every logical hold finishes with one accepted command, all accepted commands become durable, no unmatched durable command remains, overlap is zero, queues drain, first-attempt failures and extra physical attempts are reported, and rollback succeeds. Higher-rate testing remains prohibited until this correctness drill passes.

Implementation validation before deployment: Ruff passed for all changed Python files; the complete unit suite passed 146 tests; and the Redis-first PostgreSQL/Redis integration file passed all nine tests, including the 100-contender same-seat race. The new unit cases prove pipeline errors use the unknown-outcome code and the load client replays the identical key while preserving first-attempt evidence.
