# ADR 0017: Bounded, Redis-validated serialized browse responses

Status: Accepted; local correctness passed, cloud measurement pending.

## Context
The isolated-reconciliation 352-RPS run had 83 ADMISSION_FULL responses, dispatch
p95 45.218 ms, and transaction-body p95 31.514 ms. It does not prove a single cause.
Availability HTTP 200 responses still fetch and decode 300 seat JSON objects,
sort/project them and serialize the same version for each independent viewer.
Reducing this repeated read work is a bounded experiment, not an admission fix claim.

## Decision
Keep a per-RedisSeats-instance LRU of immutable serialized browse bodies, keyed by
event and representation, with at most 2,048 entries and 32 MiB of retained body bytes.
On every request pass the locally retained ETag alongside the client's validators
to the existing atomic Redis browse script. A matching client validator yields HTTP
304. A matching local validator permits HTTP 200 with the retained body. Otherwise
fetch and serialize the atomic Redis representation, then replace the LRU entry.
Never serve from the LRU without a successful Redis validation; no TTL extension.
A lock protects only LRU bookkeeping, never Redis I/O or serialization. Concurrent
misses may duplicate bounded work; do not introduce waiting queues or single-flight
locks. Oversized bodies are returned but not retained. Request-local references
can temporarily outlive eviction; the byte bound describes retained cache payloads,
not total process RSS. Endpoint response shapes, ETags and OpenAPI remain unchanged.

This extends ADR 0010's read strategy; Redis validation, fail-closed reads and
PostgreSQL ownership authority remain accepted. Hold locking, TTL, admission,
payment idempotency and outbox/Kafka delivery semantics are unchanged.

## Alternatives
Raising admission admits more work during stalls without reducing read CPU.
Unvalidated local TTL caches can hide Redis expiry or newer versions. Serializing
all representations in Redis increases single-threaded Redis write work. Adding
API processes changes pool/admission budgets. Start with reuse behind existing
atomic validators; compare on the same hardware before scaling.

## Consequences and horizontal scaling
Each API process has an independent bounded cache and cold-start cost. No cache
invalidation broadcast is required because every response is Redis-validated.
Eviction reduces hit rate, not correctness. Fixed-cardinality counters distinguish
client 304, local reuse and serialization; retained bytes/entries are gauges.

## Failure and recovery
Redis outage, missing map or interrupted projection still returns 503 even with a
local body. Rebuild incarnation changes invalidate availability validators. A
concurrent older fill may replace a newer LRU entry but the next Redis validation
rejects that version; the in-flight response remains consistent at its Redis read.
Restart or rollback simply discards the optimization; no migration or data cleanup.

## Validation evidence
Executed rebuilt full Compose suite: 84 passed, two dependency warnings, no skips.
Coverage includes HTTP conditional/update/expiry/outage behavior, concurrent encoded
version/body consistency, bounded eviction and eviction during validation. Ruff passed.
Same-machine load comparison remains pending; no capacity improvement is claimed.
