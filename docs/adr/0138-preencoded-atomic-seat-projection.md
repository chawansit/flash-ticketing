# ADR 0138: Pre-encoded payloads for atomic version-fenced seat projection

- Status: Accepted for local implementation and validation; cloud use gated
- Date: 2026-10-04

## Context

ADR0135's18,000-seat fixture warmup fails the unchanged100ms Redis socket deadline. ADR0136's bounded-command-only candidate passed49 local contracts but still failed both large-show checks:139.975ms EVAL versus147.624ms original on the same data. It was rejected and original source restored.

Separate fresh-map prefix probes put original decode at41.678ms, decode-plus-selection91.424ms and decode-plus-selection-plus-writing125.522ms. These are separate invocations, not exact phase spans. The bulk candidate's selection prefix43.629ms still grew to104.602ms after writes. Both JSON processing and per-seat command work remain candidates for optimization. Do not increase the deadline or skip warmup.

## Decision

Serialize incoming seat payloads in Python before entering Redis. Transfer each input as seat ID, source version and a complete JSON-object fragment without its aggregate version. The Lua script decodes the small row envelope, retains strict per-seat source-version selection, and appends the final aggregate version to the selected incoming JSON fragment. Retained old states are decoded for fencing and re-encoded only when dirty repair requires rewriting them.

Use bounded256-field HMGET/HSET groups from the rejected ADR0136 experiment as part of this new candidate. Keep one whole-map atomic Lua invocation. Record delta entries from the exact encoded strings written to the hash and a cjson-encoded numeric version header, avoiding another full seat-object encoding pass. Do not manually encode arbitrary string values in Lua: Python JSON encoding owns strings/objects, Lua adds only the cjson-encoded numeric aggregate and JSON punctuation.

This changes the internal EVAL argument representation and serialization placement; stored hash/delta JSON semantics and API representations remain compatible. Partially supersede ADR0009's Lua encoding/individual-command execution only if locally qualified. Source-version, aggregate, updating-marker, reader atomicity, layout ETag, incarnation, history and TTL contracts remain. ADR0136 remains rejected as a standalone factor.

## Persistence, locking, messaging, idempotency, TTL and scaling

PostgreSQL stays financial/inventory authority. No schema, SQL, lock order, hold writer fairness, payment/callback/inbox/outbox idempotency or Kafka delivery change. Preserve original100ms Redis operation deadline, all TTLs, finite durable refresh leases, and all API/worker/client/server budgets. Snapshot construction costs move to Python before socket I/O; client wall time must be reported separately from Redis server time. Full inventory memory and work remain bounded by the fixture limit and O(inventory), not constant time.

Existing and new code still store source-version-fenced seat JSON and can read the same hash/deltas. The private EVAL arguments are coupled to each caller's script and are not a cross-version persisted protocol. No scaling decision or capacity claim follows from a local pass.

## Alternatives and consequences

Larger Redis timeouts or skipped warmup alter/fail the gate. Redis publication across multiple calls needs a new generation/reader/patch-merge protocol and is deferred. Writing a new shadow hash and renaming without merging intervening patches could regress fresh seat states and is rejected. Pre-encoding only a cold fast path creates another publication branch and does not address reconciliation/repair. Keep one representation for full and patch calls.

Python serialization adds client CPU and an escaped outer JSON envelope; measure end-to-end and Redis cost. Incoming values retain normal JSON escaping, including Unicode/quotes. Aggregate version is assigned only after complete source selection. Bounded command groups exclude Lua unpack limits and prevent a single huge argument expansion.

## Failure and recovery

The existing marker precedes writes and remains if group publication fails. Browse/delta readers reject incomplete hashes and patches reject dirty or missing maps. Full reconciliation uses retained per-seat source versions and repairs missing/stale metadata without regression. Lost acknowledgement remains an error; durable leases replay safely. Replay emits no extra financial effect or duplicate version delta. Preserve patch TTL behavior and new incarnation/history reset after cache loss.

No cloud execution after failed local gates. A future production cache-factor control needs separate unchanged-load qualification against passing ADR0133 before a concentration-only comparison. Retain all financial/postTTL/zero-double-booking/full-keyspacequeue/Kafka gates, rollback/cleanup, and no higherload or mainmerge on failure.

## Validation evidence

At decision time this representation is not implemented/tested. [Rejected prior candidate and comparison](../capacity/flash-sale-opening/bulk-seat-projection-local-validation-2026-10-04.json). Required local evidence:256 boundaries and18k original-deadline prewarm, stale full/concurrent patch, interrupted publication repair, acknowledgement replay, Unicode/JSON escaping, exact history/incarnation/TTL behavior, signed large-show financial replay and full concurrency/financial regressions. Retain failed initial attempts. Generator native-filesystem qualification under ADR0137 is a separate gate and cannot qualify Redis or cloud financial queues.
