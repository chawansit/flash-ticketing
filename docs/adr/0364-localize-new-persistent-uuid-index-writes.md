# ADR0364: Localize new persistent UUID index writes

## Status
Accepted for a fresh180 qualification after local validation. No cloud capacity gain claimed.

## Context
ADR0363 fixed projection (peak222events,last0; observer passed) but180/s failed:54000scheduled,47453dispatched/fulfilled,6547drops,1initial/recovered,0final; reservation durability p959114ms and writer PG batch mean82.224ms. The exact dispatched cohort was independently reconciled and restored; original raw hash b811cc4b377413c31debb8f35067d879c4c7371002375168f869abbad4c10543 remains unchanged. Eight recovery checks passed.

Read-only diagnostics found pglz WAL compression already enabled, full-page writes/synchronous commit on,4GiB shared buffers, and large restored relations:33.34million inbox rows,9.91million orders,8.05million reservation commands. The paid-window query deltas attributed970.8MB WAL to outbox inserts,690.9MB to reservation commands and481.9MB to orders. These observations suggest random UUID index writes are worth testing, not proof of the capacity cause. No database settings changed.

## Decision
Locally validate an opt-in PERSISTENT_UUID_V7=1 factory for NEW durable identifiers only, retaining default UUIDv4 behavior. Use RFC9562 UUIDv7 with48-bit Unix milliseconds and74 fresh cryptographic random bits, explicit version/variant bits, no shared counter or global lock. Apply only to reservation command/hold/order IDs, payment attempts, bookings, outbox events and tickets. Existing IDs, caller idempotency keys, actor authorization, schema, uniqueness constraints, lease/cache/HTTP/callback tokens and producer/consumer IDs remain. Do not use identifier timestamps for sale clocks, TTL, ordering correctness or authorization. Keep the validated ADR0363 projection pipeline; do not include ADR0362's failed claim experiment.

Generation is stateless: same-millisecond and backward-clock calls receive independent random bits. Cross-millisecond index locality is expected; strict monotonic ordering is not promised. Invalid48-bit timestamps fail before emitting an identifier. Callback authentication and lease/cache/HTTP tokens retain their original factories; callback delivery IDs continue to derive from the stable payment ID. No server, database/backend, pod count, CPU, memory, batch, client, admission, timeout or messaging scaling changes.

If local evidence justifies deployment, explicitly supersede ADR0355's same-image/rate-minus20 rule only for a new-image180 qualification against genuine clean original-parent180 on identical ADR0359 resources.200 must require a genuine clean new-image180 with the exact same enabled feature and resources. ADR0363's failed180 is diagnostic evidence, not an admission prerequisite.

## Alternatives
More writer concurrency may hide transport latency but can increase pressure on the observed WAL bottleneck. Disabling durable commit/full-page writes is rejected. WAL compression is already enabled. Reindexing/VACUUM FULL or changing stored IDs adds locking and recovery risk. Resource resize is outside the automatic work envelope. Another unmodified paid run has no corrective evidence.

## Consequences
New identifiers remain native UUID values and can coexist with old UUIDv4 values. Their approximate creation time becomes visible; they are opaque identifiers, not security secrets. Randomness is74bits per millisecond rather than UUIDv4's122global random bits. Existing unique constraints remain authoritative. Local UUID test vectors, concurrent uniqueness, backward-clock, disabled compatibility, mixed-version replay and actual financial/locking tests are required. A large prepopulated index microbenchmark must measure WAL and timing; it is not an end-to-end cloud capacity result.

## Failure and recovery
Defaults remain UUIDv4; rollback redeploys the immutable prior image without rewriting any persisted UUIDv7 IDs. Existing UUID parsing and equality handle both versions. Database errors retain transaction rollback, idempotent replay and commit-before-Redis/Kafka acknowledgement. Failed quality remains failed after financial/drain/restoration. No consumed reset or main merge.

## Validation evidence
[RFC9562 section5.7](https://www.rfc-editor.org/rfc/rfc9562.html#name-uuid-version-7) specifies layout; section2.1 discusses index locality and appendixA.6 provides the deterministic vector used in tests. Built/pulled22runtime modules, exactly5changed from the validated projection parent, defaultoff and enabledversion7 verified in copied/installed code.122actual PostgreSQL/Redis integration cases passed;1optional real Kafka case skipped. Tests include16384same-millisecond concurrent IDs, invalid/backward clocks, mixed-version financial replay and Redis rollback compatibility. Three scope checks passed after correcting the test's expected internal ticket-handler name; every SQL statement and all lease/cache/HTTP/confirmation code remain unchanged.

Owned local PostgreSQL17.6 with32MiB buffers,pglz compression, durable commit/full-page writes: seeded1million UUIDv4 rows/39.79MB index.16alternating pairs,2000inserts/trial,checkpoint before each (excluded from timing). Median WAL10423624to518864bytes (95.02%lower),median208.901to14.389ms,p95272.477to28.281ms. All1064000rows/version counts verified, all owned resources removed. This is an index microbenchmark, not checkout throughput.

Published/pulled manifest7793159270ac95e20c3224eaee3298d42ab6bfeb30088d65f925b8ee5ad3b979, configuration06ee546fc6b01e8b0f1bd2638f91d8a194e5f1c8eeac80eec8ada617098d9448, source2c59942318c306c925e5dba3ae26e59c7af30260b4de4560fed823e73ecd6afe. Initial runner checks:168passed,5optional skipped,1test failed because it called the layered API contract without the actual runtime view. Corrected the test context; final33targeted cases passed,2optional skipped. Ruff passed. Separate actual-image22-helper/22-runtime rehearsal passed with synthetic inventory timestamp; final real isolated reservation passed without live allowance changes. See [local evidence](../capacity/cce/persistent-uuid-local-2026-10-19.json) and [publication](../capacity/cce/persistent-uuid-registry-2026-10-19.json). Actual cloud qualification remains required.
