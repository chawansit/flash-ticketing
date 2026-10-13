# ADR0299: Expose preloaded query statistics

## Status

Accepted; diagnostic SQL objects installed and cloud reads verified. No application performance qualification.

## Context

ADR0298 reduced fulfillment database acquisitions by 33.3%, but four candidate payment requests still received recoverable 503s. Failure captures locate long commits and connection setup alongside WAL/data-file waits, without query-level attribution. Read-only RDS inspection on 2026-10-13 found a 36,057,102,003-byte database, substantial historical test data, valid expected indexes, and indexed expiry, callback-dispatch and outbox plans. These are cumulative observations, not proof of the four failures' cause.

The managed PostgreSQL server already preloads pg_stat_statements. Version 1.11 is available, but the extension's SQL objects are absent from the application database. Enabling those objects needs no restart or preload/configuration change.

## Decision

Create pg_stat_statements version 1.11 in public on the already identified test database, using the existing protected RDS diagnostic account and verified TLS. Require the preloaded library, available version, idle generator, no active owned load, and extension absence before the operation. Bound connection, lock and statement waits. Do not change application images, roles, financial tables, indexes, pool budgets, WAL settings or machine sizes.

Read ranked query counters on one short-lived diagnostic connection while load is idle. Preserve existing counters; never reset pg_stat_statements. Retain raw SQL only in ignored private evidence. Publish query fingerprints, operation categories and aggregate costs without customer identifiers, SQL literal values, credentials or endpoints.

For subsequent tests, collect before/after snapshots outside the offered traffic window on the existing diagnostic connection. Attribute deltas only when stats-reset epochs and query identities match; detect eviction/reset rather than treating missing records as zero. Cumulative rankings can select suspects but cannot establish per-arm cost or exact failure causality. Any performance correction requires its own evidence and decision.

## Alternatives

Another identical load test without query attribution repeats the gap. New profiling infrastructure adds cost. A database restart or configuration change is unnecessary because the library is already preloaded. Cleaning historical records or adding indexes without query evidence changes the workload and could destroy recovery/idempotency history. None is selected.

## Consequences

The SQL view exposes existing instrumentation; it does not create a capacity improvement. No billing tier change or paid load is authorized by this diagnostic operation. Shared statistics cover multiple prior runs and require careful qualification. Exporting full SQL could expose data, so raw evidence stays private. This decision changes diagnostics only and supersedes no persistence, locking, messaging, idempotency, TTL, payment or scaling decision.

## Failure and recovery behavior

Fail closed on endpoint/certificate mismatch, active experiment, absent preload, unexpected version or SQL error. CREATE EXTENSION executes atomically with bounded waits. Preserve an existing extension without replacing it. On ambiguous completion, read extension state before retrying. Never restart the database automatically. Rollback, if necessary, drops only the extension installed by this operation, without CASCADE or deleting business records; check dependent objects first.

## Validation evidence

Executed two read-only application-account inventories and a protected diagnostic-account preload check. Verified 36.06 GB database size, valid targeted indexes, and indexed EXPLAIN plans. App-account preload inspection was denied; the diagnostic account confirmed the preloaded library. An optional activity query in the root inventory had a syntax error; it is not used as evidence. These initial inspections did not execute EXPLAIN ANALYZE, extension creation or a load stage; the subsequently completed operation is recorded below.

Executed the bounded CREATE EXTENSION operation successfully: version 1.11 in public, without restart, configuration change or statistics reset. Read 60 ranked historical statement rows; original statistics epoch remains 2026-09-13. Protected TLS connection closed and temporary certificate removed. Sanitized evidence: [query attribution inventory](../capacity/cce/query-attribution-inventory-2026-10-13.json). No paid load started.
