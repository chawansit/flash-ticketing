# ADR0176: Application-role database diagnostics

## Status
Accepted for design and local qualification following the user's approval on 2026-10-06. Not installed in cloud or registered for execution. Historical full-database profiles remain unchanged.

## Context
ADR0175 requires effective statistics access for full-database diagnostic coverage. The last verified observer lacks that access. Local PostgreSQL reproduced hidden foreign-role activity, and grant/revoke tests verified the privilege mechanism. The six historical RDS sessions remain unidentified. The user approved proceeding after the monitoring-access or narrower-diagnostics alternatives were presented; this design chooses the lower-privilege alternative, without granting roles.

## Decision
Define an explicitly named application_role diagnostic scope. Before future runner integration, prove that all four APIs and every configured database background replica use the same database role and database as the observer. Derive the comparison from private inspected configuration and export only a fingerprint and coverage counts. Reject missing roles, changed role/database, unknown ownership and incomplete source/inventory evidence. Validate the observer's effective role/database against its configured connection before admitting the scope.

Take one materialized activity snapshot of the current database, excluding the observer. Partition sessions by catalog role identity into application-role sessions, known foreign-role sessions and unknown identity. Require zero unknown identities and zero hidden application-role states. Report foreign sessions and masked foreign sessions explicitly; never treat foreign invisibility as full-database coverage. Preserve bounded own-role activity/wait counts, read-only 100 ms queries, overhead limits, reset detection and sample continuity. Cluster WAL/checkpoint/database counters remain aggregate observations with explicit timing availability and cannot be attributed solely to the application.

A future registered profile must declare this scope identically for baseline and candidate, pin its source and role-binding evidence, and use the new application_database_wait_evidence_complete gate. Retain a separate full_database_visibility_complete flag even when false. Do not silently rename the existing database_wait_evidence_complete gate or reinterpret historical results. All customer latency/error, authorization, post-TTL payment durability, zero-double-booking, complete queue drain and exact restoration gates remain unchanged. No extra observer connection, resource, permission grant or higher load is authorized by this design.

This provides a narrower alternative to ADR0173/ADR0175; it supersedes neither decision for existing full-database profiles. It does not change application persistence, locking, messaging, idempotency, TTL or scaling. Cloud registration is future scope and requires local qualification of the complete runner before any dispatch.

## Alternatives
- Obtain pg_read_all_stats: gives wider visibility but requires a provider-supported administrator action and exposes other sessions' statistics/query text to the role.
- Remove visibility gates globally: rejected; conceals incomplete evidence and changes old profiles.
- Infer ownership from application_name: rejected; mutable labels are not role identity.
- Ignore all null activity: rejected; could hide application or unknown sessions.

## Consequences
Application coverage can remain complete when unrelated managed sessions are masked. Shared-system contention and the exact identity of those sessions can remain unproven. Applications using multiple database roles do not qualify this single-role policy. Successful role-scoped diagnostics cannot be presented as complete RDS diagnostics, a WAL fix or production capacity qualification.

## Failure and recovery behavior
Reject malformed or inconsistent partitions, unavailable application activity, unknown role identity, invalid waits, reset counters, overhead and sampling gaps. A scope cannot pass without role-binding evidence, even if its samples are otherwise valid. Future runner failures must stop progression and preserve mandatory financial audit, drain, source identity and exact restoration. Keep failed scopes and raw evidence; never replay ambiguous identities.

## Validation evidence
Implemented the separate collector, role-binding validator and scope-aware summary. Executed 138 broader tests in 54.71 seconds; after explicit catalog qualification, 33 scoped tests passed in 0.90 seconds and one real PostgreSQL integration test passed in 0.18 seconds. Ruff passed. The final native helper completed in 3.579 seconds and removed its owned container/volume. Local replica-binding checks are synthetic; real database session visibility was verified, but no 18-replica cloud proof is claimed. The component is not registered or wired into the cloud runner. No cloud calls, permissions, deployments or paid experiment occurred. See [local qualification](../capacity/flash-sale-opening/application-role-diagnostics-validation-2026-10-06.json). A local result-wrapper failure after cleanup was retained and corrected under a fresh evidence identity; historical financial/cloud evidence remains unchanged.

References: [PostgreSQL statistics visibility](https://www.postgresql.org/docs/17/monitoring-stats.html) and [materialized common table expressions](https://www.postgresql.org/docs/17/queries-with.html).
