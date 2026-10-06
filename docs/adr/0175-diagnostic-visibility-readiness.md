# ADR0175: Diagnostic visibility readiness

## Status
Accepted for local implementation under ADR0172. Cloud permissions remain unchanged; no capacity improvement is claimed.

## Context
ADR0174 completed 18,000 paid-and-issued customer journeys, but six database activity samples failed the full-visibility requirement and prevented the paid candidate from running. The observer connection lacks effective pg_read_all_stats privilege. An idle probe seeing no hidden sessions does not establish that future foreign-role activity will be visible. A disposable local PostgreSQL fixture reproduced the hidden-session failure and verified that statistics privilege resolves it; this does not identify the six managed-cloud sessions.

## Decision
Require effective pg_read_all_stats privilege in the existing diagnostic capability preflight, before creating the paid fixture. Check effective usage, rather than membership alone, using pg_has_role(current_user, 'pg_read_all_stats', 'USAGE'). Retain the existing read-only transaction, 100 ms statement timeout, transient preflight connection and reused observer connection. Report missing privilege as a fixed diagnostic readiness failure. Never suppress restricted sessions, synthesize counters or relax visibility, latency, financial or queue gates.

This supersedes only ADR0173's capability admission that allowed a short sample with no currently restricted sessions to qualify an account lacking guaranteed statistics access. It does not change the registered profiles, workload, connection ceilings, application locking, persistence, idempotency, messaging, TTL or API placement. A role grant, alternate monitoring account or provider-supported diagnostic scope is a separate infrastructure/permissions decision and is not performed by this ADR. An existing role grant does not prove every backend will report a state; runtime visibility checks remain mandatory.

## Alternatives
- Repeat paid controls until no foreign-role activity is sampled: rejected; successful timing would not establish visibility.
- Drop restricted rows: rejected; would conceal missing evidence and change the gate.
- Grant privileges automatically: rejected; requires an authorized administrator and review of broader monitoring visibility.
- Use a dedicated observer role: desirable for future least-privilege operation, but requires credentials, table access and deployment configuration; evaluate separately.

## Consequences
Profiles requiring full activity diagnostics stop before paid fixture creation when privilege is absent. Safety qualification may already have run in the existing workflow; no claim of zero workflow deployment follows from this preflight. Unprivileged databases cannot complete this strict diagnostic profile until an approved remedy is available. Statistics privilege can expose other sessions' query text even though our collector never exports it; use an appropriate monitoring account when provisioning that access.

## Failure and recovery behavior
Keep missing-privilege failures distinct from restricted runtime activity. Preserve fixed error codes, bounded evidence and old failed scopes. Existing finally blocks continue owned cleanup, mandatory financial audits, complete drain and exact restoration. Do not run a paid replacement merely to reproduce a known missing privilege. No retry hides a failed sample.

## Validation evidence
Before implementation: one real local PostgreSQL 17.6 integration test passed in 0.25 seconds, reproducing hidden foreign-role activity and grant/revoke behavior. The owned disposable container and volume were removed; zero cloud calls occurred. Follow-up implementation checks passed: 142 focused tests in 56.51 seconds; the real PostgreSQL integration test passed again in 0.12 seconds, including cached-collector privilege revocation. Ruff passed. All owned local resources were removed. No cloud calls, grants or deployments occurred. See [local validation checkpoint](../capacity/flash-sale-opening/diagnostic-visibility-readiness-2026-10-06.json).

References: [PostgreSQL statistics visibility](https://www.postgresql.org/docs/17/monitoring-stats.html) and [Huawei RDS constraints](https://support.huaweicloud.com/intl/en-us/productdesc-rds-pg/rds_02_0012.html).
