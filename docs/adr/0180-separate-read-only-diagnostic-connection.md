# ADR0180: Separate read-only diagnostic connection

## Status
Accepted; observer connection component locally qualified under the standing work envelope. Cloud capability probes passed; observer integration and a new qualified runner binding are still required before load. No monitoring permissions have changed.

## Context
ADR0179 retained 41 unknown-role, hidden-backend samples and correctly stopped the measured candidate, despite 18,000 successful paid-and-issued tickets and full drain/restoration. A native autovacuum worker reproduced the signature; its identity in cloud remains unknown. The current application connection lacks statistics visibility and role administration. The separately supplied RDS administrator already has effective pg_read_all_stats access. A read-only direct probe with the existing mounted CA and certificate/hostname verification passed over TLS 1.3, found no currently restricted activity, removed its temporary CA, and left runtimes unchanged with the generator idle.

## Decision
Use the existing administrator credentials only for the observer's data/statistics connection. Enforce default_transaction_read_only at connection startup and sslmode=verify-full with the existing trusted CA. Replace the observer's existing data connection; do not add a concurrent connection or change application pools, PgBouncer administration, images or workload. Keep application credentials and all customer, durability, authorization, zero-double-booking, queue-drain and restoration gates unchanged.

Return to the existing full-visibility diagnostic semantics of ADR0173/ADR0175. Require effective statistics access in preflight and complete runtime activity throughout the trace, including background workers. Do not infer a hidden actor's identity, ignore unknown rows under ADR0176, reinterpret old failures or disable monitoring gates. This supersedes only the use of application DATABASE_URL for the new separately bound observer connection; historical profiles remain unchanged until the full extension is explicitly qualified.

Credentials must travel through protected input and encrypted SSH stdin, never command arguments, logs, published manifests or public evidence. Any remote credential bundle must be inside the runner's exclusively owned directory, regular and owner-only, bound by an exact hash and observer role/database identity, and removed during mandatory cleanup. Preserve the existing PgBouncer observer connection. Emit only safe identity fingerprints and capability flags. Fail closed on mismatched database, role, certificate, bundle ownership, binding, visibility or cleanup.

## Alternatives
- Grant statistics access to the application role: unnecessary; broadens application privileges.
- Disregard unknown cloud sessions: rejected; their identity is not established.
- Keep repeating the scoped paid control: rejected; does not resolve the visibility policy.
- Create another monitoring account or resource: unnecessary now and requires an infrastructure action.
- Add a second statistics connection: rejected; changes the observer connection budget.

## Consequences
Existing access can resolve the monitoring blocker without new spending or permission grants. Administrator credentials need careful confinement, and session read-only enforcement is protection against unintended writes rather than a claim that the account itself has become least-privileged. Direct observer transport changes relative to PgBouncer; declare and bind it equally in both arms, retain exactly one data connection, and do not attribute any result to placement without a matched comparison. Full activity visibility cannot prove a WAL cause or higher capacity.

## Failure and recovery behavior
Before cloud integration, qualify credential confidentiality, certificate verification, connection substitution, read-only enforcement, exact database/role binding and cleanup with synthetic and native tests. Register and locally qualify the complete runner extension before a fresh reservation. Do not use this component to bypass the runner or reopen consumed scopes. Failed controls stop progression; preserve audits, full queue drain and exact restoration. Ambiguous ownership or credential cleanup prevents more load.

## Validation evidence
Executed read-only cloud capability probes: application account lacks effective statistics visibility and grant capability; separate RDS root has effective visibility, TLS 1.3 and zero currently restricted sessions. Root probe completed in 14.062 seconds; temporary CA removed, runtime identities unchanged, generator idle, zero grants/deployments/customer dispatch. These are capability observations, not load qualification. Local component and runner validation will be recorded after execution.

References: [PostgreSQL statistics visibility](https://www.postgresql.org/docs/17/monitoring-stats.html) and [Huawei RDS root constraints](https://support.huaweicloud.com/intl/en-us/productdesc-rds-pg/rds_02_0012.html).

Local observer implementation passed 77 final host/POSIX checks in 1.29 seconds, including fourteen unsafe file cases, and one real PostgreSQL TLS integration test in 0.17 seconds. Verified certificate authentication, wrong-CA rejection, startup read-only enforcement, database-rejected writes, full statistics collection, unchanged global driver/PgBouncer calls and no second data connection. Native owned resources and temporary TLS key were removed; local tests made zero cloud calls. Ruff passed after correcting initial style findings. A local quoting failure and a missing-stdin container fixture failure were retained and corrected before successful checks.

The observer accepts the connection only with an exact approved inventory, same application database, bound credential/CA hashes and the full-visibility mode. This component is not registered for cloud execution. Protected credential staging, source pinning, startup evidence and mandatory cleanup still require full bounded-runner integration and local qualification. No new cloud load or capacity improvement was measured. See [component validation](../capacity/flash-sale-opening/separate-diagnostic-connection-validation-2026-10-06.json).
