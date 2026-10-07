# ADR0196: Bound private dependency readiness and audit retention

## Status
Accepted for local implementation under ADR0172, ADR0184, ADR0186 and ADR0195. The worker profile remains unregistered; complete live financial/observer integration and capacity qualification remain pending. This clarifies ADR0186 recovery-file retention. Other accepted decisions are not superseded.

## Context
TCP access alone cannot establish authenticated Redis, usable PostgreSQL, verified RDS TLS or correct Kafka metadata routing. The older offline lifecycle also permitted deleting recovery files despite failed mandatory financial audits.

## Decision
Add guarded read-only dependency readiness between common infrastructure application and worker startup. Probe both existing hosts with a cached immutable worker image. Bind protected RDS credentials and CA content to the fresh scope and original PgBouncer settings and CA mount. Verify a direct RDS connection with verify-full and a separate pooler connection, read-only transactions, encrypted primary backends and exact database/user identity. Ping the exact managed Redis endpoint and require the single Kafka broker advertised in metadata to match the private listener. Check all four observed API endpoints independently.

Use fresh owned probe containers with a Python-only entrypoint, read-only root, bounded tmpfs/resources, no host mounts and no pulls/builds. Protected settings travel over private standard input, never command arguments or container environment. Recheck full inventory before launch; remove only exact owned probe identities and prove original observations unchanged afterward. Use one shared readiness deadline. Failed or ambiguous operations block startup and replay; do not infer ownership of an uncertain probe.

Retain recovery files until all mandatory post-TTL/payment/booking audits, worker stop/absence, exact restoration, restored full queue drain, generator idleness and journal checks pass. Still attempt financial audits and owned restoration independently after any failure or pause.

## Alternatives
TCP-only checks miss authentication and metadata errors. Real workers used as probes may process business data before readiness. Host library installation introduces drift. Historical unbound TLS receipts may be stale. Deletion after runtime restoration alone loses evidence for unresolved financial failures.

## Consequences
A few bounded read-only connections consume existing capacity. Direct RDS verification uses the application identity, not an administrator. Temporary probes add exact cleanup obligations. Readiness does not replace source inventory, callback distribution, financial audit integration or registered load qualification.

## Failure and recovery behavior
Fail closed for changed scope/context/runtime, expired deadlines, TLS/authentication errors, unexpected metadata, missing API coverage and uncertain removal. No ambiguous replay or weakened customer gates. Owned restoration remains available; financial failures retain recovery files.

## Validation evidence
446 affected tests passed in 58.43 seconds with no skips. Two real local Docker cases verified the restricted probe lifecycle and exact removal after synthetic dependency success and failure. Eight generated-operation fault cases ran on real Linux with simulated Docker. Dependency clients were mocked: actual cloud RDS TLS, Redis authentication, Kafka metadata and API readiness remain unverified. Ruff, naming and whitespace checks passed; owned local test resources were removed.

The initial ownership-prefix assumption was replaced with the existing shared validator. A synthetic secondary drift fixture was corrected. Real Docker execution caught a missing JSON import in the generated program; the correction passed both native lifecycle cases before the complete regression suite. Mandatory cleanup-gate failures now retain recovery files, while all audits and owned restoration are still attempted independently.

The worker profile remains unregistered. Complete live audit/observer integration and load qualification remain pending. No cloud calls, customer dispatch or capacity improvement were measured. See [readiness evidence](../capacity/flash-sale-opening/background-service-separation-readiness-2026-10-07.json).
