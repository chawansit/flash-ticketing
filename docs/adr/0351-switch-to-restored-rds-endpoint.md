# ADR0351: Switch to the restored RDS endpoint

## Status

Accepted for the user-requested database endpoint switch on 2026-10-18. No capacity run or infrastructure resize is authorized by this change.

## Context

The user created RDS PostgreSQL at 10.1.21.48, supplied its private DNS name and CA files, and confirmed that the old database was already restored there. The previous endpoint is 10.1.228.129. Both endpoints accept verified TLS administrator connections. Both report PostgreSQL 17.11, twenty public tables and the same seven migration checksums. Estimated counts for five key tables match; this is not a full data reconciliation. The existing ticketing application role authenticates against the new instance. At inspection, APIs/workers are running but APIs unhealthy; PgBouncer, Kafka and the load balancer are stopped.

## Decision

Change the existing PgBouncer upstream to the supplied private DNS name, install the supplied CA bundle and retain verify-full. Preserve the existing ticketing runtime role and its credential, exact image, connection/resource limits and network aliases. Keep root for administrative verification. Do not copy or modify restored application data or run migrations. Preserve old endpoint configuration and the original stopped pooler as rollback material. Verify the actual new upstream and read-only application access before completing. Record the resulting operational state explicitly; load tests remain paused.

Use a fresh password-free diagnostic target for future work. Do not rewrite immutable historical run targets, results or benchmark bindings. Future capacity preparation must bind and qualify the new endpoint explicitly; old throughput evidence cannot establish capacity on the replacement RDS.

## Alternatives

Copy the old database again: unnecessary and could overwrite the user's restore. Disable certificate verification: rejected. Replace the application account with root: unnecessary. Resize pools or services while changing the endpoint: outside this task.

## Persistence, locking, messaging, idempotency, TTL and scaling

Database authority changes to the user's restored RDS. No schema, transaction/lock, messaging, idempotency, cache TTL, financial guarantee or scaling design changes. Do not allow concurrent application writers against both endpoints. Existing Redis/Kafka state is not migrated by a database endpoint change. No paid traffic is generated for validation.

## Failure and recovery

Reject failed DNS, TLS, schema identity or application authentication checks before cutover. Retain private old configuration and original container for rollback. Keep writers quiesced during validation. Before new writes are enabled, rollback may restore old configuration; after writes to the new database, do not silently route back to the old database without data reconciliation. Preserve the old RDS itself. Existing user pause on load testing remains in force.

## Validation evidence

Executed read-only probes from the existing API image on ECS: supplied DNS resolves to 10.1.21.48, verify-full with supplied ca-bundle.pem succeeds, PostgreSQL reports TLS enabled, both instances' migration digests equal 9ceca0539ec4c6dc5b734f4c73fcecb9ea0c89fdeafed6483e383491db004b30, and the existing ticketing role authenticates. Estimated counts match; exact count scans hit the bounded five-second query timeout and no full reconciliation is claimed.

Completed actual Docker-create preparation without starting the prepared container; image, TLS, 24-backend/no-reserve limits and resources were preserved. Two guarded verification attempts restored the original stopped pooler before application traffic: the helper was corrected to omit an unsupported startup parameter and normalize PostgreSQL's address representation. Isolated application-role access proved the actual TLS upstream. Final corrected cutover passed: actual pooled SQL server 10.1.21.48, ticketing role/database, upstream TLS enabled, direct-IP verify-full passed, all four APIs healthy, Kafka healthy and load-balancer readiness HTTP200. The user explicitly requested bringing services online; load testing remains paused.

Both existing deployment .env.rds files were backed up privately and updated for the DNS, CA and migration URL. Current private deployment/recovery files: /root/rds-switch-20261018c. Old RDS remains untouched; original pooler retained stopped. New local password-free diagnostic target validates without changing historical targets. Active dependency-probe source now points to the replacement IP; 24 focused dependency lifecycle/protocol tests passed (40 deselected). No paid run, application data copy or schema migration occurred. [Compact cutover evidence](../capacity/cce/rds-endpoint-switch-2026-10-18.json).
