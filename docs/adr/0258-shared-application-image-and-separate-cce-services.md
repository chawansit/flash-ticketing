# ADR0258: One application image, separate service containers

## Status

Accepted for packaging and local validation on 2026-10-10, following direct user authorization. Cloud migration and performance qualification remain pending. This supersedes the role-specific API/simulator image selection in ADR0251 and ADR0255 for the new deployment candidate only; their historical test receipts remain unchanged. All concurrency, durability, idempotency, TTL and messaging decisions remain in force.

## Context

The accepted API runtime and simulator were built separately. Their worker implementation is identical, but simulator configuration permits twelve HTTP deliveries independently of its ten SQL connections. Rebuilding the current checkout would also introduce five unrelated module differences from the accepted API. We need a reproducible shared image without silently changing the payment or reservation implementation.

## Decision

Derive one Linux amd64 application image from the immutable ADR0255 recovery API digest. Verify its complete 22-module source map against the accepted public receipt. Apply the ADR0251 simulator concurrency validation change and merge the accepted reservation writer pipeline code from its ADR0238 frozen source snapshots: one config field, the worker constructor selection, and the store constructor/persistence method. Integration testing exposed that API/simulator stores alone do not include this writer capability. Preserve every other method, dependencies, migrations, recovery endpoints and pipeline defaults. Verify both installed and copied package trees and record parent, source, dependency and resulting image hashes.

Every API, reservation writer, outbox publisher, ticket consumer, maintenance worker, reconciler and test payment simulator uses the exact same registry digest. Commands select the service; environment and resource limits select its capacity. Confirmation has a separate optional deployment, disabled while asynchronous confirmation is disabled. The payment simulator stays development-only and is excluded from production activation. Maintenance combines refresh and expiry; do not simultaneously activate duplicate refresh/expiry deployments.

Generate separate CCE Deployments and internal Services, initially at zero replicas. No HPA, new database, Kafka broker or Redis instance is introduced. DATABASE_URL references the existing shared PgBouncer endpoint from a Secret; its physical server budget remains 24. Application client pools are explicitly per-role and must not be confused with physical RDS connections. Existing workload configuration must be supplied and checked before activation. Image pinning does not pin configuration automatically.

The proposed test layout preserves four APIs, six consumers, three writers and one each of publisher, maintenance, reconciler and simulator. The unused confirmation deployment remains zero. Proposed supported CCE requests/limits are 1 vCPU/2 GiB per API and 0.25 vCPU/0.5 GiB per worker: 17 active pods, 7.25 vCPU, 14.5 GiB. These are proposed resources, not a measured worker sizing result. Current CCE documentation lists 1-vCPU pods with at least 2 GiB; unsupported combinations may be adjusted by the provider. Actual admission and billing must be verified before deployment.

## Alternatives

- Separate images per service: useful when dependencies or release ownership diverge, but increases drift now without a corresponding benefit.
- Rebuild all current sources: straightforward, but changes unqualified code along with deployment layout.
- Function-per-task: requires different execution/lifecycle semantics for Kafka consumption, reconciliation and leased work; outside this decision.
- Move Kafka/PgBouncer too: expands the comparison and operational risk; defer until separately justified.

## Consequences

One build and immutable digest removes role-to-role source ambiguity. Separate containers permit independent service placement and later measured scaling. Every service inherits the full application dependency set and a shared release cadence. HTTP concurrency remains independently bounded at 32; separating containers does not increase the RDS server budget or prove capacity improvement. No container image contains credentials or fixture data.

## Failure and recovery behavior

Do not roll all roles at once or run uncounted ECS and CCE replicas together. Record the current digest and configuration, pause test dispatch, wait for mandatory payment/durability/queue checks, stop one old role, then start the corresponding CCE role. Use durable outbox/consumer replay and reservation leases as already implemented; do not introduce retries that bypass idempotency. Verify per-role progress and source identity before proceeding. Roll back by stopping the CCE role and restoring its recorded ECS image/configuration/count. Preserve Redis seat TTL, PostgreSQL booking authority and customer authorization.

A worker metrics response proves the process HTTP server is alive, not that Kafka, SQL or business processing is healthy. Avoid automatic liveness restarts based on transient dependency failures. SIGTERM uses the existing graceful shutdown; if the grace period expires, durable leases and replay recover work. Readiness, queue age, consumer progress and paid-ticket reconciliation remain separate deployment gates.

## Validation evidence

Executed: immutable-parent extraction and installed/copied/imported verification of all 22 modules; 12 unit tests; 27 integration tests inside the selected image against PostgreSQL 17.6 and Redis 7.4.5, covering 100 concurrent same-seat requests, writer replay, sync/async callback paths, ticket issuance, lost responses and actual API process restarts. All eight rendered role configurations validated inside the registry-pulled image, Compose configuration validated, Ruff passed. The first packaging candidate had 4 failed tests and 2 setup errors because it lacked the accepted writer pipeline capability; it is excluded, not counted as a pass. The corrected candidate passed all 27 tests. Public hashes, excluded candidate and executed checks are recorded in [the shared-image receipt](../capacity/cce/shared-application-image-2026-10-10.json). No cloud rollout, load or performance claim follows from packaging tests. Billable migration requires a concrete scope beyond the consumed four-API-pod experiment envelope.

## References

- [Accepted API image receipt](../capacity/cce/customer-recovery-image-2026-10-10.json)
- [Simulator decision](0251-decouple-callback-dispatch-concurrency-from-db-pool.md)
- [CCE Autopilot supported resources](https://support.huaweicloud.com/intl/en-us/productdesc-cce-autopilot/cce_12_0002.html)
- [CCE Autopilot billing](https://support.huaweicloud.com/intl/en-us/price-cce-autopilot/cce_03_0005.html)
