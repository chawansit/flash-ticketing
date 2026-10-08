# ADR0228: Bounded CCE API isolation comparison

Status: Accepted for bounded TCP dependency qualification; paid comparison implementation pending

## Context

The user requested CCE performance testing before further application optimizations. ADR0226's ECS 84/s, 300-second comparison failed with ticket latency, status amplification and admission errors; independent ADR0227 recovery passed. ADR0220 authorized a bounded CCE pilot with uncapped additional spending on 2026-10-08 Bangkok only. Access and a 250m/512Mi pod dry-run passed. The existing Kafka and PgBouncer containers expose no VPC host ports; moving all application roles immediately would require additional dependency/topology changes.

## Decision

First compare four identical API replicas on CCE against the retained ECS result, using the same immutable ADR0225 API image, corrected ADR0226 generator, 84 offered journeys/s for 300 seconds, per-role/aggregate database and HTTP budgets, payment latency, callbacks, error/latency and correctness gates. Keep workers, Kafka, RDS, DCS and the existing load balancer in place. No autoscaling, new ELB, persistent storage, schema change or dependency migration. This is a hybrid API-placement experiment, not a whole-platform CCE benchmark or equal-total-hardware comparison. Declare four API pods at 1 vCPU and 1 GiB each; verify effective admission, actual resources and billing specification. A short owned dependency pod may precede deployment.

Publish the exact API image to SWR and pin its registry manifest digest after verifying the configuration digest and source manifest; never substitute the already-published newer candidate. Qualify a separate profile through the standing envelope before pod creation or load. Preserve namespace UID/ownership, source hashes, readiness, all four pod identities, no restarts and the configured budget. If required, add only an owned private TCP bridge to the existing PgBouncer, preserving its 24-server connection budget; no public database port. The existing ECS load balancer routes to verified private API pod addresses only after safety passes.

Perform short network qualification first. Dependency failure stops paid progression and triggers owned cleanup, with explicit diagnosis of the unavailable route. Moving every background service is future scope, not an implemented capability.

## Alternatives

- Move all 20 proposed pods at once: changes worker, broker access, pooler and network factors together; defer.
- Use the newer SWR image without an equivalent ECS baseline: reject as an invalid comparison.
- Use a new ELB, Kafka or database: adds unnecessary spending and factors; defer.
- Fix application code before the CCE comparison: user explicitly requested the topology comparison first.

## Consequences

CCE creates additional paid compute. The original eight ECS vCPUs remain provisioned, so a result measures dedicated API isolation and its network path, not platform efficiency at equal overall cost. CCE alone does not remove worker/database bottlenecks. Report offered/dispatched journeys and paid-and-issued tickets separately, with generator drops and customer confirmations.

## Failure and recovery behavior

Reject source, registry, resource, ownership, connectivity or budget drift before dispatch. Keep the original ECS runtime snapshot. Stop further stages when a gate fails; preserve failed artifacts. Drain and independently reconcile owned payments/tickets after TTL, verify zero duplicates and global queues, restore original traffic/runtime, and delete only owned pods/namespace/bridge with exact identity checks. Unknown cleanup blocks load. Complete owned cleanup before ADR0220's spending exception expires; never delete the user's cluster. No silent gate relaxation, failed-scope replay or retry-based masking.

## Validation evidence

Read-only access and pod admission were refreshed before implementation. Kubernetes reports v1.36.2-r0-36.0.3; no workloads or cloud load have been deployed by this decision. Planned: exact image/source verification, ownership/drift/cleanup and envelope tests, disposable dependency checks, cross-pod 100-way atomic hold/replay safety, unchanged five-minute paid comparison, post-TTL relationships, complete drain and restoration.

References: [Autopilot resource billing](https://support.huaweicloud.com/intl/en-us/price-cce-autopilot/cce_03_0005.html). Admission can accept a request while billing uses a supported specification; verify the actual deployment before claiming its cost or resources.

Implementation checkpoint: The registered `cce_dependency_probe` permits one 1 vCPU/1 GiB pod and one temporary private pooler bridge, no SQL/Redis writes, holds, payments or load. Exact namespace UID and bridge ID/image/owner checks govern independent cleanup. Ambiguous creation remains a recovery block. TCP connectivity is narrower than authenticated application readiness. Single-platform SWR publication preserves the exact Linux manifest/configuration/layers; the local Docker image identifier is an OCI index, not a configuration digest. 79 local CCE/envelope checks passed before activation; the generated remote lifecycle used simulated Kubernetes/Docker, not real cloud dependencies. The paid runner remains future work.

Live checkpoint: The exact Linux platform image was pulled by one real CCE pod (1 vCPU/1 GiB requests and limits). ECS reached the pod; the pod reached RDS and Redis over TCP, but private ECS port 6432 timed out. The original failed report is retained. Independent verification confirmed namespace/bridge/certificate removal, saved original runtime semantics, stable identities across two audit samples, idle generator, zero duplicate bookings and complete queue/Kafka drain. No pre-probe exact identity capture was retained; do not claim that stronger proof. The probe originally forwarded to an assumed internal pooler port; the actual listener is 5432. The corrected future bridge uses 6432 externally and 5432 internally, and its probe requires PostgreSQL protocol negotiation. TCP reachability alone does not establish authenticated application readiness. The initially failed runtime check included volatile health log timestamps; future checks use stable execution fields and explicit generator acknowledgement. 98 focused tests and Ruff passed after final corrections; 94 broader profile checks passed before the final CCE-only corrections. Paid CCE comparison remains unimplemented and capacity improvement unmeasured. See [dependency evidence](../capacity/cce/dependency-probe-2026-10-08.json).

Retry checkpoint: The corrected real CCE pod reached Redis and completed PostgreSQL SSL negotiation with RDS and the private ECS pooler. ECS reached the pod. The raw runtime-equality gate still failed; its original report was preserved. Independent cleanup verified saved runtime semantics, stable identities across two samples, removed namespace/bridge/certificates, idle generator, zero duplicate bookings and all queues/Kafka zero. No Config/HostConfig/Mounts differences reproduced during a short diagnostic; do not attribute the old mismatch to a confirmed cause. Future probes normalize unordered mount records and retain exact before/after fingerprints plus field-level drift; this is locally validated only. 104 focused tests and Ruff passed. No paid load or capacity gain measured. See [retry evidence](../capacity/cce/dependency-retry-2026-10-08.json).
