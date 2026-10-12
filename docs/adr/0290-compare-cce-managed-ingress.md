# ADR0290: Compare CCE-managed ingress with the ECS proxy

## Status

Accepted for provisioning one private dedicated ELB on 12 October 2026 following the user's explicit request: No ELB yet. Can you create by kubectl? This authorizes one ELB and its owned namespace, ClusterIP Service and Ingress; it does not authorize unrelated infrastructure or paid capacity traffic. Adoption as the benchmark/production entry point remains proposed and conditional on measurement.

No accepted decision is superseded yet. If adopted after measurement, this explicitly replaces the ECS Nginx request-entry routing used by ADR0228 for the selected CCE test topology only. Existing persistence, messaging, idempotency, TTL and resource-budget decisions remain accepted.

## Context

The tested route is generator -> primary ECS Nginx -> four native CCE API pods. The primary ECS also hosts dependencies; removing its HTTP proxy can separate entry-point resources from that host. The latest hour issued 297,218 unique paid tickets within 3,600 seconds, with 4,781 undispatched journeys and 602 initial-error journeys recovered. All executed payments/tickets were independently reconciled; full qualification failed.

The latest retained timing shows large client pre-send delays and delayed ticket issuance. Moving ingress cannot by itself prove those delays are caused by Nginx: client scheduling, connection reuse, network paths, Kafka processing and database waits remain possible contributors. Application identity matched the earlier numerical-target hour.

Huawei documents two CCE Autopilot ingress implementations. The managed LoadBalancer controller configures a dedicated ELB to forward to pod backends. An NGINX controller runs within the cluster and requires its own resources and maintenance. The Ingress object itself is routing configuration, not a traffic-processing pod.

## Decision

Prepare one candidate using CCE LoadBalancer Ingress and a dedicated private Layer-7 ELB in the cluster VPC. Bind a ClusterIP Service to the exact four owned API pods, with port 8000 and existing readiness. The data path is generator -> private ELB -> API pod; the managed controller maintains routing through the Service configuration. No additional NGINX pod is required by this candidate.

Initially retain private HTTP as the existing benchmark uses, with no public IP, DNS or customer-facing TLS change. Production TLS remains a separate qualification. Before deployment, record the actual ELB ID/specification, VPC/subnet/AZ, listener ownership, algorithm, persistence, timeouts, health checks, backend membership and security-group reachability. Do not enable ingress retries that conceal application failures.

Keep the application image/configuration, four API resource limits, worker placement, generator shards/concurrency, payment delays, retry policy, database connection budgets and customer/correctness gates unchanged. Register a fresh comparison identity and change only the entry routing. Protocol/balancing/keep-alive differences inherent in the ingress replacement must be recorded, not described as identical Nginx settings.

The current paid runner pins the ECS origin and its Kubernetes adapter restricts resource kinds. Implement only the Service/Ingress ownership and alternate-origin support needed for this comparison after the resource boundary is resolved; never bypass its guards or apply a manifest that silently auto-creates paid resources.

## Alternatives

- Move Nginx into CCE pods: separates CPU but retains a self-managed proxy and adds pod sizing/scaling variables. Consider only if managed ELB lacks required routing behavior.
- Keep ECS Nginx and tune it: cheapest isolated change, but still shares host resources and requires proving proxy occupancy.
- Direct pod access from the generator: useful for a bounded diagnosis, but not the intended stable production entry point and lacks comparable health-based balancing.
- Add API replicas at the same time: rejected for this comparison; it would confound attribution and could increase database pressure.

## Consequences

This can remove the primary ECS HTTP-proxy bottleneck and provides managed backend updates. It does not move PgBouncer or Kafka, remove generator limits, accelerate database commits or guarantee a ticket-throughput improvement. ELB specifications and charges are distinct from CCE API pod resources; monetary cost has not been measured; the user's new request authorizes this one private ELB provisioning action.

A short pass is necessary but does not demonstrate hourly stability. The latest failed hourly result and missing slot histories remain failed; this candidate must not overwrite or upgrade them.

## Failure and recovery behavior

Qualify endpoint ownership, all four ready backends, generator connectivity, authorization, atomic same-seat contention, idempotency and duplicate callbacks before paid traffic. Stop paid dispatch on failed preflight or correctness gates.

Do not retry an ambiguous payment with a new key. Retain original first errors, bounded recovery, final failures, generator drops and end-to-end timing separately. Backend removal, health-check failure and connection reset must remain visible.

Stop dispatch and drain/audit the exact cohort before switching routes. Preserve the ECS route for rollback. Remove only owned Service/Ingress/listener/backend objects, and verify actual ELB cleanup; deleting Kubernetes resources alone is not a receipt proving billable resources disappeared. Never delete a user-owned ELB. Preserve independent post-TTL payment/ticket reconciliation, zero double-booking, queue drain, exact runtime restoration and private-input cleanup.

## Validation evidence

Executed: reviewed the current ECS-origin guard and native route implementation; reviewed Huawei's ingress overview, controller comparison and kubectl configuration instructions. No new implementation tests, ingress deployment or capacity comparison has run.

Historical evidence: [latest hourly result](../capacity/cce/fixture-admission-hourly-result-2026-10-12.json), [independent recovery](../capacity/cce/fixture-admission-hourly-terminal-recovery-2026-10-12.json), and [offline comparison](../capacity/cce/fixture-admission-hourly-comparison-2026-10-12.json).

Planned: compare short 84 offered journeys/s stages using fresh equal-sized isolated fixtures at unchanged budgets. A failed control stops progression and remains evidence; investigate it before a candidate comparison. Collect generator CPU/event-loop/transport timings, primary host CPU, ingress/backend timing and distribution, API CPU/throttling, database waits, event lag, paid ticket issuance and queue drain. Verify slot-history completeness rather than weakening its gate. Retain mandatory restoration within each 3,600-second experiment ceiling. Only a fully passing short comparison can justify a fresh hourly qualification under the approved 5,400-second ceiling. No performance gain or maximum capacity is claimed.

## Sources

- [Huawei CCE Autopilot ingress overview](https://support.huaweicloud.com/intl/en-us/usermanual-cce-autopilot/cce_11_0094.html)
- [LoadBalancer and NGINX controller comparison](https://support.huaweicloud.com/intl/en-us/usermanual-cce-autopilot/cce_11_0850.html)
- [Creating a LoadBalancer ingress with kubectl](https://support.huaweicloud.com/intl/en-us/usermanual-cce-autopilot/cce_11_0252.html)

## Provisioning boundary

Create exactly one private Layer-7 ELB with the fixed small specification documented by Huawei, one AZ selected from live cluster node labels, no EIP, no public listener and no additional application pods. The provider must validate regional specification availability; do not fall back to a larger/elastic specification or create a second ELB automatically. First perform server-side dry-run. Persist resource UIDs, controller-assigned ELB ID/address and events privately. Reconcile existing owned resources before any repeat creation.

The owned namespace is flash-ingress-adr0290, with ClusterIP Service ticketing-api and Ingress ticketing-private. The Service initially has no API endpoints, deliberately preventing business traffic. Provisioning alone is not API readiness or a capacity result. Keep this resource prepared for subsequent owned backend integration; report that it remains billable and track exact cleanup ownership. Do not attach other resources to its automatically created ELB: Huawei warns that sharing it can leave residual resources on deletion. Existing addon load-balancer Services are outside this ownership and must remain unchanged.

## Existing ingress discovery

The user's subsequent Service manifest and executed read-only inspection prove that a dedicated private ELB already backs kube-system/cceaddon-nginx-ingress-controller. Two NGINX controller pods are Ready with zero restarts, each requesting/limited to250m CPU and512Mi memory. Ports80/443 are TCP-reachable from both generator and primary ECS. HTTP GET / returns404; there are no application Ingress objects. This verifies front-door connectivity, not API routing, TLS readiness, paid customer recovery or capacity. The previous benchmark bypassed this addon through ECS Nginx. See [readiness evidence](../capacity/cce/existing-ingress-readiness-2026-10-12.json).

New ELB provisioning was stopped before authentication or any mutation; the prepared creation manifest remains private and unapplied. The one-ELB authorization is unconsumed. No extra cloud resource was created. Evaluate using this existing NGINX ingress entry point before spending on a second dedicated ELB. The proposed direct managed-ELB topology remains an alternative, not an adopted route. Any controller sizing, routing or comparison-profile choice still needs a recorded decision before implementation; do not share or alter the addon's ELB behind its controller.
