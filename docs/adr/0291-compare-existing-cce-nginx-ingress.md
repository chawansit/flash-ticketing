# ADR0291: Compare the existing CCE NGINX ingress with the ECS entry point

## Status

Accepted for one bounded control followed by one conditional candidate under the user's Continue authorization. Production adoption and hourly qualification remain unapproved by this decision. This selects the existing addon route for the first experiment instead of ADR0290's proposed new ELB. ADR0290's new-ELB provisioning permission remains unconsumed.

## Context

The latest hour produced 297,218 unique paid-and-issued tickets inside the offered hour, with 4,781 undispatched journeys and 602 recovered first-attempt customer errors. HTTP time exceeded application time, but ingress is not yet a proven root cause. API, database, generator and confirmation delays remain possible contributors.

Read-only inspection found an existing private ELB behind two Ready NGINX addon controllers, each 250m CPU and 512Mi memory. No application Ingress exists. The paid runner currently enters through ECS Nginx, bypassing this addon.

## Decision

Reuse the proven five-minute runner at 84 offered journeys/s. Run a fresh ECS-entry control; run a fresh CCE-entry candidate only if every control gate and mandatory restoration passes. Each experiment has a 3,600-second ceiling including safety, TTL, audits, drain and cleanup. No additional ELB, API replica, worker or connection allowance is introduced.

Both arms use the same application image, four API pods at 1 vCPU/2Gi each, fourteen background workers at 250m/512Mi, simulator delay, retries, generator concurrency, fixed fixture size and database budgets. The only performance factor is the customer HTTP entry point. Payment simulator callbacks retain their existing ECS route, making this a customer-entry comparison, not a callback migration.

Add an API-only label in both arms. For the candidate, create one ClusterIP Service and one class nginx Ingress in the experiment's namespace. Select four API pods by owner and API label, excluding workers. Use a hostless / Prefix route only after verifying that this currently unused ingress entry point has no other Ingress resources. Reject concurrent/foreign ingress use. Keep HTTP private and disable redirect and proxy retries; this experiment does not qualify TLS. Bind the exact existing addon Service UID, ELB ID and private VIP before deployment. Do not mutate the addon, its ELB, controllers or configuration.

Keep persistence, locking, Redis atomic holds, idempotency, TTL, authorization, payment durability and ticket issuance unchanged. The addon has different proxy implementation/settings from ECS; retain this as part of the entry-point factor rather than attributing any change solely to CPU location.

## Alternatives

- Provision ADR0290's direct managed ELB: defer until existing-ingress evidence shows a limitation.
- Increase controller/API resources concurrently: reject because it confounds the comparison.
- Add waiting-room admission or relax quality gates: outside this experiment.
- Tune ECS Nginx first: remains an alternative if measured evidence identifies its configuration.

## Consequences

This reuses deployed infrastructure and avoids another billable ELB. The existing two controllers provide only 500m aggregate CPU; their capacity is unmeasured. Moving customer traffic may reduce ECS work or introduce a new proxy limit. No capacity increase is assumed.

Use a fresh isolated fixture per arm. Record first-attempt errors, recovery attempts, final failures, generator drops, latency, database waits, CPU, replica distribution and successful paid tickets separately. Preserve failed evidence; never reinterpret a failed historical hour as qualified.

## Failure and recovery behavior

Before paid dispatch, verify immutable API receipts, exactly four Ready Service endpoints with matching pod UIDs, the ingress class and backend, VIP publication, readiness and denial of an unauthenticated customer operation through the new route. Retain the existing cross-pod contention/replay/payment safety protocol.

Recheck routing ownership before dispatch and after the stage. Unknown ownership, missing endpoints, controller restart or addon drift fails the experiment. Stop progression if control fails. Existing bounded customer recovery uses the same idempotency key and cannot hide sustained overload.

Cleanup deletes only the owned Ingress/Service with UID preconditions, verifies route removal, then uses the existing namespace/runtime cleanup. Never delete or modify kube-system resources or the existing ELB. Reconcile an ambiguous creation only against the exact attempted object, owner and specification. Financial audits, zero double-booking, post-TTL payment durability, queue drain and exact restoration remain mandatory.

## Validation evidence

Executed before implementation: existing ingress readiness inspection, source review of the proven runner and its cleanup/ownership/origin guards. See [readiness evidence](../capacity/cce/existing-ingress-readiness-2026-10-12.json).

Implemented: opt-in Service/Ingress integration in the existing paid runner, exact addon binding, API-only backend selection, route/authorization checks, and UID-bound teardown. Executed: 303 focused unit tests passed in 35.84 seconds; Ruff passed for changed source and tests. These are local checks, not cloud capacity evidence. The fresh control/candidate comparison remains pending; no hourly qualification or performance gain is claimed.
