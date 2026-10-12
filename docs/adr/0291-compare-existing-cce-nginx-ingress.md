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

Implemented: opt-in Service/Ingress integration in the existing paid runner, exact addon binding, API-only backend selection, route/authorization checks, and UID-bound teardown. Executed: 303 focused unit tests passed in 35.84 seconds; Ruff passed for changed source and tests. These are local checks, not cloud capacity evidence. The fresh control/candidate comparison has now completed as recorded below; no hourly qualification or maximum capacity gain is claimed.

## Pre-dispatch guard correction

The first fresh attempt (adr0151-c816aa2674a7) stopped during pod admission before any paid stage. The image, imported source, API resources and container settings matched. The new full-label equality check was unnecessarily strict: verify the required owner/API routing labels and permit unrelated provider labels. Regression tests preserve rejection of replaced ownership or routing labels. Failed evidence is retained in [prepaid abort](../capacity/cce/existing-ingress-prepaid-abort-2026-10-12.json); runtime restoration, integrity and credential cleanup passed. This is not a measured control failure or an ingress capacity result.

Executed after correction: 180 focused tests passed in 22.05 seconds. The corrected comparison requires fresh identities; the original failed attempt is not replayed.

## Executed cloud comparison

Both fresh arms completed PASSED_RESTORED. Their five-minute paid windows were October 12, 2026, 22:50:08-22:55:08 Bangkok (control) and 23:13:18-23:18:18 Bangkok (candidate). See the [comparison](../capacity/cce/existing-ingress-comparison-2026-10-12.json), [control](../capacity/cce/existing-ingress-control-result-2026-10-12.json) and [candidate](../capacity/cce/existing-ingress-candidate-result-2026-10-12.json).

| Measured outcome | ECS entry control | CCE NGINX candidate |
| --- | ---: | ---: |
| Offered / dispatched journeys | 25,200 / 25,200 | 25,200 / 25,200 |
| Unique paid-and-issued tickets by completion | 25,200 | 25,200 |
| Unique paid-and-issued tickets inside offered 300 seconds | 24,961 | 25,097 |
| Inside-window tickets/s | 83.20 | 83.66 |
| Generator drops | 0 | 0 |
| Initial customer errors / recovered / final failures | 6 / 6 / 0 | 0 / 0 / 0 |
| Total journey p95 | 5,296.60 ms | 2,585.48 ms |
| Hold HTTP p95 | 12.58 ms | 11.64 ms |
| Payment HTTP p95 | 100.04 ms | 85.56 ms |
| API CPU aggregate | 1.380 cores | 1.305 cores |
| Primary ECS mean CPU | 42.74% | 38.70% |
| Observed pooler maximum wait | 238.86 ms | 8.85 ms |

Latency summaries are the worst shard p95, not a combined percentile. Both arms passed the unchanged short-profile gates, financial relationships, post-TTL durability, zero double-booking, full queue drain, complete statistics and slot-failure capture, ownership and restoration. The supplementary exact-window check against all 25,200 scheduled tickets is false in both arms: some tickets completed after offered traffic ended. It is not the short-profile acceptance gate and must not be interpreted as hourly qualification.

The candidate's observed journey p95 decreased 51.19%; HTTP improvement was smaller. Ticket-wait p95 decreased from 3,963.07 to 621.80 ms. Different database/backend conditions across sequential arms remain a possible contributor. Control failure snapshots include payment commits and callback writes; WALWrite waits occurred in both arms. One pair cannot establish ingress as the cause of the asynchronous improvement or prove a higher capacity ceiling. Ingress controller CPU was not collected, so its headroom is unmeasured.

All temporary candidate route and namespace resources were removed with verified ownership. The existing addon and ELB remained unchanged, no new ELB was created, and both original runtimes and temporary transport credentials were restored/cleared.

Recommendation: use this route as the candidate for a fresh bounded hourly qualification at the same offered rate and resource/connection budgets. A new hourly decision/binding is needed because this ADR selects only the short profile. Do not raise load, claim production qualification or merge main based on this pair.
