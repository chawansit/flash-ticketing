# ADR0293: Qualify the existing CCE ingress for one paid hour

## Status

Accepted under the user's Continue authorization and standing work envelope. Extends ADR0291's existing-ingress route to one hourly qualification. Supersedes ADR0281's historical ECS-entry prerequisite selection only for this explicitly bound ingress qualification; retains its hourly workload and recovery contract. No production adoption or main merge is authorized by this decision.

## Context

The matched ADR0291 short arms passed and restored. The CCE candidate completed 25,200 unique paid tickets, including 25,097 inside 300 seconds, with no drops, initial customer errors or final failures. Journey p95 was 2.59 seconds. One sequential pair does not prove causality or sustained capacity. The most recent ECS-entry hour failed at 297,218 tickets inside the hour, with dropped journeys and incomplete slot histories.

## Decision

Bind the exact retained passing CCE candidate as the prerequisite for one fresh 84 offered journeys/s, 3,600-second stage. Require at least 300,000 unique paid-and-issued tickets inside that hour, 302,400 expected terminal tickets, and every existing hourly quality, financial, diagnostics and restoration gate. Use the proven hourly runner with the same application image, four 1-vCPU/2-GiB API pods, fourteen 250m/512Mi workers, 24 physical pooler connections, admission budget 20, two 250-journey generator shards, gateway delays and customer recovery. No new ELB, resource resize or retry change.

Keep the ADR0291 customer HTTP route and its private HTTP scope. Payment simulator callbacks retain the ECS route. Bind exact addon ownership, API-only selection, authorization checks and UID-bound cleanup. Keep the existing addon unchanged. Require its exclusive availability before deploying the experiment route.

Register a fresh hourly identity and bound source/contract hashes. The short prerequisite must match the reviewed evidence digest, passed/restored ledger, immutable application image/configuration, all measurement gates and financial checks. Do not compare old harness source hashes to deliberately extended hourly harness source; no application code changes are introduced.

Retain ADR0289's hourly bulk fixture preparation before refreshed transition admission. Retain ADR0292's short-only after-safety staging. Each hourly experiment has the approved 5,400-second ceiling including safety, audits, drain and restoration; no rolling clock deadline or cumulative cap.

## Alternatives

- Raise offered load or add pods now: rejected; one hour at the business target remains unqualified.
- Qualify the old ECS entry again: defer; the fresh CCE candidate passed short gates.
- Treat the short rate as an hourly certification: rejected.
- Rebuild the application image or redesign payment processing concurrently: rejected; confounds the duration qualification.

## Consequences

This measures sustained delivered tickets and customer quality through the existing route, not maximum HTTP RPS or ingress-only capacity. Database/backend variation and unmeasured controller CPU remain limitations. A failed hourly diagnostic gate remains a failed qualification even if customer recovery succeeds.

## Failure and recovery behavior

Reject failed, altered or unrestored prerequisite evidence, unbound origins, addon drift, unknown fixture ownership and spent-scope replay. Do not start another paid stage after this one. Preserve first errors, recovery attempts, final errors, generator drops and inside-hour ticket counts separately. Do not relax gates to obtain a pass. Regardless of outcome, perform post-TTL payment/booking/ticket audit, global queue drain, owned route/namespace teardown, runtime restoration and temporary credential cleanup. Reconcile only known owned fixtures; retain the original failed evidence if independent recovery is needed.

## Validation evidence

Before implementation: [matched short comparison](../capacity/cce/existing-ingress-comparison-2026-10-12.json), [CCE candidate](../capacity/cce/existing-ingress-candidate-result-2026-10-12.json), and source review of the existing hourly adapter and ingress ownership guards. Implemented: opt-in ADR0293 hourly route/prerequisite binding in the existing ingress and hourly adapters. Executed: 220 focused tests passed in 40.79 seconds; Ruff passed for changed source and new tests. The first combined test invocation encountered a completed live goal's stale source hash in existing test helpers (170 passed, one failure and 49 setup errors); rerun refreshed only that in-memory test hash, without altering retained evidence or authority files. Live qualification is pending. No hourly pass or capacity increase is claimed.

## First live attempt

Run adr0151-13916e654f05 stopped before paid dispatch after passing native safety, because observer inventory was stale. FAILED_RESTORED, paid_runs_started=0; original failure retained, financial integrity and exact restoration verified. No hourly capacity measurement occurred. See the [abort evidence](../capacity/cce/existing-ingress-hourly-prepaid-abort-2026-10-12.json). ADR0294 corrects the measured hourly startup ordering in a fresh scope without relaxing freshness.
