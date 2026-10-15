# ADR0328: Test 140 offered journeys per second at the existing budget

## Status
Accepted; one five-minute experiment completed and fully passed. Hourly performance at this rate and maximum capacity remain unmeasured.

## Context
ADR0327 clean repeat adr0151-ac8debc44a23 passed all twelve gates at 120 offered journeys/s: 35,589 conservative in-window unique paid-and-issued tickets in 300 seconds (118.63/s), with no initial customer errors, retries, final failures or generator drops. The user requests more paid-and-issued tickets/s in five minutes. The preceding failed audit remains failed and independently recovered.

## Decision
Run one fresh 140 offered journeys/s, 300-second experiment (42,000 journeys; 140 isolated shows), using the same ADR0324 immutable application image, configuration, four API and fifteen worker pods, 24 physical database backends, admission budget 20, and generator concurrency 500. Require at least 41,400 conservative in-window unique paid-and-issued tickets (138/s), all existing latency gates, final customer failure rate at most 0.01%, zero generator drops and zero financial violations. Preserve current recovery and gateway timing. Require the fully passed and restored clean 120/s result as prerequisite. Construct the complete financial audit before fixture upload or customer dispatch. Allow only this exact opt-in profile; retain historical 102/s and 120/s profiles. Setup and mandatory verification remain bounded to 3,600 seconds.

## Alternatives
Repeat 120/s: useful for variance, but does not answer the requested increase. Jump directly to 168/s or add pods: larger uncontrolled step; defer until the measured bottleneck warrants it. Change batching or connection budgets: would confound this comparison.

## Consequences
Only offered workload and its isolated fixture cardinality increase. More offered requests do not prove more successful tickets. Report timestamp-window and conservative throughput, first-attempt errors, recovery, final failures, latency and drops separately. A five-minute pass does not qualify hourly production capacity or an actual flash-sale opening. No accepted application, durability, locking, messaging, TTL or scaling decision is superseded.

## Failure and recovery behavior
Stop escalation if any gate fails. Retain failed evidence and experiment identity. Complete post-TTL payment/ticket relationship checks, zero double-booking, queue drain and exact restoration. Do not relabel the earlier failed result or bypass source/image/budget safeguards. Do not run a second paid stage within this authorization.

## Validation evidence
Before traffic: execute workload, full-cardinality terminal audit and source/configuration guard tests; verify the complete binding against the passed 120/s reference; verify live image, readiness, normal runtime and empty queues. Record executed tests and cloud outcome here after completion. No new tests or cloud traffic have run when this decision is recorded.


### Executed preflight

358 focused regression tests passed in two batches (93 and 265); the import/undefined-name check passed. Both historical120/s and new140/s profiles were exercised, including construction of the full140-show/42000-ticket financial audit and pre-dispatch rejection when audit construction fails. The complete source/configuration binding matched the passed120/s reference under the declared workload-only differences. Live read-only preflight verified exact image/source identity, normal runtime and empty queues. Fresh run `adr0151-f832979c7331` is active; no performance outcome is claimed yet.


### Measured outcome

[Verified cloud result](../capacity/cce/fixed-budget-140-throughput-result-2026-10-15.json): run `adr0151-f832979c7331`, ledger `bounded_cce_ticket_target__323561612a41`, `PASSED_RESTORED`. The offered window was 15 October 2026, 08:06:23-08:11:23 Bangkok time. All 42,000 journeys dispatched and fulfilled with zero initial customer errors, recovery retries, final customer failures or generator drops.

PostgreSQL timestamps counted 41,827 unique paid-and-issued tickets inside the 300-second window (139.4233/s). The two-second inner guard counted 41,547, or **138.49/s using the full 300-second denominator**, 16.74% above the passed120/s reference's118.63/s. Remaining173 tickets completed after the offered window. No application or infrastructure optimization is attributed: offered workload alone changed. Journey p95 rose from1761.06ms to2566.36ms (+45.73%); reservation durability p95 rose from1094.53ms to2026.32ms. Payment HTTP p95 was126.45ms; ticket-wait p95 was625.15ms. These are worst-shard p95 values, not a reconstructed combined percentile.

The mandatory same-run post-TTL audit verified42,000 succeeded payments, fulfilled orders, bookings, tickets and callback deliveries. All seven relationship predicates passed with zero double-booking, payment loss, pending orders, unpublished outbox or dead letters. All twelve measurement gates passed, including complete observer/admission evidence and unchanged final pods, background configuration and ingress. Both pre- and post-restoration queues were zero; the owned namespace/resources and temporary credentials were removed, generator idleness was verified, and normal runtime was restored. No recovery exception was needed.

Customer-zero errors do not mean every internal worker attempt succeeded: diagnostics recorded four reservation PostgreSQL batch error attempts and six OrderPaid event error attempts; terminal replay/recovery converged to complete financial records. Payment-slot failure events were zero. One-second sampling observed database I/O, WAL-write and transaction waits but does not establish a saturation ceiling or prove the historical timeout cause. RDS provider CPU, node throttling and WAL sync timing remain unavailable. The measured result does not qualify one hour at140 offered/s, production concert-opening contention, or maximum sustainable capacity.
