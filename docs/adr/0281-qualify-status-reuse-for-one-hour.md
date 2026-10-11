# ADR0281 Qualify the passing committed-status reuse configuration for one hour

## Status

Executed: failed hourly qualification, independently restored. 300,000/hour remains unqualified.

Accepted for one bounded hourly experiment. Implemented and offline tested; cloud qualification pending.

## Context

ADR0280 passed the unchanged 84 offered journeys/s, five-minute quality gates. It dispatched and completed all 25,200 journeys with zero initial/final customer errors, generator drops, double-booking or payment loss. It issued 25,097 unique paid tickets inside the offered window (83.6567/s); 103 followed during drain. This exceeds the target arithmetically but does not establish one-hour capacity.

## Decision

Run one fresh hourly qualification of that exact application digest and configuration. Offer 84 journeys/s for 3,600 seconds using 1,008 isolated shows and 302,400 unique viewers/seats. Require at least 300,000 unique paid-and-issued tickets within the hour, including the existing conservative two-second boundary guard. Retain every customer latency/error, payment durability, expiry, zero-double-booking, queue-drain, source identity, resource and restoration gate.

Keep four 1-vCPU/2-GiB APIs, fourteen 250m/512-MiB workers, six fulfillment consumers, one projection consumer, three writers, the ADR0279 native database bridge, physical database cap 24, admission 20, consumer pool budget 48, simulator concurrency 16/pool 10 and its unchanged gateway delays. Enable ADR0280 reuse only on the six fulfillment consumers, with original one-second freshness and financial transactions unchanged. Keep two generator shards, 500 total active journeys, existing polling and at most three recovery attempts.

Reuse the existing hourly allocation/coordinator and ADR0278 timeout-retention workload, binding their exact identities together. Change only duration, cohort size and necessary lifetimes/coverage: API and worker deadlines 5,400 seconds; live admission capture bounded to 4,300 seconds/440 samples; existing hourly observer cadence. Preserve the short profile's 3,000-second worker lifetime and 600-second/64-sample collector. Bind the immutable passing ADR0280 evidence; reject stale or failed prerequisites and any undeclared runtime changes before paid dispatch. Keep the approved 90-minute experiment ceiling including setup, TTL audits, drain and restoration. No cumulative time cap or new machine sizing.

## Alternatives

- Extrapolate the five-minute rate: insufficient evidence for sustained capacity.
- Increase resources or offered rate now: confounds the passing configuration and is unnecessary for this qualification.
- Replace the runner or relax quality gates: unnecessary; retain the proven hourly runner and explicit extension binding.

## Consequences

This measures sustained distributed paid-ticket throughput with the same resource budgets. It does not establish simultaneous flash-opening capacity, the separate 18,000-seat projection fix, real bank behavior, failover qualification or a universal maximum RPS. Hourly results must retain generator drops, first-attempt errors, recovery attempts, final errors and customer latency separately.

## Failure and recovery behavior

Do not replay a consumed stage. Preserve failed evidence and stop further load increases if any gate fails. Audit the actual dispatched cohort after hold expiry, drain both Kafka lanes and outbox, remove only owner-bound resources/private inputs, and restore the original runtime and disabled reuse setting. An incomplete cleanup remains recovery-required; no capacity claim follows a failed or incomplete run.

## Supersession

Extends ADR0280's accepted short configuration into one separately authorized hourly experiment. Supersedes its short-only experiment binding and ADR0271 worker lifetime only for explicit ADR0281 hourly goals. Does not supersede prior results, financial correctness, TTL, messaging semantics, default settings or short-test gates.

## Validation evidence

Executed offline: 155 focused regressions passed. They cover exact hourly scope and prerequisite rejection, unchanged runtime budgets, short defaults, complete-hour admission capture with overlap rejection, worker lifetimes, hourly allocation and financial boundaries, recovery and transferred timeout-retaining imports. Ruff passed. All 535 frozen reproduction inputs verified unchanged. The historical hourly test fixture now explicitly selects its historical resources instead of inheriting newer live extension markers.

Executed the unchanged hourly load on 11 October, 22:52:06–23:52:06 Bangkok. Result: **FAILED_RESTORED**. Of 302,400 scheduled journeys, 300,228 dispatched and obtained unique paid tickets; 2,172 were undispatched (0.7183%). All 302 initially failing journeys recovered with 313 attempts and zero final customer failures. Worst-shard journey p95 was 5,568.04 ms. The hour contained 299,782 unique paid tickets (83.2728/s), 218 short of target; the conservative guarded window contained 299,593. Another 446 tickets issued after the hour. Do not qualify capacity from the 300,228 total after drain.

The pipeline retained 156 QueryCanceled rows; successful cohort-phase p95 was 2,057.44 ms and maximum observation gap 3.459 seconds. CPU collection also rejected the hourly worker-isolation profile; native coverage/slot diagnostics failed. No complete CPU attribution or confirmed causal storage/WAL diagnosis follows this run. The original report and failed gates are immutable.

After original restoration failed on the stopped candidate API, independent recovery restarted the exact saved original topology. ADR0282 audited all 300,228 dispatched payments/bookings/tickets and every financial relationship in one read-only snapshot: zero double-booking, payment loss or relationship errors; both safety fixtures passed. All global queues drained, original runtime was stable, namespace/helpers/private inputs were removed, generator idle, secondary empty and all 1,010 owned fixture events retired. Recovery changes terminal status, not qualification. The last independent verification took 152.59 seconds; earlier failed recovery attempts remain private evidence. Time accounting includes 73.07 minutes for the original experiment plus 21.63 minutes of recovery work, including failed attempts. Cleanup exceeded the 90-minute ceiling under the standing cleanup exception; no second paid stage was run.

Evidence: [hourly result](../capacity/cce/status-reuse-hourly-result-2026-10-11.json), [independent terminal recovery](../capacity/cce/status-reuse-hourly-terminal-recovery-2026-10-11.json), [ADR0282](0282-bound-hourly-terminal-relationship-audit.md).

Passing short prerequisite: [ADR0280 result](../capacity/cce/committed-status-reuse-result-2026-10-11.json).

Read-only cloud preflight passed on 11 October. Before any reservation/customer dispatch, hourly registration rejected a stale live-plan lookup during proposed-envelope validation. The validator now checks the supplied hourly goal before persistence; 41 affected regressions passed, including this exact case. No paid stage was consumed or deployed by that attempt.
