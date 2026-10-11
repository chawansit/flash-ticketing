# ADR0281 Qualify the passing committed-status reuse configuration for one hour

## Status

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

Pending: cloud readiness/safety checks, one continuous hourly run, inside-hour durable counts, mandatory post-TTL financial audit, full queue drain and restoration. Hourly capacity is not yet qualified.

Passing short prerequisite: [ADR0280 result](../capacity/cce/committed-status-reuse-result-2026-10-11.json).
