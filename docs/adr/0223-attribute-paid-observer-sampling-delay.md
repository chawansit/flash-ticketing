# ADR0223: Attribute paid observer sampling delay

Status: Proposed

## Context

ADR0222 passed customer, financial, post-TTL, double-booking, queue-drain and restoration checks at 84 offered journeys/s, but failed continuous diagnostic coverage. Its largest 2.574944-second row gap includes 2.35083 seconds before wait diagnostics; the wait collector itself took 49.97 ms. ADR0219 also has a retained gap before wait collection. Existing serial collection does not identify the slow early phase. Neither failed report may be relabeled as passed.

## Decision

Instrument the existing serial observer calls in the ADR0222 profile only: host CPU, PgBouncer, paid cohort query, each worker role and each API. Measure elapsed time with a monotonic clock, including failures, and retain timings in the same raw row. Install the cohort timing wrapper before the wait collector so those costs remain distinct. Keep call order, connection ownership, one-second cadence, SQL, images, workload and every coverage/correctness gate unchanged. This is measurement instrumentation, not an application architecture change or a parallel observer pattern.

Use a fresh registered 84/s five-minute experiment, matched to the retained reference. This is the shortest existing qualified paid profile that exercises accumulating writer load and records the suspected intermittent collection delay. Safety, post-TTL, full financial reconciliation, queue drain and restoration remain mandatory. Do not increase load or start hourly qualification if any gate fails. A subsequent collection or query correction must follow measured evidence and record its decision before implementation.

## Alternatives

- Relax the two-second coverage limit: rejected; it hides missing evidence.
- Parallelize collection immediately: deferred until the blocking phase is measured; it changes observer concurrency and connection use.
- Build another runner or change CCE topology: unnecessary for this diagnostic hypothesis and would confound the retained comparison.

## Consequences

The additional timers add a small local cost and no requests or database connections. A diagnostic-only repeat can fail the same gate; retain that failure rather than claim qualification. Customer improvements already measured remain distinct from complete capacity qualification.

## Failure and recovery behavior

Original exceptions propagate unchanged to existing error classification. Each iteration resets its timing map; later successful rows cannot conceal a missing phase or failed request. Existing cleanup, ownership, audits and service restoration remain authoritative. Rollback removes the opt-in wrapper; no application deployment change is required.

## Validation evidence

Implemented timing instrumentation. The executed local suite passed 115 tests in 385.74 seconds, covering phase attribution, preserved exceptions, iteration reset, separate wait cost, unchanged observer and exact runner contracts. Lint and naming checks passed. See [local evidence](../capacity/flash-sale-opening/paid-observer-timing-local-2026-10-08.json). No new cloud test or hourly qualification has run under this decision.

## Cloud attribution

The matched five-minute repeat reproduced the gap: 2.327144 seconds, including 2,110.82 ms in the paid-cohort query. PgBouncer peaked at 3.14 ms. It scheduled 25,200 journeys, dispatched and fulfilled 25,168, and dropped 32 before dispatch; no dispatched journey failed and no duplicate booking was observed. The original volume and diagnostic gates remain failed. Independent read-only recovery verified the exact dispatched cohort, all relationships, post-TTL, global duplicates, full queues and original runtime without new dispatch.

Read-only EXPLAIN ANALYZE took 906.289 ms while idle. It used an orders sequential scan, returned 25,168 rows, discarded 4,294,687 rows and spent 668.699 ms in that scan. Catalog inspection confirms only primary-key and hold-id indexes on orders, with no event-id index. Payment and ticket lookups already use their existing indexes. This identifies the dominant idle query cost and a targeted correction; it does not establish the cause of every transient spike or qualify hourly capacity. See [cloud attribution evidence](../capacity/flash-sale-opening/paid-observer-timing-probe-2026-10-08.json).
