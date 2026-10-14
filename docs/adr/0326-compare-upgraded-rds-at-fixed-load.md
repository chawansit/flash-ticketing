# ADR0326: Compare the upgraded RDS at unchanged ticketing load

## Status

Short comparison passed and restored. Hourly extension remains unimplemented and unexecuted. The user changed RDS from 4 vCPU/16 GB to 8 vCPU/16 GB and instructed continuation on 15 October 2026 Bangkok time. No application architecture, durability rule or accepted decision is superseded. The upgraded class is user-declared; PostgreSQL readiness/configuration and workload behavior are independently checked.

## Context

ADR0324 passed 102 offered journeys/s for five minutes at 100.90 conservative paid-and-issued tickets/s. Five initial payment errors recovered, but commit stalls remained. RDS CPU evidence supplied by the user showed an 81.5% peak over a broader window. CPU scaling does not establish a proportional throughput gain or a WAL/storage root cause.

## Decision

Repeat the existing ADR0324 immutable image and five-minute workload after read-only readiness and normal-runtime recovery checks. Preserve four API pods, fifteen background pods, four reservation writers at batch four, six fulfillment consumers at batch four, 24 physical database backends, admission 20, concurrency 500, gateway timing, customer recovery and freshness. Use fresh experiment/fixture identities and retain prior evidence unchanged. Record the new 8-vCPU/16-GB hardware declaration separately from the original software reference.

If all short-stage gates pass, prepare a separately bounded 102 offered journeys/s, 3600-second qualification targeting at least 360000 unique paid-and-issued tickets inside the hour. Its exact new workload/profile binding must be verified before dispatch. Retain the existing 5400-second hourly ceiling and mandatory audits/restoration. No automatic load increase or further infrastructure resize.

## Alternatives

Increasing connections, API pods or changing code simultaneously would prevent attribution. Reusing the old fixture or a consumed run identity would weaken auditability. Treating a five-minute rate as hourly qualification is rejected.

## Consequences

The matched repeat measures the upgraded environment with the same software and budget; it is not a randomized hardware comparison. RDS CPU metrics require a matching provider window if unavailable through SQL. More CPU may leave lock/commit stalls unchanged. The hour stage needs sufficient isolated fixture inventory and customer identity lifetime without changing booking semantics.

## Failure and recovery behavior

Any failed short gate blocks hourly escalation. Preserve first errors, recoveries, final failures and generator drops separately. Require the approved 0.01% final customer-error limit, unchanged latency gates, zero double-booking/payment loss, post-TTL durability and full queue drain. Always restore the captured runtime and remove owned resources/credentials; ambiguous outcomes require reconciliation before a fresh attempt.

## Validation evidence

Run `adr0151-6fe62a507a9a`, ledger `bounded_cce_ticket_target__0c2bceb2cf11`, passed all twelve measurement gates and restored the normal runtime. The offered window was 15 October 2026, 00:54:57-00:59:57 Bangkok time. All 30600 journeys dispatched and completed, with zero initial customer errors, retries, final failures and generator drops. The post-TTL financial audit found 30600 unique paid-and-issued tickets, zero duplicate booked seats and no broken relationships. Queues drained before and after restoration; owned CCE resources and temporary credentials were removed.

The timestamp window contained 30473 tickets; the two-second inner-window guard retained 30266, or 100.8867 tickets/s using the full 300-second denominator. Worst-shard journey p95 decreased from 2614.88 to 1704.57 ms. Captured slot failures decreased from eight to zero. These are one sequential, fixed-offered-load comparison; they do not measure a new maximum rate or prove that CPU alone caused the improvement. Provider RDS CPU and WAL write/sync timing remain unavailable. Internal worker-attempt error counters remain visible in the report despite zero customer failures.

ADR0310 remains the earlier 300000-ticket hourly qualification. The existing hourly runner is fixed at 84 offered journeys/s; it must not be relabeled as the new 102/s profile. The upgraded environment has not been qualified for one hour at 100 successful tickets/s.

[Verified upgraded-RDS result](../capacity/cce/upgraded-rds-fixed-load-comparison-2026-10-15.json).

[Prior short comparison](../capacity/cce/fulfillment-transaction-batching-comparison-2026-10-14.json).
