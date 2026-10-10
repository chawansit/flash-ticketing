# ADR0264: Close failed dispatch correction without qualifying capacity

## Status
Accepted for exact-scope recovery of ADR0263 run adr0151-692aefc90917. Extends ADR0260's independent recovery protocol to this correction; historical targets and original failed results remain unchanged.

## Context
The sixteen-slot test completed all 24,722 dispatched journeys, recovered 12 initial customer errors and dropped 478 of 25,200 scheduled journeys. Journey p95 was 7.593 seconds. Native runtime continuity and diagnostic completeness passed, but expected-count financial gates correctly failed because the offered cohort was not fully dispatched. The runner restored runtime and removed owned helpers; it retained RECOVERY_REQUIRED until actual paid-ticket relationships are independently verified.

## Decision
Register only the immutable run, ledger bounded_cce_paid_comparison__862f9e161c91, original result digest 289eab78fa43736913cb31d22e2ccfe2fb7e6c651f2d69ab30430a2defad79b1, dispatched count 24,722, drops 478 and recovered count 12. Reuse ADR0260 read-only post-TTL counts/relationship audit, both duplicate-callback safety checks, complete queues, stable original runtime, owned namespace/helper absence, generator idleness, retired exact fixture ownership and cleared credentials.

Require the exact ADR0263 correction binding (sixteen delivery slots, ten simulator SQL connections, correction arm), unchanged native pods and complete diagnostics. Closure changes only the terminal restoration state to FAILED_RESTORED. The failed customer latency, drops, original report and capacity qualification remain unchanged.

## Alternatives
Treat undispatched journeys as lost payments: mixes generator drops with actual money movement. Ignore the failed ledger: leaves ownership unresolved. Mark the test passed after actual-cohort audit: weakens the offered workload gates and is rejected.

## Consequences
Allows safe subsequent engineering after proving actual customer money/tickets and restoration, without manufacturing a passing capacity result. More dispatch concurrency reduced payment pickup delay; the remaining paid-to-ticket backlog and consumer lock timeouts require separate diagnosis.

## Failure and recovery behavior
Reject any unrelated run/digest/count, weakened financial relationship check, nonempty queue, changed original runtime, missing native continuity proof, or unproven cleanup. Do not restart load before closure. Never delete unknown fixtures or reconstruct ownership from the database alone.

## Validation evidence
Pending executed exact-target regression checks and independent cloud recovery. No capacity pass or hourly qualification claimed.

Executed 27 exact-target/financial/recovery checks passed. Independent cloud audit completed in 71.609 seconds, verifying 24,722 unique durable paid-and-issued tickets, both duplicate-callback safety tickets, zero relationship errors/double-booking/payment loss, empty queues, all 86 owned shows retired and restored runtime, namespace/helper absence and cleared private credentials. Closed FAILED_RESTORED with original report digest preserved and no capacity qualification.
