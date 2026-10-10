# ADR0269: Reconcile the failed event-lane paid cohort

## Status
Accepted exact-scope application of the independent read-only recovery protocol from ADR0260/0264. No capacity gate or runtime architecture is changed.

## Context
ADR0266 run adr0151-f80fef5b265c offered 25,200 journeys; 24,604 dispatched and received unique tickets, 596 dropped before dispatch, and 129 initial-error journeys recovered. The original result remains failed. Independent observation rejected primary-host CPU coverage and slot-failure capture; both Kafka lanes drained and runtime restoration completed. The lifecycle exception prevented the paid financial audit from completing, so accounting correctly remains RECOVERY_REQUIRED.

## Decision
Verify only ledger bounded_cce_paid_comparison__b35eb92867cd, original report digest db6de7e66724c8ea8846940b34df93e19bba9cb70486dc5d49828e530134da6d and retained fixture identity. Independently reconcile exactly 24,604 dispatched payments, bookings and unique tickets after hold TTL; audit both duplicate-callback safety cohorts; verify empty queues, stable original runtime, idle generator, absent owned namespace/helpers, retired owned shows and removed private credentials. Use the existing canonical read-only count and relationship audit builders. Closure may change only terminal recovery status to FAILED_RESTORED after every recovery check passes.

Missing diagnostic coverage remains a failed qualification gate. Do not reconstruct CPU or failure-time evidence, mark original audits passing, overwrite the failed report, hide drops, restart load or claim 84/s or hourly qualification.

## Alternatives
Audit the offered count as actual payments: confuses undispatched demand with durable money movement. Leave known customer outcomes unaudited: blocks safe recovery. Treat recovered customers as a capacity pass: hides workload drops and incomplete diagnostics.

## Consequences
Separates financial integrity and resource ownership from performance qualification. The directional latency/backlog improvement can be reported with explicit limits, while first-attempt errors and host CPU pressure remain unresolved.

## Failure and recovery behavior
Fail closed on any changed run, digest, fixture, binding or count; any incomplete payment relationship, queue, cleanup or runtime proof leaves recovery blocked. Queries are read-only and bounded. Do not delete unknown data or reset Kafka offsets. Preserve all original reports and traces.

## Validation evidence
Pending independent cloud reconciliation. Current customer evidence: 24,604 fulfilled, 596 undispatched, 129 recovered journeys, zero final customer failures; journey p95 7.007 seconds (maximum of shard p95s, not a combined percentile). Both observed Kafka groups ended at zero lag. These observations alone do not prove post-TTL payment integrity or capacity qualification.

Executed cloud result: ADR0266 run adr0151-f80fef5b265c offered 84 journeys/s for 300 seconds. All 24,604 dispatched journeys received tickets; 596 were undispatched. All 129 initial-error journeys recovered in 134 retry attempts; final customer failures were zero. Journey p95 was 7.007 seconds, compared with 7.593 seconds in failed ADR0263; paid-unfulfilled peak fell from 348 to 94. Initial errors and drops increased, so capacity improvement is not qualified. Primary CPU/slot capture was incomplete; pipeline samples observed host CPU p95 97.22%, with API CPU 1.572 cores across four CCE pods. Both Kafka lanes drained. ADR0269 independent read-only recovery executed in 89.094 seconds and verified all 24,604 unique durable paid-and-issued tickets after TTL, both safety cohorts, zero double-booking/payment loss, all 86 owned shows retired, queues empty, credentials/helpers/namespace removed and exact stable normal runtime. The original failed report remains unchanged; terminal status is FAILED_RESTORED. See [public result](../capacity/cce/event-lane-paid-result-2026-10-10.json). No hourly qualification or higher load followed.
