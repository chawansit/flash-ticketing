# ADR0166: Retain bounded admission failure evidence before teardown

- Status: Accepted for implementation and local qualification; no new cloud allowance
- Date: 2026-10-06

## Context

ADR0165 control passed all required gates with 18,000 paid-and-issued tickets. Candidate had one customer payment 503, one unpaid expired order and 17,999 paid tickets. All acknowledged successful payments had tickets; observed duplicate bookings were zero and all queues drained. Both runtimes restored. One-second samples show a native payment pool timeout at 500 ms, admission use below its cap, and elevated commit time. This is not evidence that stale admission occupancy caused the failure, nor that reclamation caused regression. Request-correlated structured snapshots exist in the image but were lost when experimental containers were removed.

## Decision

For the exact ADR0163 admission profile, collect allowlisted structured acquisition-failure records from each verified API container before teardown. Verify container ID, image and start time before and after reading logs. Bound input bytes, line size, elapsed time and records; return explicit incomplete evidence on truncation, changed identity, missing snapshots or collection failure. Never retain SQL, DSNs, credentials, request payloads, arbitrary exception text or general access logs. Preserve reason, role, elapsed time, request correlation UUID, native pool numbers and guard numbers/flags. Verify that records cover the observed fixed reason counters. Incomplete diagnostic coverage fails the experiment without suppressing mandatory financial audits or cleanup.

Use the existing shared lifecycle, immutable source bindings and owned evidence directories. Preserve historical profiles. Do not change application images, concurrency, transaction boundaries, connection budgets, deadlines, retry or architectural deployment. No load or replacement protocol is authorized by this decision; the consumed ADR0165 scope remains closed.

## Alternatives

Keeping all Docker logs can expose sensitive data and be unbounded. Collecting after restoration loses evidence. Metrics alone identify failure class but cannot provide an atomic failure snapshot. Ignoring a collection failure would repeat that gap. None is selected.

## Consequences

Adds bounded read-only diagnostics outside the offered load window. Measurements still distinguish atomic guard snapshots from adjacent native statistics. Failed diagnostic collection blocks claims of qualified performance but never changes actual financial results. Reclamation remains off outside experiments; no capacity improvement is claimed.

## Failure and recovery

Read exact verified API containers only. Reject image/start-time drift, oversized input, overlong lines, invalid numerical records and missing counter coverage. Continue independent financial/post-TTL/drain/retirement/restoration steps on diagnostic failure. Retain existing ownership locks if cleanup is ambiguous. Do not replay failed protocols.

## Validation evidence

Executed 271 local regression tests passed in 32.89 seconds, including parser/privacy checks, bounded stream input, verified container identity, per-replica counter coverage, generated remote program compilation, lifecycle order and historical profile compatibility. Changed-file Ruff and Git whitespace checks passed. Earlier test source syntax issues were corrected before the passing run. No cloud test or load started under this decision. [Local validation](../capacity/flash-sale-opening/admission-failure-retention-local-validation-2026-10-06.json).

## Relationships

Closes the evidence-retention gap in ADR0164/0165 orchestration. Leaves ADR0162/0163 financial behavior and source/images unchanged. [Measured result](../capacity/flash-sale-opening/partial-timeout-measured-comparison-2026-10-06.json).
