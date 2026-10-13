# ADR0315: Scale reservation-writer concurrency at fixed database budget

Status: Accepted for a bounded diagnostic comparison; capacity qualification failed.

## Context

ADR0314 kept four API pods but allocated two general and two payment connections per pod. It eliminated final customer failures, yet dropped 3,324 of 30,600 offered journeys. Reservation durability dominated reported latency: p95 was 8.219 seconds. Three writers processed four-command batches with mean PostgreSQL batch time of 115.134 ms. Writer CPU and local connection acquisition were low, suggesting a limit from sequential transaction service time rather than CPU saturation.

## Decision

After independently reconciling the failed reference, repeat 102 offered journeys/s for 300 seconds with four reservation writers instead of three. Change only writer count. Keep four APIs, their 2+2 connection split, all other role counts, immutable application image, per-pod resources, four-command batches, gateway delays, client recovery, shared admission limit of 20, and PgBouncer's 24 physical server connections.

The candidate owns 19 pods: four APIs and 15 background pods, requesting 7.75 vCPU and 15.5 GiB in total. The additional writer retains the existing 12-client local pool ceiling; potential client connections increase, but physical database connections stay capped at 24. Capture and restore the original 14 ECS workers. Prove all 15 native workers absent before restarting the original workers.

Authenticate the exact failed reference, fixture, binding and independent closure. Preserve the zero-drop gate, approved 0.01% final-failure target, existing latency gates, at least 30,000 unique paid-issued tickets within the offered window, financial correctness and queue drain. This supersedes only ADR0271's three-writer allocation for the explicitly selected diagnostic; historical and default profiles retain their original counts.

## Alternatives

Additional writer CPU is not supported by measured CPU use. Raising generator concurrency could conceal an application backlog without improving in-window issuance. Increasing batch size or changing persistence would introduce another factor before measuring writer concurrency. Those options remain separate experiments.

## Consequences

Parallel writers can overlap transaction service time but may increase contention at the same physical database limit. A linear estimate suggested approximately 120 commands/s; it was an estimate, not a capacity guarantee. Atomic claims, durable persistence, idempotency, replay handling, authorization and seat TTL remain unchanged.

## Failure and recovery

Stop load escalation on failed quality or correctness gates. Preserve the original failed evidence. Independently audit every actual payment, expired unpaid order and relationship, verify both safety payments, drain both event lanes, remove owned pods/helpers and credentials, then restore original identities. Recovery closure is not capacity qualification. Rollback restores three writers and the unchanged ECS snapshot.

## Validation evidence

Three pre-dispatch attempts restored without paid traffic:

- adr0151-068b514e0efb: transport whitelist rejected writer index 3.
- adr0151-6e096f5352b8: admission renewal expected 14 rather than 15 native workers.
- adr0151-bee5fe97b8ac: observer startup still assumed three writers.

The transport, renewal and declared-count startup/terminal checks now select four writers only for ADR0315. Full current binding and historical/current overlay checks executed successfully. A broader archived reproducibility test still reports an existing observe_two_host_pipeline.py lock mismatch; it is not claimed as passing.

Paid run adr0151-6fd6d0275799 completed 30,529 dispatched journeys, with 71 generator drops. All 39 initial-error journeys recovered; final customer failures were zero. Reported journey p95 fell from 8.908 to 3.353 seconds, and durable wait p95 from 8.219 to 2.015 seconds. The database recorded 30,370 unique paid-issued tickets inside 300 seconds, including 30,155 in the conservative inner window. In-window issuance improved 13.19% relative to ADR0314.

The original observer resource summary still assumed three writers and failed role coverage. The raw result remains unchanged. A separate ADR0315-only summary adapter now validates all four writer processes from every extended raw row, while retaining CPU, pool, completeness, error and counter-reset checks. Recomputing the retained trace passed those diagnostic checks; it does not override failed customer quality.

Independent read-only closure reconciled all 30,529 payments/bookings/tickets, seven relationship predicates and both safety payments. Queues were empty, the namespace and helpers absent, and original runtime identities restored. The ledger closed FAILED_RESTORED, with capacity_qualified false.

Executed validation: 208 focused unit tests passed, including 14 adversarial writer-summary tests; Ruff passed for the changed Python files. No further paid load followed the failed gate.

[Measured outcomes and independent recovery](../capacity/cce/headroom-slot-writer-comparison-2026-10-13.json). This does not qualify the four-writer configuration for an hour or for a simultaneous flash-sale opening.
