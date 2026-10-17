# ADR0341: Bound payment eligibility at claim statement start

## Status
Accepted for local implementation and validation only. Cloud deployment and paid load are not authorized. Extends the existing payment dispatch index and simulator claim behavior; no accepted ADR is superseded.

## Context
ADR0338 passed 160 offered journeys/s for 300 seconds at 158.0633 conservative paid-issued tickets/s. ADR0339 failed at 180/s with 11,145 generator drops despite recovered customer errors and independent financial restoration. Pickup mean increased from 0.105s to 3.146s and status-cache misses from 48,759 to 138,178. These lifetime observations do not establish causality.

The exact ADR0324 measured workers source matches the checkout. Its claim predicate compares indexed due_at with volatile clock_timestamp(). PostgreSQL cannot use that volatile expression as an index range bound. Future pending attempts can therefore be repeatedly visited by idle delivery slots. This is a specific local inefficiency, not proof of the cloud bottleneck.

## Decision
Change only the due_at eligibility cutoff to statement_timestamp(). Keep the lease expiration test and lease creation on clock_timestamp(). An attempt becomes eligible on the first claim statement starting at or after its due_at; a statement never claims a future attempt merely because its scan takes time. Preserve the existing partial due index, two-statement claim transaction, FOR UPDATE OF p SKIP LOCKED, and token-fenced acknowledgement.

Persistence/locking remain PostgreSQL row locks and the existing 15-second lease. Messaging and callback/payment IDs remain unchanged. No inline delivery retry, extra queue, cache or cache TTL change is introduced. Financial idempotency remains the callback handler's responsibility. Scaling stays at sixteen delivery jobs, ten simulator DB clients and the qualified API/backend budgets. Status polling and freshness remain unchanged; reduced pickup delay might reduce polling but that is unmeasured.

## Alternatives
Retain the volatile predicate: preserves unnecessary future-row scanning. Atomic claim/batching: a larger change with separate historical work. Add retries or concurrency: does not correct the scan and confounds the fixed-budget comparison. Change status-cache freshness: outside the correctness envelope.

## Consequences
The existing index can bound due eligibility. Attempts becoming due during a claim scan wait for a later scheduler iteration. Active leases are still filtered and may still cause scanning; this does not solve every form of backlog. There is no claim of throughput improvement.

## Failure and recovery behavior
Claim rollback releases row locks without delivery. Delivery failure leaves the existing lease to expire; retry uses the same callback/payment IDs. Ack failure remains replayable and token fenced. Stop drains owned jobs as before. Recovery is source/image rollback to ADR0324; no schema migration is required.

## Validation evidence
Executed with LOCAL_CLAIM_TESTS=1: `pytest tests/integration/test_simulator_claim_cutoff.py tests/unit/test_simulator_dispatch.py -q --tb=short -p no:cacheprovider`: **20 passed**. Five checks use a disposable PostgreSQL 15 container and the actual simulate_one path/query. They cover future/incomplete/completed eligibility, intentional duplicate delivery targets, SKIP LOCKED, delivery outside the claim transaction, lease recovery with the same callback ID and stale-token acknowledgement fencing. With 20,000 future pending attempts, native EXPLAIN ANALYZE showed the old predicate removing at least 20,000 rows by filter; the candidate used `due_at <= statement_timestamp()` as an Index Cond and removed zero. Neither returned an eligible row. This synthetic idle/future-heavy case does not reproduce the saturated cloud workload or establish a cloud throughput gain.

The first native attempt failed at database readiness (15 scheduler checks passed); moving the disposable database data directory to tmpfs and allowing bounded startup fixed the test setup. The successful run above replaced no failed cloud evidence. Focused Ruff checks passed.

Actual local preparation invoked the existing `prepare_fulfillment_batch_image.prepare` with `cce_fulfillment_batch_profile` as the measured parent and a strict overlay: require the complete measured source map, require exactly one `p.due_at<=clock_timestamp()` occurrence, replace it with `p.due_at<=statement_timestamp()`, and require exact equality to the checkout workers bytes. All other runtime bytes remained pinned. The generated manifest/label identified ADR0341 and the sole workers change. The existing installer verified copied and installed package bytes during a network-disabled local build. A network-disabled candidate run verified all 22 source modules and the imported worker's hash/predicates. [Local image receipt](../capacity/cce/payment-claim-cutoff-local-2026-10-17.json) retains both source maps, dependency hashes, image identity and the local preparation recipe hash. The owned recipe is `tmp/adr0341-prepare.py`; no new maintained runner or cloud profile was added.

The compact retained-evidence audit for ADR0339 passed, including its hash-bound independent recovery. It performed no database audit and left quality FAIL. No cloud calls, publication, deployment, infrastructure changes or paid load occurred. Best passing throughput remains 158.0633/s.

## Next test proposal
First retain/register immutable candidate preparation and verify the unchanged role configuration, then request a new fixed-budget allowance for a matched control/candidate comparison at 160 offered journeys/s for 300 seconds per arm. Use the exact ADR0338 resource, pool, simulator, gateway-delay, polling and freshness settings. Retain the original correctness/latency/zero-drop gates and existing financial/post-TTL/drain/restoration audits; stop after a failed control. Compare conservative paid-issued rate, pickup delay, cache misses per dispatched journey, API CPU and acquisition latency. Do not increase to 180/s until a reproducible passing comparison supports it. No part of this proposed paid comparison is authorized or executed.
