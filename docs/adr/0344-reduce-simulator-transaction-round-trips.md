# ADR0344: Reduce simulator transaction round trips

## Status

Accepted for local implementation and qualification only. Extends ADR0341's indexed due-time cutoff and ADR0193's atomic-claim approach. Supersedes the two-command claim in the development simulator only after local correctness and protocol validation. No cloud deployment, paid allowance or production adoption is authorized by this decision.

## Context

The ADR0343 unchanged-image 180/s control failed with 12,035 drops and two final journey failures. Retained observer-lifetime means are 16.282 ms for claim, 16.434 ms for acknowledgement and 78.231 ms for successful delivery. The database executed the claim itself in 0.328 ms on average. Status snapshots missed 144,002 times and generated 142,648 committed status reads. These observations suggest reducing callback slot occupancy may reduce confirmation backlog and repeated reads; they do not establish a single causal bottleneck.

ADR0193 previously measured a modest claim improvement with one atomic statement at lower load, without proving higher capacity. ADR0255's API read pipeline was not adopted, and ADR0320's writer pipeline was rejected. Neither is re-enabled here. The current one-second cache age, polling, HTTP slots, database clients, gateway delays and recovery remain fixed.

## Decision

Replace the simulator's SELECT-then-UPDATE claim with a single CTE UPDATE RETURNING statement. Select the earliest eligible attempt using the existing partial due index, statement-time due cutoff, wall-clock lease eligibility and FOR UPDATE SKIP LOCKED. Write the same fresh token and 15-second lease atomically; return the same payment/order fields and database-clock pickup observation. Keep all joins required for a valid order inside the selected cohort and limit each claim to one attempt.

Locally evaluate using the existing transaction pipeline for simulator claim and acknowledgement only. Adopt it in this candidate only if the actual driver/database correctness checks and a bounded local protocol comparison demonstrate reduced transaction occupancy; otherwise retain sequential transaction setup. Preserve explicit BEGIN, transaction-local timeouts, commit durability, rollback and pool return. Never hold a SQL connection during HTTP delivery. This does not alter API status reads, payment writes or reservation batching.

Prepare the candidate from the exact immutable ADR0324 source and dependency inputs. Allow changes only inside simulate_one, including the already reviewed ADR0341 predicate. Verify the complete source map and imported module in a network-disabled local image. Keep old published image receipts unchanged and do not register this new source as the old candidate.

## Alternatives

Increase simulator slots or database pools: previously mixed results and outside this fixed-budget correction. Extend cache TTL or slow client polling: changes freshness/workload. Coalesce reads of different actor/order identities: unsafe; same-order fill races account for only 937 outcomes here. Re-enable rejected API/writer pipelines: not supported by their evidence. Remove financial writes, timeouts, token fencing or commit acknowledgement: rejected for correctness.

## Consequences

One dependent claim command is removed per delivered callback. Pipelining may reduce setup waits, but enqueue timings are not execution times and fewer messages are not automatically fewer network round trips. Measure whole operation time. Cloud capacity remains unknown. This targets upstream confirmation progress while keeping the status freshness contract intact.

## Persistence, locking, messaging, idempotency, TTL and scaling

PostgreSQL remains the authority. The row lock and lease update share one transaction; SKIP LOCKED preserves independent bounded workers. Failed or uncertain commits produce no HTTP delivery from that call. HTTP starts only after claim commit. Callback/payment identities, repeated delivery targets, outbox and inbox semantics are unchanged. Acknowledgement still updates only the owning token after successful HTTP response; uncertain delivery retains the lease for existing recovery. Cache TTL, 15-second lease, worker count, delivery concurrency and connection budgets are unchanged. No new messaging, persistence store or background thread is introduced.

## Failure and recovery behavior

SQL/protocol/commit errors propagate through the existing rollback and pool-return path. No delivery follows a failed claim commit and no successful acknowledgement is inferred from an error. Lost delivery response and stale-token recovery remain idempotent. Stop scheduling drains only owned in-flight tasks. Restore the immutable parent image on regression. No paid retry or gate relaxation is authorized.

## Validation evidence

Before implementation, inspected retained ADR0343 query costs and phase counters, actual simulator/database/cache source, and ADR0193/ADR0255/ADR0320 results. Implementation, local native tests, protocol comparison and candidate build are pending. Append only executed outcomes; local evidence is not cloud throughput or database reconciliation.

## Executed local validation

Implemented the atomic claim and simulator-only transaction pipeline.53 focused checks passed:34 native eligibility/commit-failure/concurrency/scheduler/preparation checks and15 full financial replay/concurrency cases, plus4 signed-HTTP acknowledgement-failure/replay cases. Ruff passed. The actual new preparer built the immutable-parent local image with networking disabled and verified all22 copied/installed/imported runtime modules; only simulate_one changes. Existing published receipts were not altered.

Six alternating trials per arm,128 actual worker calls each, measured median local claim-plus-ack time6.4732ms for cutoff/sequential,5.2930ms for cutoff/pipeline,6.7839ms for atomic/sequential and5.0372ms for atomic/pipeline. The combined candidate reduced local operation time22.18% versus cutoff/sequential. Atomic SQL alone did not improve this local measurement. Retain pipelining only for this simulator candidate; do not infer benefit for the rejected API/writer pipelines. The benchmark omits HTTP delay and cloud contention and measures no paid issuance rate.

An initial new concurrency test failed its cold fixture's10-waiter guard before10connections were established. Warming that same10-client ceiling corrected the fixture; final native and financial concurrency checks passed without increasing an application/cloud budget.

[Sanitized local evidence](../capacity/cce/simulator-roundtrip-local-2026-10-17.json). Local image flash-ticketing-shared:adr0344-56af658a57e6 is unpublished; cloud throughput and status-read reduction remain unmeasured. Existing ADR0343 failed control and158.0633/s passing reference are unchanged. Live runner registration for this distinct source and any paid comparison remain future scope.
