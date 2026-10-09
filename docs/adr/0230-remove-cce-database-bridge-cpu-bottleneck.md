# ADR0230: Remove CCE database bridge CPU bottleneck

Status: Accepted; short paid stage measured, full-run and hourly qualification incomplete.

## Context

The first CCE 84/s, 300-second paid run dispatched every journey but failed customer gates. Its database bridge was limited to 0.125 vCPU, relaying all four APIs to the unchanged PgBouncer pool. Retained 61 CPU samples show mean bridge usage 0.1295 cores and 59 of 60 intervals above 0.11 cores. APIs used 1.1507 cores combined; database acquisition averaged 109.6 ms. This strongly supports bridge CPU contention, but a controlled comparison is required to attribute improvement.

## Decision

Raise only the bridge CPU limit to 1 vCPU on the existing ECS. Retain its network, TLS forwarding, image, command, 64 MiB memory, four 1-vCPU/1-GiB API pods, 24 PgBouncer server connections, application pools, 84 offered journeys/s for 300 s, simulator and all customer/correctness gates. Bind and verify the bridge allocation explicitly. Compare against the failed CCE baseline. This supersedes only ADR0228's 0.125-vCPU bridge allocation; other decisions remain accepted.

Preserve helper owner in CPU trace serialization so full CPU validation can execute; this changes observation identity only. Do not reinterpret the original failed observation gate as passed.

## Authorization and boundaries

The user instructed "Continue until finish 84/second" after approving uncapped comparison/scaling spending. Archive the consumed original goal authorization and create a new bounded one-stage authorization for this correction. Retain every old scope and counter unchanged. One fresh registered comparison, maximum 3600 s, immediate owned cleanup and all financial/TTL/drain/restoration checks. No machine resize, unrelated infrastructure or unattended schedule.

## Alternatives

Add API pods, increase DB connections, bypass PgBouncer or replace the bridge. Those change additional factors and are not justified while a common CPU-limited relay is saturated. Keeping the bridge cap would repeat a known choke point.

## Consequences

The bridge may consume up to one existing ECS core, potentially contending with background workers. Observe both bridge and worker CPU plus database waits. Successful HTTP admission alone is insufficient; require all scheduled customer outcomes and paid/issued durability. The target remains 84 successful tickets/s, not just offered journeys/s.

## Failure and recovery

Stop progression on failed gates, preserve raw evidence, drain queues and independently reconcile ticket/refund/unpaid terminal states before another fresh experiment. Remove exact owned namespace/helpers and restore original services. Rollback removes the owned bridge; baseline deployment is unchanged. No retries hide errors or correctness relaxation.

## Validation evidence

Executed read-only retained CPU capture: tmp/adr0151-5db81bfa8ec2/primary-cpu-retained.private.json. Original result: docs/capacity/flash-sale-opening/cce-paid-comparison-2026-10-09.json. Local allocation, ownership serialization and authorization tests must pass before cloud launch; results recorded in CURRENT_STATE. Before launch, capacity improvement remained unmeasured; the executed result below records the outcome.

Executed result: adr0151-73e82b8d5210 completed all 25,200 distinct paid-and-issued customer journeys, with zero drops or customer failures. All nine measurement gates passed. API pool acquisition mean fell from 109.56 ms to 4.05 ms; pool errors from 26,256 to zero. API process CPU averaged 1.236 cores, bridge 0.345 cores, and primary host 67.74%. The original runner failed intermediate candidate restoration. Final ECS restoration and independent post-TTL financial, resource and queue verification passed; ADR0229 closes recovery while preserving the failed report. Evidence: docs/capacity/flash-sale-opening/cce-bridge-comparison-2026-10-09.json. Five-minute target measured; full runner and hourly qualification remain unproven.
