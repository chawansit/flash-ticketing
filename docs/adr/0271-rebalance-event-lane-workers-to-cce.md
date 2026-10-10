# ADR0271: Rebalance event-lane workers to CCE

## Status
Accepted bounded correction following the user's continue instruction and standing uncapped CCE comparison/scaling permission. Supersedes ADR0266's ECS worker placement only for this experiment; default deployment and historic profiles remain unchanged.

## Context
ADR0266 five-minute traffic at 84 offered journeys/s dispatched 24,604/25,200, recovered 129 initial-error journeys and issued all dispatched tickets. It failed capacity qualification. Primary pipeline CPU p95 reached 97.22%; supplemental retained counters attribute approximately 2 cores to application workers. Four CCE APIs together used 1.57 cores. Existing native worker migration already supports ownership transfer, source validation and restoration; adding more API pods does not move the measured background load.

## Decision
Reuse the proven runner, generator, ADR0267 immutable shared image and ADR0266 independent event lanes. Move the fourteen active measured workers to separately admitted CCE containers: six fulfillment consumers, three reservation writers, one projection consumer, publisher, maintenance worker, reconciler and simulator. Confirmation stays disabled on ECS. Retain four API pods at 1 vCPU/2 GiB, fourteen worker pods at 250m CPU/512 MiB; maximum 18 pods, 7.5 requested vCPU and 15 GiB. No new ECS, RDS, DCS or Kafka instances are created.

Keep PgBouncer physical connections 24, admission 20, API pools 4/payment 2, fulfillment pools 6x7 plus projection pool 6 (aggregate 48), simulator HTTP slots 16/SQL pool 10, existing writer budgets, gateway delay and three-attempt customer recovery. Kafka remains on ECS with the existing private owned Kafka forwarder; PostgreSQL continues through the same private pooler bridge. Verify full role environment and source/configuration identities, including these transport adaptations. Stop captured ECS workers before native activation; verify all fourteen processes and both groups before customers.

Run one fresh bounded correction at 84 offered journeys/s for 300 seconds. The retained failed ADR0266 result is a directional baseline, not a passing control. A failed result blocks escalation and hourly qualification. All current customer and financial gates remain. Record a fresh experiment identity and authorization binding. Publish reviewed code/evidence to the codex branch; main requires approval.

## Alternatives
More API pods: API CPU has headroom and background saturation remains. A new ECS: unnecessary infrastructure. Start another load runner: unnecessary. Change SQL/polling/payment simultaneously: prevents attribution. Move Kafka/RDS too: adds financial/topology risks.

## Consequences
Application work gets independent CPU capacity; private forwarding and Kubernetes overhead can offset benefit. Placement plus required transport is the single measured factor. Resource admission and native worker CPU/progress must be observed. This decision guarantees no throughput increase.

## Failure and recovery behavior
Reject image, environment, connection, pod identity, progress or role-count drift before traffic. No overlapping ECS/CCE worker counts. Keep ownership snapshots and acknowledged financial work. Stop dispatch on failure, independently reconcile actual dispatched paid tickets after TTL and drain both Kafka lanes before worker removal. Confirm namespace absence before restarting original workers. Restore exact original runtime/routing and remove only owned resources and credentials. Never reset offsets or discard payments. Cleanup may exceed the one-hour experiment ceiling.

## Validation evidence
Pending affected unit checks, historical overlay verification, native dry-run/readiness/safety and the fresh paid run. ADR0270 already preserves the CPU topology binding. No cloud load or capacity improvement for this correction yet.

Executed local validation: 196 affected unit tests passed, including the new worker identity/resource contract, projection batch progress, burst rollover, conflicting duplicate, endpoint replacement and diagnostic completeness gates. Ruff passed. The reproduction verifier checked 535 retained historical files and 20 current overlays without cloud calls. Cloud preflight has started; paid traffic and capacity outcome remain pending.
