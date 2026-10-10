# ADR0263: Bound payment dispatch slots independently

## Status
Accepted for one isolated configuration correction and bounded five-minute test. Supersedes ADR0251's twelve delivery slots for this experiment only. ADR0261 shared-image identity and batching remain unchanged; worker placement comparison is deferred.

## Context
The latest 84 offered journeys/s run completed 24,410 tickets with 790 generator drops, journey p95 7.233 seconds and simulator due-to-claim mean 2.682 seconds. Claim, delivery and acknowledgement mean times were 13.751, 118.003 and 12.946 ms. Twelve occupied delivery slots imply an approximate 83 deliveries/s service ceiling using these means. This is an investigative estimate, not proof of the sole bottleneck: database tails, status polling and primary CPU remain relevant.

## Decision
Increase only SIMULATOR_CONCURRENCY from 12 to 16 in the same immutable ADR0261 image. Keep refill dispatch, simulator SQL pool 10, PgBouncer physical connections 24, API acquisition budget 20, four API pods, ECS worker counts, generator concurrency 500, customer recovery settings, gateway delay distribution and all quality gates fixed. The existing claim transaction ends before HTTP delivery; acknowledgement uses a separate transaction. More delivery slots must not hold extra SQL connections while waiting on the gateway.

Reuse the proven bounded CCE paid runner for a single correction run: 84 offered journeys/s for 300 seconds, with mandatory post-TTL financial audit, zero double-booking/payment loss, complete queue drain and exact restoration. Register a fresh goal and experiment identity. No ECS-vs-CCE placement arm, hourly qualification or automatic further concurrency increase.

## Alternatives
Reduce gateway delay: unrealistic and changes the workload. Increase SQL pool or API replicas: mixes factors and risks database pressure. Move workers now: mixes placement with dispatch capacity. Retain twelve slots: preserves the measured payment backlog.

## Consequences
Expected benefit is less payment dispatch waiting and fewer status reads. More simultaneous callbacks may worsen database contention or CPU. A result must report scheduled/dispatched/completed/in-window tickets, initial errors, retries, final failures, journey latency and drops separately. Success counts including the completion tail do not prove hourly capacity.

## Failure and recovery behavior
Preserve payment leases, idempotent callback acknowledgement, duplicate replay and restart recovery. Stop load escalation if quality fails. Restore the exact original topology/configuration after the experiment; never reuse a failed report as passing control. Authorization remains within the approved bounded testing envelope.

## Validation evidence
Pending executed concurrency/transaction-boundary tests, sealed runner checks and a fresh paid run. Capacity improvement remains unmeasured for sixteen slots.

Executed 81 integration checks passed inside the unchanged immutable ADR0261 image with isolated PostgreSQL 17.6/Redis 7.4.5. Delivery/SQL pool combinations 12/2, 16/2 and 16/10 verify SQL availability while all deliveries wait, lost committed responses and stale lease replay, unique payment effects and ticket issuance. Targeted runner checks passed 212 with one skip; profile/dependency checks passed 123. Ruff passed. A fresh single-stage goal is registered; cloud outcome pending.

Fresh five-minute run completed 24,722 unique tickets, recovered 12 initial customer errors and had 478 undispatched journeys (1.8968%). Payment pickup mean fell from 2.682 seconds in the retained reference to 105.575 ms; paid-unfulfilled backlog peaked at 348 (p95 325) and consumers recorded 24 lock timeouts, SQLSTATE 55P03. Journey p95 7.593 seconds remains failing. Primary ECS mean CPU 84.858%; CCE API aggregate process CPU 1.590 cores. Diagnostics and unchanged native runtime passed. Independent ADR0264 audits verified all actual paid/ticket relationships, no duplicates/loss, empty queues and exact restoration; original capacity result remains failed. This configuration is experimental, not a production default. Public evidence: [sixteen-slot result](../capacity/cce/payment-dispatch-16-result-2026-10-10.json).
