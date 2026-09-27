# ADR 0057: Isolate refresh and expiry maintenance lanes

Status: Accepted for controlled validation; sustained 1,000 RPS remains unproven.

This decision supersedes ADR 0050's deferral of separate maintenance processes and follows ADR 0053's requirement to isolate WAL commit concurrency. ADR 0054's refresh batching and ADR 0056's strict versus diagnostic verdicts remain in force.

## Context

The verified 30-minute 1,000 RPS diagnostic at revision `2cace0f` delivered all 1,800,000 scheduled requests with low API latency and zero overlapping hold intervals, but failed the fixed audit. It left 3,921 overdue active holds, 777 pending refresh rows and one unpublished outbox event. Refresh generation increased by 176,325 while completion increased by 144,652. A later read-only check showed eventual recovery, which does not change the failed audit.

One `maintenance` process currently performs a refresh batch first and then expires up to 100 holds in the same serial loop. Refresh and expiry therefore block each other even though they have separate durable claim mechanisms. Scaling identical combined workers previously improved drain but increased concurrent WAL commits and destabilized the API.

Each expiry currently commits one hold per transaction. At the measured 5% hold mix, 1,000 RPS creates about 50 expiring holds per second after the TTL, so per-item commits consume capacity needed by holds, outbox publication and refresh acknowledgements.

## Decision

Add dedicated `refresh` and `expiry` worker roles and corresponding Compose services. The refresh worker performs only the existing generation-fenced refresh batch. The expiry worker performs only expiry work. Retain the combined `maintenance` role as the default and rollback configuration.

Expire up to eight due orders in one PostgreSQL transaction. Claim order rows in expiry order with `FOR UPDATE OF o SKIP LOCKED LIMIT N`, then apply the existing hold, seat, order and outbox mutations for each claimed order inside that transaction. Keep lock ordering and ownership predicates unchanged. Expose `EXPIRY_BATCH_SIZE` from 1 through 100 with a default of 8; keep `expire_one` as a one-item compatibility wrapper.

Run one refresh service and one expiry service for the controlled candidate. Give each a maximum pool size of two and simulator concurrency two so their combined configured worker pool budget is four, below the previous two-combined-worker experiment and independent of API pools. Do not increase API admission, Kafka consumers or RDS parameters in this experiment.

Capacity deployment must build, force-recreate, count and source-hash verify both dedicated services. Rollback removes them and restores the original combined maintenance replica count.

## Alternatives considered

- Add another identical maintenance replica: rejected because prior multi-worker runs increased WAL contention and did not sustain the full stage.
- Increase the post-load drain window: rejected because it hides insufficient steady-state throughput.
- Increase hold TTL: rejected because it changes the reservation contract without increasing processing capacity.
- Make expiry asynchronous through Kafka: rejected because PostgreSQL is already the authoritative expiry queue and another delivery path would add ordering and recovery states.
- Use an unbounded or large expiry batch: rejected because longer transactions and larger lock sets could interfere with payment callbacks.
- Change API admission or add another retry: rejected because neither fixes expiry or refresh backlog.

## Consequences

Refresh and expiry cannot starve each other in one process. Expiry uses fewer commits, at the cost of holding up to eight order/hold/seat lock sets until one batch commits. A batch failure rolls back all rows in that batch. PostgreSQL `SKIP LOCKED` allows another expiry worker to claim different rows if later scaling is tested.

The candidate adds one worker process compared with the rollback topology but caps its configured database pools. Metrics expose `refresh_batch` and `expire_batch` separately through the existing worker operation counters and timings.

## Failure and recovery behavior

If an expiry batch transaction fails, PostgreSQL rolls back every mutation and outbox row in that batch; all orders remain eligible for a later claim. If a process stops after commit, the durable state and outbox events remain authoritative. If a refresh process stops, its existing lease expires and the generation remains replayable. Deployment or hash verification failure stops before load. Every test outcome restores the combined maintenance service and removes dedicated services.

Zero double-booking, exact acknowledged durability, zero broken links and zero queues remain mandatory. Later drain cannot retroactively pass a failed fixed audit.

## Validation evidence

Local validation completed on 2026-09-27:

- Full Ruff validation passed after reconstructing executable bits from shebangs inside the disposable Linux test container; this avoids Docker Desktop marking every copied Windows file executable.
- The full unit suite passed: 136 tests.
- The focused PostgreSQL/Redis reservation suite passed: 13 tests, including bounded multi-order expiry, two concurrent expiry batches claiming each order once, reclaimed-seat protection and rollback of every mutation and outbox row after an injected mid-batch failure.
- POSIX shell syntax validation passed for `scripts/huawei_capacity_backend.sh`.
- Base and RDS Compose rendering passed. The rendered split services use the intended roles, `DB_POOL_MAX=2`, `SIMULATOR_CONCURRENCY=2` and the pooled RDS URL.
- Deployment tests verify candidate selection, source-hash verification and restoration of the original maintenance, refresh and expiry replica counts.

Cloud validation starts at 1,000 RPS for five minutes with four APIs, admission five, two consumers, one refresh worker and one expiry worker. Promotion follows the strict and diagnostic rules in ADR 0056. A 30-minute result is required for any sustained diagnostic claim. Cloud validation has not yet run for this decision.
