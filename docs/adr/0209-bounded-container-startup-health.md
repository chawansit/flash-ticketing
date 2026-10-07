# ADR0209: Bounded container startup health

## Status
Accepted for implementation. Supersedes ADR0195 immediate return after detached Compose application. All dependency, customer, correctness and restoration gates remain in force.

## Context
ADR0208 captured the fresh pre-dispatch failure at Kafka metadata discovery: NoBrokersAvailable. Direct RDS TLS, the pooler and Redis passed. The broker is recreated when its private listener is configured. Current Compose application returns once containers start, while the preserved Kafka healthcheck checks readiness every ten seconds. Independent recovery passed after automatic restoration, whereas its immediate database/queue audits failed. This is evidence of a startup sequencing gap; a startup delay is a hypothesis, not proof that private Kafka connectivity is correct.

## Decision
Use Compose's existing health wait for each owned application and restoration: --wait --wait-timeout 90, within the existing 110-second subprocess and 135-second remote action ceilings. This executes one application, not a replay or customer retry. Healthchecked services must be healthy; other services must be running. Keep all existing healthchecks and exact application settings. Then perform the existing authenticated private dependency probes on both hosts before workers or customers start. Broker health alone cannot replace private advertised-listener validation.

Qualify actual Compose health success and bounded unhealthy failure locally, and test generated action failure/cleanup paths before another fresh registered experiment.

## Alternatives
Increasing Kafka client request timeouts would conceal deployment sequencing and alter probe timing. Retrying whole experiments wastes time and loses comparability. A fixed sleep may delay a healthy deployment and still miss slow startup. Waiting for the already configured healthcheck answers startup readiness within the existing bound.

## Consequences
Application and restoration may take longer to acknowledge readiness, but remain inside existing deadlines and costs. This does not change booking transactions, payment processing, connection budgets or measured capacity. Persistent private connectivity or unhealthy services still fail closed.

## Failure and recovery behavior
A health timeout or unhealthy service fails the owned application; never proceed to customers or increase load. Observe and remove only owned resources, attempt exact original restoration, and retain files and ownership until mandatory independent audits pass. No scope refund or replay. Preserve original failed results.

## Validation evidence
ADR0208 failed run adr0153-parents-8113d716d1af retained sanitized Kafka failure in tmp/adr0153-parents-48875bc9806a/0042-dependency-probe-failure.json. Independent recovery tmp/adr0153-parents-24f5989ba8ec/recovery-verification.json passed all required checks and closed the scope FAILED_RESTORED with zero paid dispatch. Implemented. Final affected execution/readiness/runner/profile/recovery and Linux-generated/native suite executed: 278 passed, zero failures/skips, in 213.02s. Includes actual Compose healthy success and unhealthy failure, exact owned probe cleanup and frozen-image imports. Ruff, repository naming and diff checks passed. Fresh run adr0153-parents-6e300e94f5da passed every private dependency check on both hosts and started workers. It stopped at inventory before customer dispatch. Automatic exact restoration, zero-double-booking, full queues, generator idle and configuration cleanup all passed. No capacity improvement claim.
