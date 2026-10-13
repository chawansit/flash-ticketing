# ADR0314: Rebalance general slots at the measured headroom failure

Status: Accepted for bounded diagnostic comparison; capacity qualification failed.

## Context
The initial102/s trial failed.16943order-status503responses dominated622payment503responses; general waiters reached17per replica, mean pool acquire80.44ms, and API CPU averaged2.007of4cores. Physical PostgreSQL WALWrite waits also peaked23, so the complete root cause remains unconfirmed. All27941paid orders/tickets and76unpaid expirations were independently reconciled under ADR0313. The raw failed benchmark is immutable.

## Decision
Test exactly one configuration correction at the same102offered/s300s: change API_PAYMENT_POOL_MAX from3to2, giving each API2general+2payment slots instead of1general+3payment. Keep4slots per API,4API pods,14background pods,24physical database connections, admission20, generator concurrency500, image, client recovery, gateway delays and quality/financial thresholds unchanged.

Authenticate the passed84/s recovery reference and the exact failed/restored102/s receipt plus independent financial closure before the new experiment. Require30000unique paid-issued tickets inside the window, conservative target, zero drops, current latency gates,0.01%final customer failures and zero financial violations. Failure stops escalation. This is a targeted corrective verification, not a passing-control promotion.

For this diagnostic only, supersede ADR0306's adopted1+3allocation. Retain its healthy84/s hourly evidence and normal runtime configuration until the correction passes. No untested default changes or resource growth.

## Alternatives
Add API CPU: current average is below the four allocated cores. Increase total database connections: changes the budget and may worsen WAL contention. Increase retries: amplifies work without fixing slot availability. Change cache/query/transaction patterns simultaneously: loses attribution.

## Consequences
General reads get more parallel capacity; payment requests lose one local slot per API. Payment/callback latency and stalls must be measured explicitly. A pass does not prove WAL issues eliminated, hourly100/s capacity or flash-opening qualification.

## Failure and recovery
Preserve failed evidence, reconcile every actual payment and expired unpaid order, drain both event lanes and restore exact runtime/credentials. Rollback selects the unchanged1+3profile. Seat TTL, idempotency, persistence and messaging behavior remain unchanged.

## Validation evidence
Pending exact2+2semantics/budget tests and one bounded cloud repeat. No corrective paid traffic has started.

Published comparison: [measured outcomes and independent recovery](../capacity/cce/headroom-slot-writer-comparison-2026-10-13.json). Failed raw results remain unchanged; no four-writer hourly or flash-opening qualification is claimed.
