# ADR 0033: Re-evaluate hold admission after owner-index optimization

Status: Proposed; bounded experiment authorized, default admission remains eight

## Context

ADR 0020 retained hold admission eight after admission sixteen reduced overload
responses in concentrated contention but made failed-response latency worse. ADR
0029 later confirmed that one API worker with a pool of twelve passed 400 RPS
for 30 minutes, while 500 RPS produced 29 admission rejections before the active
seat-owner index existed.

ADR 0031 removed two full `event_seats` scans from every expiry. With that index
and the keep-alive margin from ADR 0032, the matched 400 RPS 30-minute run passed
720,000 requests with zero unexpected responses, transport errors or drops. At
500 RPS, 899,997 of 900,000 requests completed with expected success statuses
and three holds returned `ADMISSION_FULL` in the same millisecond at occupancy
eight. PostgreSQL averaged 33.63% of one core, sampled active connections peaked
at four, sampled lock waiters stayed at zero and all 44,997 accepted holds
passed durability and overlap checks. The prior admission comparison therefore
does not represent the current database cost.

## Decision

Run a bounded admission-twelve candidate with one API worker, DB pool maximum
twelve, PgBouncer and all other topology unchanged. Compare it at 500 RPS for
30 minutes against the admission-eight run from the same commit, fixture class,
workload mix, client/server keep-alive settings and separate generator.

Admission twelve matches the application pool ceiling and does not increase the
configured database connection budget. Retain no-retry transport diagnostics,
CPU/database observers and post-expiry durability, overlap and queue checks.
Adopt the new default only if 500 RPS has zero unexpected responses, transport
errors and drops, latency remains within documented targets, durable ownership
is exact, queues drain and pool/transaction evidence does not show unsafe
saturation. Do not proceed to 600 RPS if the candidate fails.

This changes only an opt-in Compose experiment until evidence is reviewed.
PostgreSQL remains the ownership authority and Redis remains an atomic admission
shield; transaction, idempotency, TTL and Kafka semantics are unchanged.

## Alternatives considered

- Keep admission eight. It bounds pressure most aggressively, but the current
  matched 500 RPS evidence shows three unnecessary immediate rejections while
  sampled PostgreSQL activity remains below its budget.
- Re-test admission sixteen. ADR 0020 found worse failed-response latency, and
  sixteen exceeds the pool ceiling, so it is not the next bounded step.
- Add an automatic retry for `ADMISSION_FULL`. That would hide offered-load
  failure and can amplify a short saturation wave.
- Add API workers first. That changes scheduling and per-process admission
  behavior before the single-worker budget is understood.

## Consequences

The candidate can allow four more concurrent hold handlers to wait for or use a
database connection. This may remove brief false overload responses, but it can
also increase pool waits, transaction latency and CPU bursts. Read traffic is
not admitted through this semaphore.

If accepted, aggregate admission must remain bounded when API instances are
added; twelve is a per-process setting and cannot be multiplied without a new
scaling decision.

## Failure and recovery behavior

Invalid application limits continue to fail configuration validation. If API
readiness fails after applying the override, restore admission eight. If any
500 RPS gate fails, retain the evidence, restore admission eight and stop rate
escalation. A lost SSH session does not stop the background generator or
collectors.

After the run, wait beyond the hold TTL and verify every HTTP 201 against its
idempotency record, hold and order. Verify zero overlapping seat intervals,
zero active or overdue holds and drained outbox, refresh and dead-letter queues
before restoring or advancing configuration.

## Validation evidence

Pending. The admission-eight control at commit `1a68518` attempted 900,000
requests with zero transport errors or drops. Three holds returned
`ADMISSION_FULL`; read p95 was 17.914 ms and hold p95 was 54.937 ms. All
44,997 accepted holds passed post-expiry durability and overlap checks, and all
observed queues drained.
