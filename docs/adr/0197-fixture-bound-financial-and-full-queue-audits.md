# ADR0197: Fixture-bound financial and full queue audits

## Status
Accepted for local implementation under ADR0172, ADR0184, ADR0185 and ADR0186. Worker separation remains unregistered. No production persistence or locking decision is superseded.

## Context
Worker placement execution has guarded restoration primitives but lacks a concrete financial audit adapter. Aggregate payment, booking and ticket counts alone cannot establish matching order, customer, event and seat relationships or consistent order totals and currencies. A missing fixture must not silently become an empty audit or an inferred cohort.

## Decision
Retain exact ADR0185 fixture identity and predetermined order/payment expectations before dispatch, bound to the original execution scope and arm. Read financial counts and relationships in one read-only repeatable-read PostgreSQL snapshot after all cohort hold expirations have passed according to the database clock. Independently audit duplicate bookings across the database. Never lower expectations to observed rows or infer ownership from database contents.

Reuse the existing complete queue audit, including reservation streams, pending entries, refresh, outbox, refunds, callback delivery, confirmation receipts/capacity and Kafka lag. Require the exact running consumer membership for the observed original, full or owned partial arm layout. During the consumer-free handover, require a stable Empty group with no members and compare committed and end offsets for every topic partition before and after observation; missing offsets never become zero lag. This supplements the active-only observer without changing its accepted behavior. Bound polling with a shared deadline; retries are read-only observations and do not hide customer errors. Stop mutation when any queue is unresolved.

Execute audit programs only inside an exactly observed primary API container, using private standard input and immutable runtime identity rechecks. Preserve scope binding on cleanup even after pause or expiry. Attempt mandatory financial, duplicate and queue checks independently. Retain recovery files when checks or journal persistence fail. These adapters do not authorize cloud dispatch or register a profile.

## Alternatives
Aggregate-only checks miss relational corruption. Database-derived expectations can certify lost payments. Redis-only queue checks miss durable/Kafka work. Deleting unknown historical cohorts or replaying ambiguous runs loses ownership evidence. A new privileged diagnostic account is unnecessary for these application-visible checks.

## Consequences
Read-only audit snapshots and global scans consume existing database capacity after dispatch stops. Bounded observation may fail rather than certify an unresolved state. Exact counts assume the existing one-ticket-per-journey workload; a multi-seat profile needs a separately validated expectation contract.

## Failure and recovery behavior
Missing, altered, unbound or late fixture evidence fails closed. Unknown transport outcomes are not replayed for financial actions; mandatory independent checks and exact restoration remain available. Timeout, incorrect relationships, duplicates, nonzero queues, unexpected Kafka membership and journal failures preserve evidence and block further load.

## Validation evidence
488 affected tests passed in 147.07 seconds with zero failures and zero skips. The new coverage includes 57 focused cases and 23 PostgreSQL integration cases. Six generated transport cases ran on real Linux, with simulated Docker observations and real local database queries. Tests cover equal-count relational corruption, totals/currencies, a concurrent commit between audit queries, actual hold-expiry waiting, transport loss/drift, a Linux child deadline, all queue categories, dormant partition offsets and guarded start/stop/restoration integration. Local test containers were removed; Ruff, repository naming and whitespace checks passed.

Early failures were confined to synthetic scope seals, duplicate test module names and the native test harness/network. Their evidence is retained; the corrected final suite passed. Queue audit client behavior is not a live Redis/Kafka qualification. Complete observer/paid-stage lifecycle assembly and profile registration remain pending. No cloud readiness, deployment, customer dispatch, capacity improvement or complete worker-runner qualification is claimed. See [audit evidence](../capacity/flash-sale-opening/background-service-separation-audits-2026-10-07.json).
