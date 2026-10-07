# ADR0191: Pinned fixture format and zero-dispatch abort

## Status
Accepted under ADR0172; implemented and locally tested, exact zero-dispatch abort freshly verified. New control retry pending. No change to application financial authority or future customer gates.

## Context
Fresh slow_database_control qualification adr0151-0013af72d51b passed 100-way concurrency, replay, authorization, one paid-issued ticket, duplicate callbacks, post-TTL durability, complete queue drain and restoration. Paid load never started. The stage fixture receipt rejected the pinned producer output because the historical producer does not emit fixture_layout; it supports distributed fixtures only. Earlier unit tests used a newer synthetic output shape and did not exercise this pinned producer contract. The finalizer correctly kept the scope blocked rather than presume recovery.

## Decision
Normalize the missing distributed layout only for the exact source-verified historical producer SHA256 99d09157747843478768a5d8be7d3cb326cbd2d4d4387e8873c501fca4fcc13c. Pass the producer fingerprint from the already bound immutable helper bundle. Unknown producers or incompatible layouts remain rejected; identity count, UUIDs, sale window, development-only checks and exclusive durable receipt writes remain strict. Record the producer hash in the receipt. Exercise the actual pinned producer locally before another cloud attempt.

Extend the existing append-only verified-abort mechanism only for bounded_slow_database_diagnostics__7c5c5950fffd and its exact immutable entry/binding. Require its single retained failed qualification report, zero capacity stages and zero paid-run reservations, the exact fixture format failure before token minting, complete retained safety/financial/authorization/retirement/credential/restoration evidence and a fresh unchanged-runtime, idle-generator, absent-secondary, absent-owned-credentials and all-queues-zero observation. Do not change its failed result or reopen the consumed scope. This is a verified zero-dispatch abort, unlike ADR0190's accepted historical paid uncertainty.

This exact slow control never staged administrator diagnostic credentials. Its verifier requires that staging flags are absent, common credential snapshots were removed, private cleanup passed and fresh owned credential paths are absent; it does not invent an administrator-cleanup flag. Original ADR0182 administrator cleanup remains mandatory for its diagnostic case. Retained abort artifact hashes are rechecked on subsequent admission.

A future attempt uses a new registered reservation and repeats all gates. A failed control stops progression. ADR0182's original case remains unchanged; only this separately pinned case uses ADR0191 verification.

## Alternatives
Relax the validator for any missing layout: rejected. Replace the frozen fixture producer or application images: changes the identified baseline unnecessarily. Ignore the new recovery flag: rejected. Repeat the failed scope: rejected. Broad automatic recovery for arbitrary failures: rejected.

## Consequences
The retained fixture format now matches its verified producer. Original workload, schema, budgets, machine sizes, financial transactions and capacity gates remain unchanged. This correction proves orchestration compatibility, not a capacity improvement.

## Failure and recovery behavior
Unknown producer, malformed fixture, missing or changed retained evidence, nonzero paid dispatch/reservation, different failure, safety/financial/cleanup failure, stale observations, active runs, pauses, replay or tampering remain blocking. Partial receipt writes preserve ownership and stop progression. The previous historical paid exception cannot cover this or any future failure.

## Validation evidence
Qualification ran for 305.438 seconds; paid capacity stages zero. Safety accepted exactly one of 100 concurrent requests; one successful payment, booking and ticket persisted after TTL; three callback deliveries, zero duplicates, all queues drained and normal runtime restored. Original failure retained under tmp/adr0151-0013af72d51b. Executed 242 affected unit/integration tests in 13.33 seconds, including the actual pinned producer against isolated local PostgreSQL and Redis; 93 final abort/envelope checks passed in 5.76 seconds after the exact no-administrator-staging lifecycle correction. Ruff passed. Earlier local tests and the initial read-only abort helper failure are retained; no paid load occurred.

Fresh read-only restoration completed in 22.391 seconds and passed all eleven abort recovery gates. The [verified receipt](../capacity/flash-sale-opening/fixture-format-abort-recovery-2026-10-06.json) clears only the original restoration block; its RECOVERY_REQUIRED entry, failure and counters remain unchanged. Fresh registered admission is available. The recovery receipt itself contains no capacity measurement; the subsequent fresh control is recorded below.

Fresh registered control completed on 2026-10-06: qualification adr0151-98d66aca4c67 and measured run adr0151-c8bb0a476b4c passed. All 18,000 scheduled journeys completed with 18,000 durable successful payments, bookings and tickets, zero customer errors/drops/double-booking, post-TTL and global/Kafka drain passed, and original services restored. Internal callback errors recovered; they are not claimed absent. Exact fresh fixture identity was retained. The combined protocols took 1,358.375 seconds. See [fresh control evidence](../capacity/flash-sale-opening/fresh-slow-database-control-2026-10-06.json). Historical failed entries remain unchanged. ADR0189 benefit, 84 tickets/s and hourly qualification remain unmeasured.
