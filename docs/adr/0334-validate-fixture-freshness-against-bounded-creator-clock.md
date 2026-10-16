# ADR0334: Validate fixture freshness with a bounded creator-clock reference

## Status
Implemented for test admission; local regressions and real read-only transport verified; paid outcome pending.

## Context
The ADR0333 CCE attempt failed before native safety or paid journeys because a newly generated fixture failed the strict freshness guard. Runtime and queues were restored, with zero paid stages consumed. Independent clock probes show the primary ECS clock ahead of the controller by at least0.1438seconds (bounded interval0.1438..0.6342seconds), and the generator ahead by0.1442..0.3201seconds. Fixture creation occurs in an audit container on the primary ECS; a fast response can therefore look future-dated to the controller. No raw fixture receipt was retained before timestamp validation, leaving a diagnostic gap.

## Decision
Keep the existing fixture identity/freshness validator and its0..120second short-window limits unchanged. Persist the producer's complete raw fixture receipt locally before validation. For an explicitly source-bound ADR0334 comparison, sample UTC inside the same trusted audit container immediately after creation. Bound the creator/controller clock-offset interval to +/-2seconds and probe round-trip to5seconds; reject larger uncertainty. Use creator UTC plus the full probe round-trip and subsequent controller monotonic elapsed time as a conservative validation reference. Reuse that reference at subsequent fixture-retention validation in the same controller process and same fixture ID. Reject malformed, mismatched, replayed or unavailable evidence. Retain old local-clock behavior for all unselected historical profiles.

Only fixture admission and receipt retention change. Do not change application clocks, OS time, holds/seat TTL, payment idempotency, sale windows, client recovery, latency gates, timestamp-window guards or database budgets. The pending one-stage160/s comparison remains the only paid allowance; the prior attempt consumed zero paid stages.

## Alternatives
Changing system clocks risks application leases and unrelated work. Simply allowing future fixture timestamps weakens a global guard without attributing the discrepancy. Repeating unchanged setup cannot eliminate a measured clock offset. Moving generators to CCE would not fix controller-versus-producer timestamp comparison.

## Consequences
Two small clock probes (one per safety/paid fixture) add negligible setup work and no paid database connections. Validation may conservatively reject borderline-old receipts; it must never extend their sale window or make an old fixture fresh. Evidence is bound to the current process, fixture and verified container. No production persistence, locking, messaging, idempotency, TTL or scaling decision is superseded.

## Failure and recovery behavior
Record the raw receipt and clock evidence before rejecting admission. Never recreate an ambiguously created fixture or infer its identity from unrelated database rows. Do not dispatch buyers if creator-clock proof is missing or outside its bounds. Preserve failed results, complete ownership-based cleanup and financial/drain/restoration checks.

## Validation evidence
Measured clock intervals are recorded above; the failed ADR0333 raw result remains unchanged. Planned tests cover small offset admission, oversized clock skew, stale receipts, excessive RTT, mismatched fixture/process IDs, monotonic replay, and preservation of old behavior and paid gates. Append executed checks and paid outcome only after completion.

### Executed local verification

327 focused regressions passed, including the bounded clock proof, stale/closed fixture rejection, monotonic/process/fixture replay rejection, raw-receipt retention before failure, four-process accounting, historical profiles and lifecycle restoration. The535-file reproduction check, full160-show audit compilation and complete reservation admission in isolated state passed. No application source, image, customer TTL, payment policy, latency/error gate or database budget changed. The prior failed attempt remains FAILED_RESTORED with zero paid stages consumed; the pending retry retains the single paid-stage allowance.

### Adapter correction and real transport validation

A second pre-dispatch attempt (run `adr0151-d56170529f15`) correctly retained its raw fixture receipt but failed because the new clock call supplied a10-second outer timeout to `Session.api`, whose subprocess budget subtracts15seconds. No native safety or paid journeys started; all resources and runtime were restored (FAILED_RESTORED, zero paid stages). Preserve that failed result separately.

Corrected the outer timeout to30seconds (15seconds inside the existing adapter), keeping the measured clock-probe RTT limit at5seconds. A new regression exercises the actual adapter's generated command and rejects nonpositive inner budgets.107 focused follow-up checks passed; the prior broader327-check result is retained with its original source identity (these test sets overlap).

The real read-only SSH/container clock probe passed in0.282seconds without creating any fixture or payment. Its measured creator/controller offset interval was0.1191..0.4034seconds, inside the unchanged +/-2second admission bound. See [the transport receipt](../capacity/cce/fixture-clock-transport-2026-10-17.json). Reproduction, source/configuration binding and complete isolated reservation admission were rechecked before retry.
