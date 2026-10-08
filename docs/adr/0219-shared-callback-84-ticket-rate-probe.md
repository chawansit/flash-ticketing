# ADR0219: Shared callback 84 ticket rate probe

## Status
Accepted under ADR0172. Implemented and locally qualified; no cloud load or 84 tickets/s qualification yet.

## Context
ADR0217 restored the normal runtime after a passing matched placement comparison. Its 1+3 candidate delivered 18000 unique paid-and-issued tickets from 60 journeys/s offered for 300 seconds, with no customer failures or drops and primary CPU 60.88%. The next question is whether the same configuration can fulfill 25200 journeys offered at 84/s. The hourly goal requires at least 83.33 successful unique tickets/s for a full hour.

## Decision
Register one candidate-only shared_callback_rate_probe under the standing envelope. Reuse the proven protected paid runner and its existing 84/s fixture and financial-audit support. Keep frozen ADR0163 application images, gateway delay settings, shared callback routing, 1+3 placement, polling, workers and aggregate connection/admission budgets unchanged from the passing ADR0217 candidate. Only offered rate and corresponding isolated inventory grow. Identify the single measured arm as candidate, retaining the actual 1+3 identity in every collector and observer.

Offer 84 journeys/s for 300 seconds (25200 journeys), matching the baseline observation duration and covering intermittent stalls. One fresh safety qualification precedes one paid stage; two safety tickets/protocols total. Retain the 420-second customer completion deadline, mandatory post-TTL audits, all queue/Kafka drain and original runtime restoration. The complete experiment ceiling is 3600 seconds; it is a ceiling, not a target. No new resources or spending. Pin the baseline evidence and all exact source/configuration bindings before dispatch.

This supersedes ADR0217's pair and 60/s experiment restriction only for this separately registered probe. Consumed scopes and historical results remain unchanged. This is a sequential rate probe against an identified baseline, not a simultaneous matched comparison or hour-long qualification.

## Alternatives
Repeat the 60/s pair: it does not answer the higher-throughput question. Run an hour immediately: expensive before the short probe passes, and requires a larger inventory plus exact-hour issuance audit. Change simulator performance or add compute: confounds the measured placement/routing result. Build another runner: duplicates proven execution and recovery behavior.

## Consequences
No change to seat atomicity, persistence, payment idempotency or delivery guarantees. Four APIs retain 16 general pool connections with eight included payment slots and shared PgBouncer 24. Passing the short probe establishes bounded customer outcomes at offered 84/s; completion grace means it does not prove that every ticket was issued inside the 300-second offered window or qualify 300000/hour.

## Failure and recovery behavior
Reject naming, immutable source/image, inspected route/placement, budget, fixture or diagnostic drift before dispatch. Require existing customer latency/error gates unchanged, zero double-booking and payment loss, customer authorization, post-TTL durability, full queue/Kafka drain, credential removal and exact restoration. Stop progression on any failure; retain evidence and consume the identity without replay. Uncertain ownership or restoration blocks further load. No retries are added to hide errors.

## Validation evidence
Final focused profile and real diagnostic collector run: 12 tests passed in 76.28 seconds. The earlier broader run passed 238 checks and had two failures because the baseline hash was added while that process had already imported the old contract; those two real collector cases passed in the final stable run. Ruff and canonical naming checks passed. No cloud load started. Local tests exercise the actual candidate-only constructor, fresh scope accounting, 84/s fixture/audit expectations, real generated diagnostic collection for 1+3, callback coverage on all four APIs and drift rejection before cloud mutation. Baseline: [ADR0217 placement evidence](../capacity/flash-sale-opening/shared-callback-placement-comparison-2026-10-08.json).
