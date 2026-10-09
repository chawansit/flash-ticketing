# ADR0232: Bounded hourly paid ticket qualification

Status: Accepted for local implementation; cloud hourly load requires a fully passing short control and a separately registered, locally qualified hourly runner.

## Context

The user approved hourly qualification after an unchanged short control, with a 90-minute experiment ceiling and the existing CCE comparison/scaling spending allowance. The proven coordinator and CCE stage are deliberately limited to 300 seconds and 25,200 tickets. Replaying old scopes or relaxing their gates would be invalid. A one-hour test at 84 journeys/s needs 302,400 unique seats, fresh sale windows and tokens, and observations covering the full hour.

## Decision

Register a separate exactly bound cce_hourly_qualification profile: one paid stage, 84/s for 3600 seconds, at most 5400 experiment seconds, four 1-vCPU/1-GiB API pods, the 1-vCPU bridge, unchanged backend images, background services, database connection budgets, polling, payment simulator and client concurrency. Require the identified fully passing short control before admission. Preserve all existing customer latency/error, financial, TTL, queue, authorization and restoration gates.

Prepare 1008 isolated 300-seat shows. Use twelve successive five-minute groups of 84 shows so the offered workload retains the short control's active-show count and per-seat allocation. A new bounded hourly coordinator delegates unchanged frozen leaf scheduling, HTTP requests, metrics and aggregation; a small allocation adapter selects the correct five-minute group. Keep two shards, each 250 active journeys and eight HTTP clients. The leaf generator already supports 3600 seconds. Do not modify its customer transaction algorithm or add retries. Bind and verify all adapter sources.

Count unique tickets with durable successful payments and orders fulfilled, whose database issued_at is within the exact common-start-to-start-plus-3600s interval. Require at least 300,000 in that interval, plus the complete 302,400-journey terminal audit. Tail completions and refunds cannot count toward the hourly sales target. Require the conservative inner window (two seconds excluded at each boundary) also to contain at least 300,000 tickets. Require database/API clock skew within two seconds and shard start timestamps consistent with the common start. Record timing uncertainty and all observation gaps.

Use fresh canonical ownership directories and source-bound reservations. Keep the original short profile fixed at 300 seconds and 3600 experiment seconds. Hourly-only code paths have explicit profile/duration/fixture requirements; no old scope is reopened. The main merge still requires approval.

## Alternatives

Multiply the short-stage rate by 3600, repeat twelve disconnected stages, or use one large seat projection. Extrapolation does not qualify an hour; disconnected stages introduce gaps and restart effects; large projections change the unresolved high-seat-count workload. Continuous scheduling over 300-seat shows retains the measured workload geometry. This qualifies distributed sales throughput, not single-concert hotspot or simultaneous-opening performance.

## Consequences

The fixture contains twelve times as many shows, so catalog/reconciliation costs are part of this qualification. Each five-minute block has disjoint actors and seats; idempotency keys retain block identity. Extra adapters are test orchestration, not application architecture. Observers, credentials and sale windows must cover setup, the full offered hour and mandatory recovery. Report actual cloud duration; monetary billing is not measured automatically.

## Failure and recovery

A failed short control blocks hourly admission. Stop increasing load on hourly failure, preserve partial generator and observer evidence, and always stop owned jobs, audit payment/ticket/refund relationships, wait past hold TTL, drain queues, retire exact owned fixtures, remove the owned namespace/helpers and restore the original ECS deployment. The 90-minute limit is a ceiling; required owned recovery continues if it is reached. A successful short or hourly measurement never overrides a failed restoration gate.

## Validation evidence

The prior measured stage and recovery are in docs/capacity/flash-sale-opening/cce-bridge-comparison-2026-10-09.json. ADR0234 now supplies a fully passing short comparison. Its 20-slot acquisition setting must be retained for the hourly comparison; hourly integration/registration is still incomplete. Hourly adapters and registry must pass offline allocation, uniqueness, timing, source-drift, financial-window and bounds tests before any hourly cloud mutation. At initial acceptance, no hour-long test had been executed. See the executed hourly result below; full qualification remains failed.

## Integration bounds

Retain ADR0234's passing 20-slot CCE acquisition setting; ECS restoration retains 12. The hourly profile alone permits 3600 offered seconds, 1008 shows, 302400 actors, 7200-second sale/token windows and a 5400-second experiment ceiling. Short profiles remain unchanged. Bind the hourly coordinator, allocation adapter, issuance audit and profile source to the fresh reservation and verify transferred helpers before dispatch.

The hourly audit helper and API pod lifetime must cover setup, offered traffic and mandatory verification within the approved ceiling. Predeclare a 240-second fixture preparation bound. Sample cohort-wide counts at most once per ten seconds during hourly observation while continuing API, worker, connection/wait and global queue observations each second; terminal financial and queue audits always query fresh state. This bounds measurement overhead without relaxing customer or correctness gates. Report the cohort sampling interval explicitly.

A new reservation is admitted only after local qualification and a full passing short baseline; consumed scopes and their original evidence remain immutable. The following local qualification preceded cloud execution; the executed hourly outcome is recorded below.

Local integration validation executed: 800 tests passed, one platform skip in 23.88 seconds; lint and repository naming checks passed. Exact source bindings and hourly plan are in docs/capacity/cce/hourly-runner-qualification-2026-10-09.json. A fresh hourly goal begins at ledger index 52; prior consumed goals are retained. These local tests preceded the fresh hourly run recorded below.

## Executed pre-dispatch finding

Run adr0151-b166e473c60c stopped before hourly traffic and finished FAILED_RESTORED after 722.062 seconds. Both safety protocols passed; the 1008-show fixture was 170 seconds old after preparing and retrieving its 302400-token manifest, exceeding the inherited 120-second fixture check. No paid stage started; ownership, durability, queues and restoration passed.

For the exactly registered hourly cohort only, accept a creation receipt at most 600 seconds old and require at least 4200 seconds of remaining sale time when admitting the paid stage. Keep the short fixture's 120-second freshness limit unchanged. Preparing the same fresh owned receipt and validating its remaining window is safe; reuse of prior run fixtures remains forbidden.

Bound hourly private transfers to 128 MiB and use 16 concurrent SFTP prefetch requests for reads and acknowledged pipelined writes. Await close and retain existing read-back fingerprints and ownership checks. Observer manifests contain only their needed environment and show IDs; customer tokens remain in the private generator manifest. These transport changes reduce measured setup overhead and do not change backend requests, retries, connection budgets or measurement gates. Failed or ambiguous transfer still triggers owned cleanup.

## Executed hourly result and recovery

Run adr0151-0f735e978cba offered 84 journeys/s for 3600 seconds from 08:49:33 to 09:49:33 Bangkok on 9 October 2026. All 302400 journeys dispatched, with zero generator drops and no customer retries. The committed database issuance window contains 302262 unique paid-and-issued tickets, including 302037 inside the conservative two-second inner guard. The numerical 300000/hour target was exceeded.

The strict customer gate failed: 302399 journeys succeeded and one payment request returned HTTP 503 (0.000330688% of dispatched journeys). Recovered API counters classify that request and three callback 503s as PoolTimeout. Independent ADR0235 recovery verified all 302400 orders: 302399 durable successful payments/bookings/tickets and one expired unpaid order, zero duplicate bookings, valid financial relationships, elapsed TTL, all queues zero and exact normal topology restored. The original failed report remains immutable and the consumed scope is FAILED_RESTORED.

The 264435078-byte observer trace exceeded its 128-MiB transfer bound. Read-only late collection recovered the exact file, but 77 sample gaps above two seconds (maximum 3.022377 seconds) prevent strict database continuity/native CPU qualification. This is measured distributed hourly throughput, not a passing production qualification, maximum capacity estimate or simultaneous flash-opening qualification.

Sanitized evidence: docs/capacity/flash-sale-opening/cce-hourly-qualification-2026-10-09.json. Final local validation executed: 826 tests passed, one platform skip in 24.76 seconds; lint and repository naming checks passed. No further paid stage is authorized under this consumed hourly scope.
