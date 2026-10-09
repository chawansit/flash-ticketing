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

The prior measured stage and recovery are in docs/capacity/flash-sale-opening/cce-bridge-comparison-2026-10-09.json. ADR0234 now supplies a fully passing short comparison. Its 20-slot acquisition setting must be retained for the hourly comparison; hourly integration/registration is still incomplete. Hourly adapters and registry must pass offline allocation, uniqueness, timing, source-drift, financial-window and bounds tests before any hourly cloud mutation. No hour-long test or hourly capacity is currently qualified.
