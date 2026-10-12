# ADR0294: Bootstrap hourly observers before native expiry qualification

## Status

Accepted for a fresh ADR0293 hourly attempt under standing Continue authorization. Supersedes ADR0293's inherited observer-after-safety startup order for hourly profiles only. ADR0292 short fixture order remains unchanged.

## Context

ADR0293 run adr0151-13916e654f05 passed native safety but stopped before paid dispatch: the inventory captured at 16:49:11 UTC was older than the unchanged 300-second observer admission limit by startup. Worker readiness, ingress readiness, native TTL qualification and observer preparation consumed that window. The failed scope restored with payment integrity and credentials cleared; no paid stage began. See [retained abort](../capacity/cce/existing-ingress-hourly-prepaid-abort-2026-10-12.json).

## Decision

For hourly profiles only, prepare the already-created paid fixture inputs and start/qualify existing diagnostic collectors after native worker and ingress readiness, before native payment/replay/expiry safety. Dispatch remains prohibited until every safety and readiness gate passes. Do not alter capture timestamps, validation timestamps, the 300-second freshness limit or short profile behavior.

Extend only the early hourly collectors' bounded lifetime from 3,780 to 4,200 seconds so setup and the complete paid window remain covered. Record this opt-in order and duration in the source-bound verification contract. Reject dispatch if the scheduled paid-window end plus two seconds exceeds the earliest collector's remaining lifetime. The paid stage remains 3,600 seconds at 84 offered journeys/s; customer deadlines, 5,400-second experiment ceiling, resource sizes and database connection budgets remain unchanged. Stop existing collectors after paid-window collection using the proven ownership-bound cleanup.

Preserve native API/worker receipt checks, immutable images, current host identities, existing diagnostic credentials and all freshness checks. Statistics continue every second on one dedicated existing connection, with complete offered-window coverage and maximum two-second gaps. Collector activity during safety is preparation evidence, excluded from offered-window customer/CPU measurement.

## Alternatives

- Increase the inventory freshness limit or rewrite captured_at: rejected.
- Reinterpret pre-transition API metadata as a new post-transition observation: rejected.
- Start paid traffic before TTL/payment safety: rejected.
- Rebuild the observer framework: unnecessary for this measured startup-order conflict.

## Consequences

No application capacity improvement is assumed. Collectors operate earlier during preparation with the same single diagnostic connection. Current worker startup can still fail before collection if readiness itself exhausts inventory freshness; retain that failure. Longer collector bounds do not relax hourly coverage, financial or customer gates.

## Failure and recovery behavior

Stop before paid dispatch on stale inventory, failed observer startup, failed safety, unknown ownership or insufficient remaining collector coverage. Preserve the original failed record. Perform owned job stop, post-TTL financial/global queue checks, route/namespace deletion and exact runtime restoration. Never replay a spent scope or proceed to another paid stage on a failed run.

## Validation evidence

Before implementation: executed ADR0293 failure, unchanged freshness check and lifecycle order reviewed. Executed correction validation: 298 focused tests passed in 150.33 seconds, including safety rejection, cleanup, inventory freshness, exact remaining collector coverage, independent sampler bounds, SQL ownership and original evidence-gap checks. Ruff and whitespace checks passed. Tests use an in-memory refreshed harness identity for historical completed authorization fixtures; live authority and failed evidence are unchanged. The fresh cloud attempt is pending. No paid load or capacity improvement was measured by the aborted attempt.

The first corrected wrapper passed cloud readiness but was rejected before experiment registration: its hourly bootstrap marker was retained in the internal structural short-profile view. No deployment or paid stage started. The view now omits that hourly-only marker while the real hourly authorization validates it before translation. The complete composed hourly plan was then executed locally without writes and passed. A regression test covers plan composition and rejection of an unknown bootstrap decision.

After the composition correction, 299 focused tests passed in 146.31 seconds; Ruff and whitespace checks passed. The fresh paid hourly attempt remains pending.
