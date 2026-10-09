# ADR0226: Reap completed generator tasks before admission

Status: Proposed; implemented and locally qualified; cloud performance comparison failed

## Context

The ADR0225 84 offered journeys/s, 300-second comparison fulfilled every one of 25,196 dispatched journeys, but dropped four of 25,200 scheduled journeys at one generator shard's configured 250-journey cap. Three snapshots counted completed tasks whose cleanup callback had not yet run; one showed 250 unfinished tasks. Generator CPU did not establish a hardware ceiling. The callback/ticket tail remains a separate concern. The mutable local generator includes unrelated fixture-layout validation absent from the frozen reference, so copying it wholesale would invalidate a one-factor comparison.

## Decision

Before the existing concurrency admission check, reap completed tasks from the active set using the same completion cleanup function. Make cleanup idempotent so a delayed callback cannot remove a later task or change occupancy twice. Keep open-loop scheduling, true active cap, connection budgets, outcome accounting, polling, retries, fixture, backend images/settings and all gates unchanged. Apply only these two edits to the pinned frozen generator in the existing 78-file harness, with exact parent/candidate hashes and normalized transferred-source read-back. Register a fresh generator_completion_probe with one safety qualification and one unchanged 84/s, 300-second paid stage against the retained ADR0225 reference. No image build or new infrastructure is needed.

This extends ADR0090 bounded generator controls; it does not supersede its admission limit. ADR0225 writer pipeline and transaction authority remain unchanged. A harness correction cannot be described as increased backend capacity.

## Alternatives

- Increase the 250-journey cap: changes the workload budget and can conceal the bookkeeping defect; defer.
- Await a slot or retry dropped journeys: changes open-loop offered traffic and masks drops; reject for this comparison.
- Add a scheduling sleep to allow callbacks to run: perturbs arrival timing without ensuring cleanup; reject.
- Optimize the callback path first: may reduce true occupancy but leaves the measured stale-membership defect; assess after this isolated correction.

## Consequences

Admission counts unfinished work rather than delayed cleanup membership. Iterating the bounded active set adds local generator work; measure its CPU and dispatch lag. A true cap breach still drops and fails the existing gate. Three stale snapshots do not prove all four drops will disappear. Changes must enter the actual transferred frozen harness; editing the local script alone is insufficient.

## Failure and recovery behavior

Unknown generator parent hash, patch drift, altered backend factor, index/source mismatch, failed safety or unresolved recovery blocks dispatch. Use fresh scopes, preserve failed evidence and run post-TTL durability, zero-double-booking, complete queue drain and exact restoration. Roll back by using the original frozen generator; no persistent schema change is introduced. Do not reopen consumed scopes or weaken financial counts to convert a failed run to a pass.

## Validation evidence

Planned: a deterministic delayed-cleanup regression that reproduces stale drops on the parent and allows all offered journeys on the corrected generator, true saturation still drops, idempotent cleanup, unchanged outcome/HTTP budgets, exact one-file frozen diff and full deployed-generator binding. Execute focused generator, profile and recovery tests before activation. Run the same 300-second window because the observed drops occurred near 282-283 seconds; a shorter early sample would miss the defect. Current profile error/latency gates and all mandatory audits remain unchanged. The planned checks were subsequently executed as recorded below.

Implemented the exact two-edit frozen patch and corresponding local behavior. Executed 20 generator tests in 9.73 s, including delayed-callback parent/candidate reproduction and preserved true saturation. Executed 108 profile/envelope/recovery checks in 289.26 s, including the actual constructor, allocation selector, source-bound protocol seam, fixed backend images/settings and rejected factor drift. Ruff and naming passed. [Local evidence](https://github.com/chawansit/flash-ticketing/blob/8c244f033523a2d24aa16717a8c15f321abd113f/docs/adr/https:/github.com/chawansit/flash-ticketing/blob/8c244f033523a2d24aa16717a8c15f321abd113f/docs/capacity/flash-sale-opening/generator-completion-local-2026-10-08.json). No cloud improvement is claimed at this checkpoint.


The cloud comparison executed at 84 offered journeys/s for 300 seconds with unchanged backend and budgets. It dispatched 23,545 of 25,200 journeys, confirmed 23,435 tickets, recorded 1,655 true-cap drops and 110 customer 503 outcomes. The transferred generator hash matched the candidate. All 16 retained drop snapshots had zero completed tasks counted. Durability p95 remained 2.06 s, but payment-to-ticket p95 worsened from 3.97 s to 8.56 s and status reads rose from 3.79 to 7.95 per journey. This failed comparison establishes no capacity improvement. Sequential environment variability remains unresolved; do not attribute the regression solely to this patch.

Independent ADR0227 verification reconciled 23,533 successful payments/tickets and 12 expired unpaid orders, elapsed TTL, relationship integrity, zero global duplicates, complete queue/Kafka drain, identical index, restored runtime and generator idle in 157.719 s. Original failed gates remain unchanged. [Cloud evidence](../capacity/flash-sale-opening/generator-completion-probe-2026-10-08.json), [recovery receipt](../capacity/flash-sale-opening/generator-completion-recovery-2026-10-08.json). Hourly qualification remains blocked.
