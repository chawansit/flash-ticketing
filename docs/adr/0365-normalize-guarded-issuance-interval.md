# ADR0365: Normalize the guarded issuance interval

## Status
Proposed; requires explicit user approval before changing live acceptance criteria. Existing runner and historical results remain unchanged.

## Context
ADR0364 run c5125e4d9cd8 completed54000/54000 journeys with0drops,0initial/recovered/final failures. Financial, observer, projection, drain and restoration passed. Full300-second durable issuance was53660; guarded issuance53305. The only failed gate compared the guarded count with53400. Original raw hash6a769feb85750c842b232ef8850ff39846aa5bcfb1d193a5446e807a080be285 remains failed/restored.

The query in cce_hourly_window.py counts guarded tickets from start+2 through start+298 (296seconds), while cce_recovery_headroom.issuance_program substitutes the300-second minimum(rate*300-600) into that count comparison. A uniform180/s stream produces53280 tickets in296seconds; the current threshold53400 requires180.405/s inside that interval. At200/s a uniform stream produces59200, below the59400 threshold. Boundary transients can change counts, so optimizing latency alone cannot reliably resolve this inconsistency.

## Proposed decision
Keep the exact full-window and guarded SQL intervals, joins, distinct counts, two-second clock guards, clock-agreement check and expected totals. Require BOTH:

- Full300-second unique paid/issued count >= rate*300-600, unchanged.
- Guarded296-second count /296 >= the full offered rate, using exact integer multiplication.

Thus180 retains full minimum53400 and requires guarded53280;200 retains full minimum59400 and requires guarded59200. The guarded rate has no two-per-second tolerance. Report each count with its own duration/rate. Preserve zero generator drops, zero initial/recovered/final failures for progression, complete dispatched/final accounting, all existing latency, financial, post-TTL, observer, drain and restoration gates. No application, persistence, locking, messaging, idempotency, TTL, scaling, resource or offered-window changes.

After approval, implement as an explicit source-bound measurement mode and run a fresh180/s300-second qualification, then200only if that fresh round fully passes/restores. Do not promote the retained failed run, reset consumed identities or rewrite raw reports/journals.

## Alternatives
Keep current criteria: conservatively possible to satisfy through favorable boundary transients, but a stable ideal offered stream can fail. Reduce clock guards, extend offered traffic or increase offered rate: changes the approved measurement envelope. Ignore the guarded count: loses a useful clock-conservative check. Promote retained run after local reevaluation: rejected; fresh execution will avoid retroactive acceptance.

## Consequences
This is an acceptance-criteria correction and can change pass/fail classification. Explicit approval is required under the repository instruction to preserve the work envelope and avoid correctness relaxation. It does not declare200/s achieved or reduce financial/business correctness. Existing failures remain visible.

## Failure and recovery
Draft code is isolated and is not imported by the live runner. Reject malformed cohorts/timestamps/rates. Existing financial/drain/restoration and consumed-attempt stop behavior remain. New failure stops progression; no main merge or infrastructure resize.

## Validation evidence
13 deterministic/negative/source checks passed. Five actual PostgreSQL query-model cases passed in the immutable ADR0364 image: ideal180/200streams retain identical SQL counts/guards while correcting classification, and low full-window count, low guarded count and duplicate join still fail. The initial synthetic model lacked ANALYZE statistics and hit statement timeouts; its owned test was stopped/cleaned, then the analyzed model with integer start timestamps passed all5cases in7.67s. All owned local resources removed; Ruff passed. This tests query semantics, not financial reconciliation or cloud capacity.

The proposed assessment of retained counts (53660full,53305guarded;178.867/s over300s and180.084/s over296s) is diagnostic only. Original failed/restored status remains unchanged and cannot authorize200. [Reviewable proposal](../capacity/cce/guarded-window-correction-proposal-2026-10-19.json). Live runner criteria have not been changed.
