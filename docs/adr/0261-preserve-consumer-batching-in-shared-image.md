# ADR0261: Preserve accepted consumer batching in the shared image

## Status

Accepted for an isolated packaging correction. A corrected image and fresh cloud qualification are pending. Supersedes ADR0258's selection of older consumer methods in the shared image; other role behavior and the failed ADR0259 evidence remain unchanged.

## Context

The failed ADR0259 control issued 20,040 eventual tickets from 25,200 offered journeys, with high primary CPU, heavier status polling and database-slot pressure. Source comparison against the previous hourly worker inventory identified a concrete regression: shared worker source `dec31b79...` differs from accepted consumer source `8cf69f94...` by its writer constructor and two consumer behaviors. It flushes seat-refresh intents before each business event and lacks stable show locking order. The accepted consumer deferred refresh intents within each bounded Kafka partition batch and sorted show locks. Those changes were omitted when the API-parent source and writer were combined.

## Decision

Restore only `consume_events` and `consume_refresh_batch` from the verified accepted consumer source into the shared source, retaining the writer pipeline constructor and every other module/function. Keep business event order, transactional inbox deduplication, replay handling, latest committed seat projection and bounded batch size. This restores accepted behavior rather than adding another concurrency or delivery model.

Expose an explicit corrected-build option with ADR0261 provenance and a separate receipt/tag. Preserve historical ADR0258 output by default and the immutable selected image/evidence. Do not silently repoint deployments, relax quality gates or start the blocked placement candidate. A corrected image must pass image/source checks and correctness tests before a freshly registered control can measure its effect.

## Alternatives

Scale immediately: cannot distinguish lost batching from placement pressure. Replace the complete worker module: risks dropping the accepted reservation-writer pipeline. Revert all roles to old images: loses the approved shared-image goal. Change polling at the same time: mixes factors and masks slower ticket issuance.

## Consequences

Expected benefit is fewer refresh transactions and reduced lock conflicts, potentially shortening ticket completion and avoiding repeated status reads. Source regression is proven; its contribution to measured slowdown is a hypothesis until compared under the same paid workload. No improvement or production capacity is claimed.

## Failure and recovery behavior

Reject unknown source hashes or unexpected function spans. Duplicate envelopes must remain deduplicated in the same transaction. Failed batches remain replayable; locking order must be independent of input order. Preserve the prior image/receipt for rollback. A failed fresh control stops progression and requires durability, queue and restoration checks.

## Validation evidence

Executed 16 unit checks passed, including mixed business/refresh batches, stable lock order under reversed input, duplicate replay, writer preservation and rejection of unexpected source. Ruff passed. No corrected image has been published or deployed, and no additional load has started.
