# ADR0261: Preserve accepted consumer batching in the shared image

## Status

Accepted for an isolated packaging correction. The corrected image is built, published and locally verified. Fresh cloud qualification is pending. Supersedes ADR0258's selection of older consumer methods in the shared image; other role behavior and the failed ADR0259 evidence remain unchanged.

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

Executed 16 unit checks passed, including mixed business/refresh batches, stable lock order under reversed input, duplicate replay, writer preservation and rejection of unexpected source. Ruff passed. The corrected image has been published as immutable manifest `sha256:8dfa1e1455e4df32df7f639403ecfd454e551974ab5a28bfe6bd41418c891cc5`. Executed 75 integration tests inside that image passed against isolated PostgreSQL 17.6 and Redis 7.4.5; all 22 copied/installed/imported modules and bytecode match. Only workers.py differs from the previous shared image. No cloud deployment or new capacity result yet.

The ADR0259 placement runner now explicitly pins the separate ADR0261 receipt, with its complete file hash. Runtime worker identity derives from this single verified receipt rather than duplicating digest constants. Historical image receipts and failed reports remain immutable; a new goal and ledger identity are required. A candidate must match a fresh passing corrected control.

The first corrected control `adr0151-1a2670d5cd13` failed before paid dispatch: a remote observer imported a repository-only comparison module that was not shipped. Both safety protocols passed and full restoration completed; paid stages started: zero. Correct the observer packaging with a standalone identity-pin module included in the transferred helper bundle. An isolated subprocess import without repository artifacts verifies this boundary. This changes measurement packaging only, not the application image, workload or quality gates.

Corrected paid control `adr0151-2d46f190577f` completed 24,410 distinct tickets from 25,200 scheduled journeys; 790 were not dispatched. All 83 initially failing journeys recovered. The original result remains failed. Reuse the ADR0260 independent recovery protocol with an explicit immutable run/ledger/result/count target; reject mixed original/corrected scope and preserve both historical reports. Independently verify every actual paid ticket and restore ownership before clearing recovery; this does not qualify capacity.

Fresh control diagnostics were complete (353 pipeline rows, all 86 slot failures retained). Primary mean CPU remained 83.19%; simulator due-to-claim mean was 2.682 seconds at a 12-delivery limit. Journey p95 improved from 11.235 to 7.233 seconds, with 24,410 eventual unique tickets (+21.81%) and 790 drops. Post-load startup-proof retrieval failed; this verification gate remains unproven. Independent recovery reconciled every actual ticket, both safety tickets, zero payment loss/double-booking, empty queues and exact restoration in 96.359 seconds. Original failed report unchanged; candidate blocked and no hourly capacity claim. Public evidence: [corrected control](../capacity/cce/shared-worker-consumer-restored-control-2026-10-10.json).
