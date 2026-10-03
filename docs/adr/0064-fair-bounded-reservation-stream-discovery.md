# ADR 0064: Fair bounded reservation-stream discovery

Date: 2026-09-28
Status: Accepted for implementation; production activation pending clean cloud validation

## Context

ADR 0058 uses one Redis Stream per event so intake Lua keys remain in one Redis Cluster slot. Writers discover those streams through an advisory registry plus bounded keyspace scans. The implementation sorted all discovered keys and retained only the first 5,000. That is a permanent lexical cutoff rather than a fair bound.

The 28 September batch-size-4 diagnostic exposed the failure after repeated isolated fixtures accumulated 5,636 stream keys. A read-only audit found that all 5,000 selected streams were empty while all 1,314 remaining entries were in 103 streams outside the selected prefix. Four recovery writers reduced the backlog until only those excluded streams remained, then made no progress. The writers and batch size were restored after this bounded recovery attempt. No double-booking occurred, but the contaminated capacity stage cannot qualify batching.

## Decision

Keep the per-event stream pattern from ADR 0058 and replace prefix truncation with rotating bounded windows. A writer must finish polling every stream in its current window before refreshing discovery. When the registry exceeds the 5,000-stream window, the next refresh starts at the previous window's end and wraps around the sorted registry. Thus each finite registry member enters a writer window within a bounded number of complete window cycles; no lexical prefix is permanently preferred.

The work performed by one writer iteration remains bounded by `stream_batch_size`, and the hard command batch bound in ADR 0063 remains transaction-wide. Registry and keyspace discovery remain advisory and redundant: registry failure is recovered by incremental `SCAN`, while connection reset clears cursors and restarts discovery safely.

The capacity preflight added with ADR 0063 must reject a stage when any global reservation stream entry or consumer-group pending entry exists. A failed stage is recovered and drained before preparing the next comparison; old commands must never share a measured window with a new fixture.

This decision supersedes only ADR 0058's implementation detail that a bounded registry may retain the first sorted prefix. Per-event streams, Redis Cluster slot locality, PostgreSQL authority, at-least-once persistence and all other ADR 0058 decisions remain unchanged.

## Alternatives considered

- Raise the cutoff above 5,000. This postpones the same starvation failure and provides no fairness guarantee.
- Use one global stream. This conflicts with the per-event hash-slot decision and creates a single Redis ingestion hotspot.
- Delete all empty streams during acknowledgement. Cross-slot registry cleanup is not atomic with an event stream, and stale consumer-group caches create additional recovery races. Lifecycle cleanup may be designed separately after fair discovery is validated.
- Flush DCS between tests. It would hide the production defect and risks deleting unrelated state; test isolation must be enforced by identifiers and clean-start gates.
- Scan the full registry on every writer iteration. It removes the cutoff but makes per-iteration work proportional to historical fixture count.

## Consequences

Every discovered stream becomes eligible without increasing the per-iteration stream or PostgreSQL command bound. Large registries can increase worst-case discovery time by whole window cycles; command-age metrics and the lag-admission gate remain required. Sorting and registry reads occur once per completed window rather than once per command, but the registry itself can still grow with historical event streams. Registry lifecycle compaction remains future work and must receive its own decision if it changes deletion or ownership behavior.

Multiple writers may inspect overlapping windows. Redis consumer groups preserve single delivery, so this costs discovery calls but does not duplicate durable ownership. Writer scaling and batch sizing must still be measured independently.

## Failure and recovery behavior

- A writer crash loses only its local window and cursors. Its replacement begins at a valid window and consumer-group reclaim recovers assigned entries.
- A DCS connection reset clears local discovery and group caches; registry plus incremental keyspace scan rebuild them.
- Missing advisory registry membership is recovered by `SCAN`; no stream entry is deleted by this decision.
- A registry larger than one window rotates only after the current window has been fully polled, preventing time-based refresh from repeatedly skipping the end of a window.
- If clean-start preflight reports nonzero entries or pending messages, no load begins. Operators run bounded writers, verify zero/zero, and only then prepare a fresh stage.
- Rollback restores the preceding source revision and PostgreSQL reservation mode. Existing stream entries remain recoverable; they are never flushed as rollback.

## Validation evidence and activation gates

Before rerunning the batch comparison:

1. Unit tests must prove a registry larger than the window rotates to keys outside the first window and that connection reset clears all discovery state.
2. Existing multi-stream hard batch-bound and Redis/PostgreSQL persistence tests must pass.
3. On Huawei DCS, bounded recovery must reduce global stream entries and pending messages to zero, including the 103 previously excluded nonempty streams.
4. The next batch-size-4 stage must start with zero reservation entries/pending, use a fresh fixture, preserve zero overlap and exact linkage, and finish with zero queues.
5. Only a clean short pass permits the 15-minute 1,000 RPS / 6% write validation.

The discovery audit is diagnostic evidence, not a production-capacity result.

Implementation validation completed on 2026-09-28:

- The focused writer-recovery and discovery suite passed: 5 tests.
- Ruff passed for the changed implementation and tests.
- The isolated Docker Compose suite passed: 232 tests, with 2 dependency deprecation warnings and no failures.
- Huawei DCS backlog recovery and a clean batch-size-4 capacity stage remain activation gates; this local evidence does not claim cloud activation.

Whole-selected-batch cursor advancement is superseded by [ADR 0122](0122-reservation-writer-visited-stream-continuation.md); its other bounded discovery and recovery decisions remain accepted.
