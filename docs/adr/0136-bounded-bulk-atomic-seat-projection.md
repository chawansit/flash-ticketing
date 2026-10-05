# ADR 0136: Bounded bulk commands within atomic seat projection

- Status: Rejected after local qualification; original runtime restored
- Date: 2026-10-04

## Context

ADR0135 implements bounded single-concert fixtures but fails local qualification. Fresh18,000-seat prewarm fails the existing100ms Redis operation deadline. An owned-local reproduction measured249.684ms server EVAL and197.665ms client timeout; an independent unchanged-deadline probe also failed. The eventual complete hash does not qualify acknowledgement. ADR0009 already retained large full-refresh timeouts.

The current PUT Lua script reads every seat with HGET and writes every selected seat with HSET inside one atomic invocation. This incurs one Redis command dispatch per seat in each pass. The same-layout synthetic generator timing failure is separate: both single-concert and distributed controls fail arrival/completion gates. This decision does not fix or waive that failure.

## Decision

Use bounded HMGET and HSET command groups inside the existing PUT Lua invocation. Each group contains at most256 seat fields (512 HSET field/value arguments). Select each seat with the same strict per-seat source-version comparison, calculate the same aggregate version, then publish the selected changes with the same aggregate marker.

Retain the whole-map atomic Redis invocation, updating marker, cache/delta key formats, incarnation, layout ETag, source-version replay fencing, delta generation/history bound, and current TTL rules. Readers cannot interleave between groups. This changes command dispatch granularity, not snapshot authority or the external protocol. Input parsing and total map memory remain O(inventory); this is not an unbounded inventory claim.

Partially supersede ADR0009's per-seat Redis command execution only. Its persistence and recovery contract, and ADR0071/0074/0075 delta/TTL/incarnation contracts, remain. No schema, PostgreSQL transaction/locking order, financial idempotency, Kafka delivery, reservation scheduling, API pool/client budget or scaling change.

## Alternatives and consequences

Increasing the100ms operation timeout would hide the failure and alter service budgets; reject it. Skipping fixture warmup because the paid control disables order-status caching does not qualify the large seat projection; reject it. Publishing separate chunk transactions requires reader fencing and race-safe publication across calls; defer it. A fresh-map-only fast path bypassing source-version checks risks interrupted or expired-map repair and adds another mutation protocol; reject it.

Bulk commands reduce Redis call overhead but still encode/decode every selected seat. They may be insufficient for18,000 seats; measure rather than assume a pass. Chunk size256 bounds Lua unpack/command argument counts. Existing duplicate input selection behavior is retained, including the same ordering of writes.

## Failure and recovery

The updating marker precedes seat writes and remains until all seat/version/layout writes finish. A Redis/Lua error after partial group publication leaves the marker, making browse/delta unavailable and rejecting patches. Full source-version-fenced reconciliation repairs it. A lost acknowledgement remains ambiguous and the durable worker lease remains replayable. No retry is added to the client or fixture preparer.

Full refresh may extend the existing cache/delta TTL; patches must not extend it. Missing cache rejects patches. Stale full snapshots and out-of-order/duplicate patches cannot lower source or aggregate versions. Redis stays advisory; PostgreSQL remains the financial and inventory authority.

No cloud deployment or workload progression on failed gates. A future cloud qualification of this runtime factor must be separate from concert concentration and retain financial/postTTL/zero-double-booking/full-keyspacequeue/Kafka gates, cleanup and restoration to passing ADR0133. No higher load or main merge.

## Validation evidence

At decision time no bulk implementation or test has executed. Retained failing baseline: [ADR0135 local qualification](../capacity/flash-sale-opening/single-concert-tooling-local-validation-2026-10-04.json).

Required local checks:256-boundary and multi-group cases,18,000-seat unchanged-deadline prewarm/export/signed financial replay, out-of-order and acknowledgement replay, concurrent full/patch selection, dirty-marker repair, TTL/history/incarnation behavior and financial/concurrency regressions. Compare the unchanged implementation and candidate with identical local data/deadlines and retain failed attempts. Synthetic generator timing and cloud capacity remain separate unresolved gates.

## Executed outcome and disposition

Focused49passed/2failed13.33s with no skips; both18,000-seat original-deadline qualification cases failed. Bulk server EVAL139.975ms versus original147.624ms in one owned-local same-data comparison; both exceeded100ms and timed out. Prefix probes are separate invocations, not exact spans. No capacity improvement or successful large-show signed replay is claimed. Broader regression and cloud execution were not started after these failed gates.

The candidate code/tests/patch are retained privately with hashes and failures. Original cache source was restored exactly; the intended partial supersession of ADR0009 did not take effect. Existing publication, locking, financial, idempotency, TTL and scaling decisions remain active. See [retained outcome](../capacity/flash-sale-opening/bulk-seat-projection-local-validation-2026-10-04.json). No cloud access/load, higherload, push or mainmerge. A different encoding/publication pattern requires a new ADR before implementation.
