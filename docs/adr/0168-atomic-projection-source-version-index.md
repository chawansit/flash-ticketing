# ADR0168: Atomic projection source-version index

- Status: Evaluated; rejected for default/cloud adoption; isolated candidate retained
- Date: 2026-10-06

## Context

ADR0167 measured 18,000-seat full-publication arguments of approximately 2.7 MB. Existing full Lua spends roughly 60–90 ms decoding all old seat JSON, including unchanged rows, against the unchanged 100 ms socket timeout. Two bounded local contention runs passed but provide little deadline margin and do not close the earlier intermittent timeout. Client encoding and read scheduling also contribute; this decision targets only server-side comparison cost.

## Decision

Keep two co-located keys and single atomic Lua publication. Use seatmap:v3 and seatdelta:v2 namespaces so legacy writers cannot silently invalidate the version index; this changes projection schema only, not the seat hold/shield keys. A new namespace starts cold with a new incarnation, and callers use the existing warming/reset behavior. Store a numeric source-version field alongside each seat JSON in the existing hash. Read source fields in bounded groups; skip decoding old JSON when its version index exists and no dirty repair requires the old body. Read old JSON in groups for index-missing maps or dirty repair. Populate missing indexes only after all input validation, and update changed bodies and their indexes in the same HSET. Legacy-shaped maps in the new namespace can be repaired by successful publication; older namespaces are not copied blindly or modified. Redis remains a projection; PostgreSQL remains the ownership authority. No TTL, hold, delta-history, incarnation, retry or measured deadline changes. Supersedes ADR0167's no-correction-selected state, not its measurement protocol or accepted atomic-publication decisions.

## Alternatives

Increasing the socket timeout would conceal insufficient margin. EVALSHA removes little relative to a 2.7 MB payload. A separate index key introduces additional expiry and generation coordination. Client serialization changes do not directly address the measured Lua cost. None selected in this correction.

## Consequences

Adds one small hash field per seat and bounded index-read calls. Namespace isolation requires fresh warm-up before rollout; rollback uses the previous namespace and its existing freshness policy. Fresh and reconciled maps can skip old JSON decoding. A legacy map pays one migration scan. Encoding, payload transport and large reads remain separate costs. A local performance improvement is not a production-capacity claim.

## Failure and recovery

Validate every incoming frame before writing. Body/index updates occur in the same HSET within the single Lua execution. Interrupted group publication remains marked unreadable; a full reconciliation repairs it. Missing or invalid indexes fall back to the persisted body source version. Missing aggregate metadata forces full body repair even when per-seat indexes remain. Older-version containers write the previous namespace; they cannot corrupt the new index. A stale full snapshot must preserve any newer acknowledged patch. Ambiguous acknowledgements retain existing replay semantics. No retry masks measured timeouts.

## Validation evidence

Executed106 native integration tests: projection migration/repair, group interruption, stale races, replay, malformed frames, TTL/history, browse/reconciliation and payment recovery. Initial missing-aggregate repair regression failed and was corrected before qualification. Bounded contention compared original and candidate at unchanged resources/deadlines with no retries. Reversed order yielded39% lower Lua median but18% worse client publication and20% worse large reads; hash data increased9%. Candidate is rejected for default/cloud adoption. Original application source and namespaces restored. Keep reviewed candidate in artifacts/projection-source-index/adr0168.patch with manifest; no cloud image/profile changed. See [comparison](../capacity/flash-sale-opening/large-projection-contention-comparison-2026-10-06.json). The18,000-seat issue remains open.
