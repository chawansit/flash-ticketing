# ADR 0139: Framed pre-encoded payloads within atomic seat projection

- Status: Accepted for local implementation and validation; cloud use gated
- Date: 2026-10-04

## Context

ADR0138's corrected patch header passes its regressions, but the resumed focused run is 51 passed / 2 failed: both 18,000-seat warmup checks time out at the unchanged 100ms Redis socket deadline. Five alternating fresh-map samples on the same owned Redis show original acknowledgement 0/5 and raw-argument candidate 4/5. Mean Redis EVAL time falls from 120.892ms to 82.416ms, while client total rises from 159.557ms to 234.348ms. The candidate sends 18,012 EVAL arguments and constructs a JSON encoder per seat. This is not qualification or a capacity claim.

## Decision

Partially supersede ADR0138's one-body-per-Redis-argument choice. Keep its small ID/source-version metadata envelope, but concatenate compact Python-encoded JSON objects into one newline-delimited raw payload argument after the fixed ten-field header. Reuse one JSONEncoder per call. JSON encoding escapes literal newlines inside arbitrary strings, so newline framing is unambiguous without decoding the seat objects in Lua. Lua scans frames in input order, checks exact count and object boundaries before any write, and keeps the existing selection, aggregate-version insertion and bounded HMGET/HSET publication. Empty seat sets have an empty payload. Full and patch calls use the same representation; no additional dependency is introduced.

The script and caller travel together; this is an internal invocation protocol, not a persisted or client-visible format. Older callers can still read the same stored hash/deltas. No large-map special path or multiple-call publication is introduced.

## Alternatives

Keep raw arguments: it retains measured client overhead and timeout. Return to a JSON array of encoded strings: it adds escaping and Lua decoding already rejected in ADR0138. Raise the deadline or accept eventual writes after timeout: changes or waives the gate. Chunk publication across calls: requires a different reader/generation and patch-merge protocol and is deferred. Binary serialization/new JSON dependency: unnecessary before measuring framing.

## Consequences

Total bytes and full-snapshot work remain O(inventory); one payload reduces RESP argument handling, not the underlying write count. Python and Lua costs must be measured independently. Framing may not remove server-side variance, and a failed local gate still blocks cloud progression. The 100ms socket timeout does not bound Python construction or the whole request wall time. Source-version/aggregate/history/incarnation/TTL and API JSON remain compatible.

## Persistence, locking, messaging, idempotency, TTL and scaling

PostgreSQL remains authoritative. No financial transaction, locking, hold ownership, payment idempotency, outbox/inbox/Kafka semantics, TTL, lease, worker fairness, scaling or connection budget changes. Atomic Redis shield and projection remain single Lua invocations with existing hash-tagged keys. All socket deadlines remain unchanged.

## Failure and recovery

Malformed frame count/boundaries must return an error before writes or history deletion. A publication error after the updating marker leaves the map unreadable until source-version-fenced full reconciliation repairs it. Lost acknowledgements remain errors; replay must retain the original TTL/incarnation and avoid duplicate deltas or financial effects. Cache loss creates a new incarnation and resets history. No retry is added to hide deadline failures.

## Validation evidence

Recorded before implementation. Preserve ADR0138's failed focused log and same-host comparison. Required: empty and group-boundary inputs, escaped newline/Unicode/quotes, malformed framing before writes, stale full/concurrent patches, interrupted group repair, lost-ack replay, exact history/TTL/incarnation, 18k unchanged-deadline warmup and signed financial replay. Run full local unit/integration tests only after focused tests pass. Compare identified baseline and candidate on the same owned Redis with acknowledgement counts and both encoding/client/server times. No cloud test, push, merge or production sizing is authorized by this decision.

### Measured implementation refinement

Initial framing passed 65 focused cases but failed both 18k gates. Same-host comparison: original 0/5 acknowledgements, framing 1/5; mean server148.023ms/102.261ms respectively. Separate prefix probes measured cold metadata20.429ms, selection69.203ms, complete89.199ms; replay metadata20.644ms, selection109.708ms, complete95.497ms. These are independent samples, not additive spans. Redis TIME was fixed during the invocation here; discard its zero-span diagnostic. Before the next implementation, remove redundant per-seat Lua table allocations and table.insert dispatch by reusing decoded metadata rows for selection and indexing bounded arrays directly. This retains the same framing, selection and single-invocation protocol. Qualification remains failed until rerun; failed evidence is retained.

The allocation refinement passed 66 focused cases, including the 18k projection/replay test, but the large-fixture warmup still timed out. Before the next implementation, avoid redundant HMGET/field-array construction only when Redis EXISTS confirms the whole hash is absent. A hash lacking version metadata, a dirty partial publication or any retained seat fields still takes normal source-fenced reads. Keep the same selection/write loops, acknowledgement gate and recovery protocol; this is not a separate publication path.

The absent-hash optimization still leaves both18k gates failed (66 passed/2 failed). The next local representation refinement flattens ID/source metadata into alternating values and stores selected IDs/bodies/retained-old states in separate Lua arrays. This removes one decoded metadata table per seat and avoids replacing it with one selection object per seat. Exact even metadata/frame counts are validated before writes. The same external representation, source-version fencing and single atomic invocation remain; do not broaden tests or cloud runs on failure.

The flat-metadata focused run is67passed/1failed: actual fixture warmup still times out. The flat comparison uses a reduced seat shape without hold_id/reserved_until_epoch, so its66.214ms server mean is not an actual-fixture qualification. Next refine the same encoding contract before implementation: Python appends the predicted aggregate (sum of input source versions) as the final numeric field. Lua validates this exact suffix and uses the input body unchanged only when complete source-fenced selection produces that same aggregate. Otherwise Lua replaces only the validated numeric suffix. This removes a large body copy/concatenation for cold snapshots without trusting the prediction over retained source versions. Patches, stale snapshots, dirty repairs and aggregate mismatches retain the computed aggregate. Compare with the exact worker seat fields as well as the reduced diagnostic shape; do not hide prior failed acknowledgement evidence.

### Executed final candidate

The final encoding uses flat alternating ID/source metadata, one framed payload and a predicted-version argument after the fixed ten-field header. JSONEncoder owns string escaping; the predicted numeric version is the final field in each frame. Lua validates the exact suffix and uses it unchanged only if the computed source-fenced aggregate matches; otherwise it replaces that suffix. Grouped writes, updating-marker recovery, deltas, TTL and incarnation contracts stay unchanged.

Windows focused68passed/1failed21.50s:18k projection and signed paid replay passed, but a513-seat replay timed out. Overall Windows gate remains failed. Native Linux ADR0140 focused69passed/full721passed, no failures or skips, at unchanged100ms. Final full-worker-shape comparison on native Redis: original18k ack0/5/server155.398ms; candidateack5/5/server45.991ms. Candidate construction94.936ms/EVAL50.944ms/client145.88ms; baseline client time is censored by timeout. This qualifies only the specified local Linux tests, not cloud capacity. [Evidence and retained failures](../capacity/flash-sale-opening/framed-seat-projection-local-validation-2026-10-04.json).
