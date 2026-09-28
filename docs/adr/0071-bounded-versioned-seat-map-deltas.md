# ADR 0071: Serve bounded versioned seat-map deltas after an initial snapshot

Date: 2026-09-28
Status: Proposed

## Context

The corrected three-minute 1,000 RPS control passed, but the same topology did not
sustain 1,000 RPS for 15 minutes. Read p95 rose to 403.157 ms, hold p95 rose to
892.393 ms, and 2,664 of 900,000 scheduled requests were not dispatched because
the generator's bounded in-flight budget was full. Correctness remained intact:
53,826 accepted holds were durable, double booking was zero, and every queue
drained.

PostgreSQL was not the primary read bottleneck in that run. Its maximum observed
query time was 14.438 ms and at most eight interesting waiters were observed.
The workload returned roughly 492,000 full availability bodies and 349,000 HTTP
304 responses. Every changed aggregate seat-map version invalidates the full-map
ETag, so polling viewers repeatedly load, decode, sort and transfer about 300 seat
states even when only one or a few seats changed.

The existing `/seat-deltas` endpoint reduces response size but still performs a
full Redis hash read and decodes every seat before filtering in Python. It therefore
does not remove the hot-path work that the sustained run exposed.

## Decision

Retain `/availability` as the initial snapshot and recovery representation, and
make `/seat-deltas` a bounded versioned change feed for subsequent polling.

1. Every atomic Redis seat-map mutation records one change entry containing the
   prior aggregate version, the resulting aggregate version and only the selected
   current states that changed. This includes projector patches, Redis-first
   provisional holds, and compensation releases.
2. The change feed is stored in a Redis sorted set in the same cluster hash slot as
   the seat-map hash. The resulting aggregate version is the score. Updating the map and appending the change entry occur in one Lua
   invocation.
3. Retain at most 512 change entries per show. This bounds memory independently of
   request volume and viewer count.
4. A delta read atomically validates the seat map and reads only stream entries
   newer than the client's `since` version. Repeated changes to one seat are
   collapsed to its newest state in the HTTP response.
5. If `since` equals the current version, return an empty delta without reading seat
   fields or stream entries.
6. If the cache was rebuilt, the stream expired, the requested version is invalid,
   or retained history cannot bridge the requested version, return a full current
   snapshot with `reset_required: true`. This keeps the endpoint safe for old or
   disconnected clients without a PostgreSQL fallback.
7. A normal delta response includes `reset_required: false`, `from_version`, the
   resulting `version`, and changed seat states. Existing `event_id`, `version` and
   `seats` fields remain present.
8. Delta reads renew the bounded TTL of both the map and its change stream. Full
   reconciliation initializes or repairs the stream; ordinary availability reads
   retain their existing map TTL behavior.
9. The load generator may model production clients by taking one initial snapshot
   per viewer and then applying deltas. Capacity comparisons must report the read
   mode and may not compare snapshot polling and delta polling as identical workloads.
10. A full reconciliation of an existing map preserves a monotonically increasing
    aggregate version. It applies only source-version-fenced changes and repairs a
    lagging aggregate to at least the sum of retained source versions; it does not
    recompute a lower version that discards provisional Redis mutations.
11. Cache loss creates a new incarnation whose reconstructed aggregate version may
    be lower than a client version from the lost incarnation. A client version above
    the current version therefore receives a full `reset_required: true` snapshot.
    Negative or malformed versions remain validation errors.
12. On any reset response, a client replaces its local version with the returned
    version even when it is lower. Normal contiguous deltas remain monotonic.

This extends ADR 0010 and ADR 0017. It supersedes their decision to defer an indexed
delta representation now that sustained cloud evidence identifies full changed-map
responses as the dominant read cost. PostgreSQL remains the ownership authority,
Redis remains advisory, and reservation locking, idempotency, TTL, Kafka delivery
and payment decisions do not change.

## Alternatives considered

### Keep full-map ETag polling and add API replicas

More replicas repeat Redis reads, JSON decoding and response transfer. It can add
short-term headroom but does not reduce work per changed map and may increase Redis
pressure.

### Store one fully encoded availability body in Redis

Reads become cheaper, but every single-seat change rebuilds and writes the complete
300-seat payload on Redis's command thread. This trades repeated read work for write
amplification and still sends full bodies to every viewer.

### Push changes with WebSocket or server-sent events

Push can reduce polling but introduces connection ownership, fan-out, reconnect and
backpressure patterns that have not been designed or measured. It is outside this
MVP step.

### Keep filtering a full Redis hash in Python

This preserves compatibility but retains the measured `HGETALL`, decode and sort
cost on every request.

## Consequences

- Active viewers normally receive a few changed seats rather than a full map.
- Redis retains at most 512 small entries per show in addition to the current map.
- One atomic patch performs an additional stream append and bounded trim.
- Disconnected clients may receive a full reset snapshot; clients must replace their
  local state when `reset_required` is true.
- The full snapshot endpoint remains necessary for bootstrap, repair and simple
  integrations.
- Capacity claims must include the modeled client behavior and delta fallback rate.

## Failure and recovery behavior

- The seat mutation and its delta entry share one Redis Lua invocation. An
  ambiguous connection failure is recovered through the existing idempotent command
  replay; Redis cannot expose only one half of the atomic mutation.
- If a future direct mutation path advances the aggregate version without adding a
  delta entry, continuity validation detects the gap and returns a full reset
  snapshot instead of returning incomplete changes.
- If the delta history is lost while the map remains, the next delta request returns
  a full reset snapshot. It never returns an incomplete change set.
- If the map is missing or marked updating, reads continue to fail closed with
  `SEATMAP_WARMING`; reconciliation or rollout pre-warm restores it.
- A full reconciliation of an existing map cannot lower its aggregate version.
  If the entire map is lost, the rebuilt incarnation may start at a lower durable
  version; clients with a higher prior-incarnation version receive a full reset
  snapshot and replace their local version.
- Redis outage continues to return `SEATMAP_UNAVAILABLE`; PostgreSQL is not exposed
  as a browse fallback.
- Duplicate or replayed source rows remain fenced by their durable source versions
  and do not create a newer change entry.

## Validation evidence

This decision is proposed. Required evidence before acceptance:

1. Unit and real-Redis integration tests for empty deltas, multi-seat patches,
   repeated-seat collapse, bounded trim, expiration, rebuild, invalid versions and
   history-gap reset.
2. Existing snapshot, atomic hold and zero-double-booking tests remain passing.
3. Complete unit/integration and lint suites pass.
4. A three-minute 1,000 RPS, six-percent-write cloud control using the documented
   delta client model passes correctness, latency, workload and queue-drain gates.
5. A 15-minute 1,000 RPS run passes before any higher-rate discovery.
6. HTTP error evidence records request IDs and bounded response diagnostics so an
   unclassified 500 cannot be accepted as a capacity result.

### Local implementation evidence

The bounded sorted-set change history, continuity-checked reset fallback, TTL
inheritance, OpenAPI response model, fixed-cardinality metrics and explicit
snapshot-plus-delta load mode are implemented. Before the cloud run, the complete
unit and integration suite passed 253 tests with two dependency deprecation
warnings. After correcting direct Redis-first mutations, the complete suite
passed 254 tests with the same two warnings. Focused real-Redis coverage includes
empty and changed
deltas, repeated-seat collapse, bounded trim, missing history, invalid versions and
recreated-history TTL. Ruff passed for all changed Python files when excluding the
documented Windows executable-bit mount artifact. The Huawei generator shell passed
a Linux syntax check. The first cloud validation at commit `3e61c75` did not pass. The three-minute
1,000 RPS, six-percent-write run dispatched 137,400 of 180,000 requests and
dropped 42,600 at the generator's bounded in-flight gate. Read p95 was 921.095 ms
and hold p95 was 1,283.302 ms. Eighty-four requests timed out. Nginx also returned
small HTML 500 responses without application request IDs, which is consistent
with upstream pressure rather than a classified application response. The
workload returned roughly 116-121 MB per worker for only about 16,000 reads,
showing that clients received near-full reset bodies rather than bounded deltas.

The failed run exposed an implementation gap: Redis-first provisional holds and
compensation releases advanced the aggregate map version directly but did not
append a delta entry. Continuity validation therefore correctly rejected the
gapped history and returned safe full snapshots. The audited data still had zero
overlapping seat intervals and empty queues. Seven of eight per-worker durability
checks passed; one had 1,020 durable commands for 1,019 acknowledged HTTP 202
responses, so the combined durability gate failed because equality is required.
This is not evidence of a lost accepted reservation.

The correction makes both direct Redis-first mutation paths append and trim the
same bounded delta history inside their existing Lua transaction. The cloud
observer also captures fixed-cardinality delta outcome counters.

The same-topology three-minute rerun at commit `39c5b67` passed every gate. It
completed all 180,000 scheduled requests with zero drops, transport errors or
retries. Worst-worker read p95 was 14.616 ms and hold p95 was 26.601 ms. All
10,800 acknowledged holds were durable after expiry, overlapping seat intervals
were zero, and every queue drained. Total measured response-body volume fell from
954,518,611 bytes in the broken run to 34,717,504 bytes while the number of
completed reads increased from 129,099 to 169,200. The load-balanced observer
recorded empty and changed-delta outcomes in 246 samples and no reset series; this
metric sample is diagnostic rather than an exact aggregate. [Compact cloud
evidence](../capacity/seatmap-delta/README.md) is retained with the comparison.

The first 15-minute rerun at commit `ac8d299` did not pass the workload gate.
It scheduled and physically attempted all 900,000 requests with zero generator
drops, transport errors or retries. Worst-worker read p95 was 15.540 ms and hold
p95 was 28.813 ms. All 49,775 accepted holds were durable after expiry,
overlapping intervals were zero, every queue drained, and rollback completed.

The sustained run exposed a second lifecycle gap. Periodic full reconciliation
recomputed a lower aggregate version after provisional Redis mutations. The
server safely returned reset snapshots, but the generator retained
`max(old_version, returned_version)`; it then sent a prior-incarnation version
that was above the current map and received repeated 422 responses. Those invalid
reads did not renew the map TTL, producing a later cascade of
`SEATMAP_WARMING` responses. Backend samples recorded reset outcomes beginning
during the load, and every worker showed the same 422-to-503 pattern. The
correction specified above preserves versions during in-place full reconciliation
and makes incarnation reset behavior explicit on both server and client.

At commit `2c2c2f5`, the corrected three-minute 1,000 RPS control passed all gates: 180,000 physical attempts, zero drops, transport errors and retries, read p95 20.637 ms, hold p95 33.962 ms, 10,800 durable holds, zero overlap and drained queues. The following 15-minute run no longer showed the 422-to-503 version-reset failure, but failed for an independent edge limit recorded in ADR 0072: Nginx exhausted its 1,024 file-descriptor process limit even though `worker_connections` was configured for 4,096. That run completed 775,280 physical attempts, dropped 124,720 scheduled requests and recorded 168 read timeouts. It does not validate sustained delta capacity.
The corrected implementation passed the focused Redis lifecycle, browse reset and
generator reset regression suite (16 tests), Ruff on every changed Python file, and
the complete unit/integration suite (254 tests; two dependency deprecation warnings)
on 2026-09-28. Cloud validation remains pending.

ADR acceptance and any higher production-capacity claim require a corrected
15-minute 1,000 RPS rerun.
