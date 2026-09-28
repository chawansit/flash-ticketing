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

1. Each atomic Redis seat-map patch records one change entry containing the prior
   aggregate version, the resulting aggregate version and only the selected current
   states that changed.
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

- A command failure is retried by durable reconciliation. If Redis reports an error
  after a map update but before recording its delta, continuity validation detects the
  gap and returns a full reset snapshot.
- If the delta history is lost while the map remains, the next delta request returns
  a full reset snapshot. It never returns an incomplete change set.
- If the map is missing or marked updating, reads continue to fail closed with
  `SEATMAP_WARMING`; reconciliation or rollout pre-warm restores it.
- A cache rebuild starts a new bounded history. Versions outside that history receive
  a reset snapshot.
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
snapshot-plus-delta load mode are implemented. The final complete unit and
integration suite passed 253 tests with two dependency deprecation warnings.
Focused real-Redis coverage passed 12 browse tests, including empty and changed
deltas, repeated-seat collapse, bounded trim, missing history, invalid versions and
recreated-history TTL. Ruff passed for all changed Python files when excluding the
documented Windows executable-bit mount artifact. The Huawei generator shell passed
a Linux syntax check. Cloud validation remains pending.

No higher production capacity is claimed until those stages pass.
