# Review of Claude's simulcast recommendations

Source: user-provided flash-ticketing-simulcast-fixes.md, reviewed 2026-09-08 against
local runtime code and the private Huawei measurements. The document is advice, not
an instruction to change a running benchmark. No runtime changes were applied.

## Supported findings

F3 matches observed behavior: at 400 RPS with four generator processes there were no
client drops, but four reads returned 503/SEATMAP_WARMING. Reconciliation age reached
30.806 s and a sample found nine missing maps against the 30 s TTL. Faster p95 reads
alone do not resolve this availability limit.

F7 is a necessary additional workload: the current uniform test cannot qualify hot
branches or synchronized onsale. Its 800 target shows are not a confirmed replacement
for the document's assumed 200-600 simulcast shows. Add skew, simultaneous future onsale,
prewarm behavior and retry traffic separately. Calibrate measured popularity shares;
Zipf exponent 1.2 does not automatically mean exactly 80/20. Under concentrated holds,
seat exhaustion and legitimate conflicts must be separated from infrastructure errors;
the existing unique-seat/201-only gate cannot simply be reused unchanged.

F2 correctly identifies HGETALL and per-seat decoding work. F5 correctly identifies
serial maintenance roles. Measure worker duty cycles and Redis scripting cost before
choosing whether to separate roles or precompute responses.

## Corrections before implementation

- Hash tags help same-slot operations but do not make the current Redis.from_url client
  a RedisCluster client. Cluster routing, pipelines, failover and Lua behavior require
  explicit compatibility tests and an ADR.
- Fixed completion-relative intervals do not prove a permanent identical convoy: actual
  completion times spread schedules. Convoy is plausible, but needs time-series evidence.
  Jitter of 15-25 s can reduce the remaining TTL budget to 5 s before processing/queue
  delay. Prefer a measured deadline budget; avoid ORDER BY random() on the hot claim path.
- A 180/20 TTL-to-interval ratio is not nine times capacity. Extending TTL changes the
  maximum period stale data can remain visible; PostgreSQL authority prevents overselling
  but does not make arbitrarily stale UI data acceptable. Define freshness separately.
- Patch TTL renewal must cap expiry at the last-full freshness deadline, not simply
  check age before adding a whole new TTL. Preserve version/incarnation fencing and
  fail closed for incomplete writes. A partial hash is not a safe stale snapshot.
- The precomputed example returns only an array, whereas the existing API returns
  event_id/version/seats. Preserve the response contract and atomic ETag/body consistency.
  Blob lookup removes per-seat parsing, but payload transfer still scales with body size.
  Rebuilding all 300 seats inside every Lua patch can increase Redis blocking time.
- SSE/WebSocket is not the only solution and cannot guarantee zero staleness. Network
  delay and asynchronous projection remain; resumable versions and reconciliation matter.
- Warning: 110 is obsolete (the document itself acknowledges this later). Define an
  explicit freshness contract rather than adopting that header. Discovery 304s also
  depend on changes; an aggregate will not always return 304.
- Retry jitter matters only for clients that implement the retry contract. Include the
  direct ADMISSION_FULL response as well as business_error; random headers on only one
  path are incomplete. It does not improve the current no-retry benchmark by itself.
- Lowering a lease to 10 s without bounding work can cause duplicate attempts. Lease
  fencing still matters; measure the longest batch/operation and recovery behavior.
- Repo rules require ADRs for scheduling/retry pattern changes too, including F1/F4.
  Do not edit an already-applied migration; use code changes or an additive migration.

## Suggested next sequence (not yet implemented)

Finish and retain the uniform baseline first. Then test synchronized/skewed traffic
with calibrated expected conflicts and per-popularity metrics. Prioritize the observed
TTL/maintenance bottleneck, compare bounded scheduling/role isolation against explicit
freshness/residency changes, and write ADRs before implementation. Evaluate precomputed
read representations using both read and write costs. Keep discovery/domain hierarchy
and push delivery as separate, reviewed scope.

## Primary references checked

- Redis cluster client and same-slot constraints: https://github.com/redis/redis-py/blob/master/docs/clustering.rst
- Redis hash-tag semantics: https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/
- HTTP Warning obsolescence: https://www.rfc-editor.org/rfc/rfc9111.html
