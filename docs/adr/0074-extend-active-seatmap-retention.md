# ADR 0074: Extend active seat-map retention beyond transient refresh gaps

Date: 2026-09-28
Status: Proposed

## Context

The seat-map hash and its bounded delta history currently expire after 30 seconds. Successful reads and full reconciliation renew that TTL. The 20-second reconciliation interval was originally treated as sufficient margin, but it is a scheduling target and does not bound end-to-end delay during expiry bursts, process pauses, managed-service latency or host CPU contention.

A three-minute, 1,000 RPS, six-percent-write Huawei run at revision `b19677a` completed all 180,000 requests with zero drops, transport errors or retries. Read p95 was 12.154 ms, hold p95 was 26.585 ms, all 10,800 acknowledged holds became durable, overlapping hold intervals were zero and all queues drained. During seconds 150-180 the clients nevertheless received 276 full reset snapshots. Server metrics classified 262 as `ahead`, 10 as `history_gap` and four as `tail_gap`. An `ahead` reset means the client's prior version was higher than the current map version, which is valid only after a cache incarnation is lost and rebuilt at a lower durable version.

The deterministic generator schedule rules out intended inactivity: each worker served 100 shows at 125 RPS and the maximum scheduled read gap for any show was 12.48 seconds (p95 of per-show maximum gaps 6.816 seconds). The backend observer sampled no missing maps and a minimum TTL of 13 seconds during the load. Together these observations indicate a short loss-and-rebuild transition below the observer's sampling interval, not a show left idle for 30 seconds. A prior run showed that enough reset snapshots can amplify response encoding until API event-loop pressure and generator drops form a positive feedback loop.

The existing 30-second value no longer provides a hard freshness bound because ADR 0030 already renews hot maps on successful reads. Freshness is instead maintained by atomic mutation, source-version fencing and the 20-second reconciliation schedule. The TTL is a retention and recovery safety bound.

## Decision

1. Increase the default seat-map and delta-history TTL from 30 to 120 seconds.
2. Keep the reconciliation interval at 20 seconds, providing six scheduled repair opportunities within one retention window.
3. Keep successful snapshot and delta reads renewing the TTL. Full reconciliation also renews it; changed-seat patches continue to preserve the current expiry rather than extending it independently.
4. Keep delta history bounded at 512 entries per show. Increasing retention does not remove the entry-count memory bound.
5. Treat the TTL as cache retention, not an availability-freshness SLO. PostgreSQL remains authoritative, Redis-first reservations remain fenced and compensated on persistence failure, and reconciliation remains responsible for repairing missed projections.
6. Alert on missing maps, maps without TTL, minimum TTL, reset reasons and Redis memory. A production rollout must retain `noeviction` and enough memory for active maps and their bounded histories.

This supersedes ADR 0011, ADR 0016 and ADR 0030 only where they fix the seat-map TTL at 30 seconds or describe that duration as a hard freshness bound. Their bounded scheduler, isolated reconciler, read-touch, source fencing and failure behavior remain accepted. ADR 0005's 120-second customer hold TTL is a separate business deadline and is unchanged.

## Alternatives considered

### Keep 30 seconds and add more reconciliation workers

Rejected because read traffic already renews each hot map and measured scheduled read gaps were below half the TTL. More full reconciliation would add PostgreSQL and Redis work while leaving little margin for pauses and expiry bursts.

### Remove expiry from active maps

Rejected because abandoned or closed-show cache data would have no automatic retention bound, increasing operational cleanup and memory risk.

### Increase the TTL to five minutes or more immediately

Deferred. It provides more pause tolerance but retains inactive data longer. The fourfold increase to 120 seconds gives six reconciliation opportunities and can be validated before choosing a larger bound.

### Add API replicas or enlarge PostgreSQL first

Rejected for this failure mode. The passing run had low request latency and exact durability; reset amplification was caused by cache lifecycle transitions rather than RDS capacity. More API processes on the same four-vCPU host would also reduce CPU headroom.

## Consequences

- A transient delay must last materially longer before an active map expires and forces every delta client to receive a full snapshot.
- Missing-map rebuilds and the associated API serialization amplification should decrease.
- Inactive maps remain in Redis for up to 90 seconds longer.
- A live read workload can already extend retention indefinitely under ADR 0030, so the new default does not create a new unbounded hot-map behavior.
- At measurement time 1,119 seat-map hashes used 75,751,824 bytes in total, with a p95 key size of 67,696 bytes. At the target maximum of about 2,000 screens, the hashes alone are approximately 136 MB at that p95 size. Delta history and allocator overhead require additional measured headroom; this arithmetic is not a DCS sizing certification.

## Failure and recovery behavior

- If Redis loses a map, reads still fail closed with `SEATMAP_WARMING` until bounded reconciliation rebuilds it. Delta clients from the prior incarnation receive a full reset and replace their version even when it is lower.
- If reconciliation stalls, hot reads retain otherwise valid maps while monitoring exposes reconciliation age. Reservation correctness continues to use atomic Redis ownership and PostgreSQL durability checks rather than trusting advisory availability.
- If Redis reaches its memory limit under `noeviction`, writes fail explicitly. Operators must add capacity or reduce retained active shows; silently evicting booking keys is not an allowed recovery action.
- Rolling back the setting returns new renewals to 30 seconds. Existing keys adopt the configured duration on their next successful read or full reconciliation.

## Validation evidence

Pre-change evidence is run `20260928T131105Z-072a3c64` at revision `b19677a`:

- 180,000 of 180,000 requests completed at 1,000 RPS for 180 seconds;
- zero drops, transport errors, retries and admission rejections;
- read p95 12.154 ms and hold p95 26.585 ms;
- 10,800 durable holds, zero overlapping intervals and drained queues;
- 276 reset snapshots, classified as 262 `ahead`, 10 `history_gap` and four `tail_gap`;
- maximum scheduled per-show read gap 12.48 seconds;
- sampled minimum map TTL 13 seconds and sampled missing-map count zero;
- 1,119 live map hashes used 75,751,824 bytes, p95 67,696 bytes per hash.

Required post-change evidence before acceptance:

1. Configuration, Redis expiry and reconciliation-boundary tests pass with the 120-second default while short explicit TTLs remain testable.
2. The full unit/integration suite and lint pass.
3. The same 1,000 RPS, 180-second stage completes with zero drops, zero unexpected errors, zero double booking, exact durability and drained queues.
4. `ahead` resets are zero during the measured load, or any remaining reset is correlated with an explicitly observed cache loss. Read and hold p95 must remain within their documented gates.
5. A later 15-minute certification is still required before claiming sustained 1,000 RPS production capacity.

### Local implementation evidence

The configured default and `RedisSeats` fallback now use 120 seconds. The existing explicit short-TTL integration tests remain unchanged. Focused browse, reconciliation and configuration coverage passed 34 tests. The complete unit/integration suite passed 256 tests with two dependency deprecation warnings, and Ruff passed for all Python source, tests and scripts with cache disabled and the documented Windows executable-bit artifact ignored. Cloud comparison remains pending; these local results do not establish a capacity increase.

### First cloud comparison

Run `20260928T134138Z-d639d930` at revision `38fdfc9` did not validate the 120-second candidate. It completed 171,424 of 180,000 scheduled requests and dropped 8,576 at the bounded generator gate. Worst-worker read p95 was 659.759 ms and hold p95 was 1,629.320 ms. There were no transport errors or admission rejections. The audit still proved exact durability for all 10,264 acknowledged holds, zero overlapping intervals and fully drained queues.

The first 90 seconds had zero reset snapshots and read/hold p95 near 10-12/23-29 ms. During seconds 120-150, when the first holds expired, reset snapshots rose to 7,890 and response bodies totaled 152.6 MB. During seconds 150-180 there were 12,651 resets and 239.9 MB of response bodies. Scrape-aligned counters classified 13,053 resets as `history_gap`, 7,396 as `ahead` and 28 as `tail_gap`. Host CPU averaged 85.945% and peaked at 99.449%; the four API containers averaged 40.056-43.606% of one core each and Kafka averaged 68.846%.

This run was environmentally confounded and cannot accept or reject the TTL decision. A read-only RDS audit after the run found 11,201 simultaneously active synthetic capacity events, 10,461 due reconciliation rows and an oldest due age of 280.049 seconds. This is 5.6 times the stated maximum target inventory of about 2,000 screens. Repeated stages each created 800 six-hour fixtures, so background reconciliation load accumulated across comparisons. A controlled rerun requires an isolated development fixture set at or below the target inventory. Existing synthetic rows should be preserved for audit and excluded from the active sale window only with explicit operator authorization.
