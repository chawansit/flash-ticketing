# Coherent reconciler: controlled 1,000 RPS comparison

Date: 2026-09-28 UTC. Both runs used four API replicas, one reconciler, one refresh worker, one expiry worker, three Redis-first reservation writers, one Kafka consumer, 800 shows with 300 seats each, 94% delta reads / 6% reservation writes, and Huawei RDS/DCS. Each scheduled 30,000 requests over 30 seconds. These are short diagnostics, not sustained production-capacity proof.

| Measure | Before: stale reconciler, `1ef5ae1` | After: coherent reconciler, `c8b3963` |
| --- | ---: | ---: |
| Requests delivered / scheduled | 30,000 / 30,000 | 30,000 / 30,000 |
| Generator drops / unexpected errors | 0 / 0 | 0 / 0 |
| Worst-worker read p95 | 99.096 ms | 12.824 ms |
| Worst-worker hold p95 | 205.220 ms | 26.776 ms |
| Durable acknowledged holds | 1,800 / 1,800 | 1,800 / 1,800 |
| Booking overlap / final pending queues | 0 / 0 | 0 / 0 |
| Atomic Redis observer samples | 381 | 427 |
| Same-incarnation version regressions | 533 | 0 |
| Delta-history overlap observations | 5,299 | 0 |
| Delta-history tail mismatches | 5,796 | 0 |
| DCS server identity changes | 0 | 0 |

The prior reconciler had `cache.py` SHA-256 `215a97bb…`; the current seat-map writers used `a92c148f…`. Its older full-snapshot Lua recomputed the aggregate version from PostgreSQL source versions while the current Redis-first path also advances that version. The deployment omitted the reconciler from rebuild and image verification. [ADR 0078](../../../adr/0078-coherent-reconciler-image.md) records the fix. The latency difference is observed, but these short runs are insufficient to attribute the entire latency improvement to this single change.

The after-run observer counted 25,967 repeated observations of a nonpositive `0→0` bootstrap delta entry. This entry does not advance the cursor; it is distinct from version regression, overlap and tail mismatch. It should be removed before longer delta-feed validation. Both stage rollback and the fixed post-TTL audit passed. The synthetic sale windows were retired without deleting database rows; tracked synthetic reconciliation work returned to zero.

Next: remove zero-length bootstrap history in a separate ADR-backed change, validate the delta chain again, then run a sustained stage before claiming production capacity.
