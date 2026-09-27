# Redis-first write-mix capacity boundary

**Run window:** 27-28 September 2026, Asia/Bangkok
**Source revision:** `5929222430a050743a8e924a5bf2fdcff6daf73d`
**Outcome:** 1,000 total RPS passed for 15 minutes at a 97% read / 3% hold mix. Higher write shares passed only short stages or failed sustained durability and are not production capacity claims.

## Purpose

This sequence measured how much of a 1,000 RPS workload can use the Redis-first
seat-hold path while PostgreSQL remains the durable authority. Every stage used
the same four API replicas, admission limit four, two reservation writers, two
Kafka consumers, separate refresh and expiry workers, eight generator workers
and the managed Huawei Redis and PostgreSQL services. Only the write percentage
and stage duration changed.

HTTP 202 is a provisional Redis acknowledgement. A stage passes only when every
acknowledged hold has a matching durable PostgreSQL command after the TTL wait,
seat-ownership intervals never overlap, all queues drain and rollback restores
the PostgreSQL reservation topology.

## Results

| Total RPS | Read / hold mix | Duration | Physical requests | Drops | Read p95 | Hold p95 | Durable holds | Result |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1,000 | 90% / 10% | 3 min | 179,807 | 193 | 291.13 ms | 627.17 ms | 12,838 of about 17,980 | Failed latency, delivery and durability |
| 1,000 | 94% / 6% | 3 min | 180,000 | 0 | 65.29 ms | 133.09 ms | 10,800 / 10,800 | Passed short stage |
| 1,000 | 93% / 7% | 3 min | 180,000 | 0 | 83.79 ms | 185.24 ms | 12,600 / 12,600 | Passed short boundary stage |
| 1,000 | 94% / 6% | 15 min | 899,944 | 56 | 141.83 ms | 299.79 ms | 34,781 / 53,997 | Failed delivery and durability |
| 1,000 | 97% / 3% | 15 min | 900,000 | 0 | 28.53 ms | 51.15 ms | 27,000 / 27,000 | **Passed sustained confirmation** |

All five audits found zero overlapping seat ownership. Every stage drained its
outbox, refresh, dead-letter and Redis reservation queues and passed rollback.
Empty queues therefore do not prove durability: the failed 6% sustained stage
had no durable PostgreSQL record for 19,216 provisional acknowledgements even
though its queues
eventually returned to zero.

## Sustained planning point

The validated planning point for this exact topology is:

- **1,000 aggregate RPS for 15 minutes**;
- **970 seat-map reads/s and 30 seat holds/s**;
- zero generator drops, transport errors, retries and admission rejections;
- 28.53 ms worst-worker read p95 and 51.15 ms worst-worker hold p95;
- 27,000 of 27,000 provisional holds durable in PostgreSQL;
- zero double-booking and empty queues after the TTL audit.

This is a 15-minute capacity result, not a 30-minute production certification.
It is also a workload-specific point: changing seat-map payload size, hot-seat
contention, hold TTL, payment traffic, topology or background work requires a
new matched validation.

The 7% result demonstrates a three-minute burst boundary of 70 holds/s. It must
not be used as a sustained production target. The 6% sustained failure shows
that a short passing stage can consume temporary Redis/TTL headroom while the
durable writer falls behind.

## Bottleneck evidence

The 6% sustained stage accepted 53,997 holds but persisted only 34,781, an
observed durable rate of about 38.65 holds/s over the 900-second load window.
PostgreSQL sampling repeatedly observed `WalSync` and `WALWrite` waits, with up
to eight interesting waiters. It recorded about 881.8 MB of WAL during the
observation. The reservation queues eventually drained, but 19,216 provisional
acknowledgements
still had no durable PostgreSQL record at the post-TTL audit.

At the passing 3% point, the observer still saw WAL waits, but the offered
30 holds/s stayed below the observed durable ceiling. The stage generated
27,000 holds and the associated refresh pipeline completed 54,000 updates. Peak
Kafka lag was 21, peak pending refresh was 350, and the observed queues drained
after their peak in 16.9 seconds. No WAL buffer-full event or requested
checkpoint was observed; WAL timing is unavailable on this managed template.

The result supports the current diagnosis: Redis removes the API admission and
lock bottleneck for short bursts, while the PostgreSQL reservation transaction
and WAL path set sustained write capacity.

## Decision and next engineering work

Use 30 hold RPS per tested topology as the current sustained planning limit and
keep the 70 hold RPS result classified as a short burst boundary. Do not promote
60 or 70 hold RPS to production capacity from these results.

Raising the write limit requires an ADR before implementation because it changes
the persistence/scaling pattern. Candidate work should reduce transactions and
WAL amplification per reservation, then compare controlled writer counts and
connection budgets. Each candidate must repeat the same post-TTL durability,
overlap, queue-drain and rollback checks before a longer confirmation.

## Evidence

- [Compact stage comparison](summary.json)
- Local raw observer output remains Git-ignored and was not committed.
- Credentials, private manifests and private service addresses are excluded.
