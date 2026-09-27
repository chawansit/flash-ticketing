# 600 RPS instrumentation rehearsal — 2026-09-20

Status: **Passed all rehearsal gates.** This was a three-minute measurement
check, not a new long-run production-capacity claim. The API and generator
used clean checkout commit 9eb6323 on the two Huawei ECSs. Four API replicas
used admission four each; the unattended workflow restored admission four,
audited after hold expiry and removed private manifests.

| Metric | Result |
|---|---:|
| Offered load | 600 RPS for 180 seconds |
| Completed responses | 108,000 |
| Generator drops / transport errors | 0 / 0 |
| Worst-worker read / hold p95 | 7.984 / 51.210 ms |
| Acknowledged and audited holds | 5,400 / 5,400 |
| Overlapping seat intervals | 0 |
| Admission rejections | 0 |
| Undrained queues | 0 |

The direct RDS observer collected **1,043 samples** at a requested 0.2-second
interval with zero query errors. Its maximum query time was 5.749 ms and
maximum scheduling lag 3.017 ms. The bounded slow-DB log summary counted
seven commits above 100 ms on each API replica. Several coincided around
**03:17:56 UTC**; the longest per-replica commit was about 341–342 ms.
At 03:17:56.187 UTC, the RDS observer sampled **11 sessions waiting on
LWLock** and one on IO. These are temporally correlated observations, not
proof of which PostgreSQL wait event or operation caused the commit delay.
The observer at this revision recorded wait type but not wait-event name;
the next diagnostic revision adds aggregate event names. A zero WAL sync
time delta is inconclusive without confirming whether WAL timing is enabled.

The short 600 RPS pass supports starting a bounded 750 RPS **diagnostic**
with the refined observer. It does not promote 750 RPS to a validated
capacity level. The latest repeatable clean 30-minute baseline remains
600 RPS.

Evidence: [stage gates](stage-result.json),
[generator summary](load-summary.json), [post-TTL audit](durability.json),
[RDS wait summary](rds-waits-summary.json), and the four bounded
[slow-DB summaries](db-slow-0.json). Raw RDS samples, private manifests,
credentials and request logs remain outside the repository.
