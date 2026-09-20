# 750 RPS wait-event diagnostic — 2026-09-20

Status: **Passed this three-minute diagnostic's safety gates.** This is not
a 750 RPS production-capacity qualification. Both Huawei ECSs ran clean
checkout commit ad60160. Four API replicas used admission five during the
run; the unattended workflow restored admission four, audited after hold
expiry, and removed private manifests.

| Metric | Result |
|---|---:|
| Offered load | 750 RPS for 180 seconds |
| Completed responses | 135,000 |
| Unexpected responses / generator drops / transport errors | 0 / 0 / 0 |
| Worst-worker read / hold p95 | 7.674 / 47.791 ms |
| Acknowledged and audited holds | 6,750 / 6,750 |
| Overlapping seat intervals | 0 |
| Admission rejections / undrained queues | 0 / 0 |

The direct RDS observer collected **1,043 samples** at a requested
0.2-second interval with zero query errors. Its highest query time was
7.141 ms and highest scheduling lag was 4.793 ms. It sampled up to seven
sessions waiting on the LWLock event **WALWrite** and one on the IO event
**WalSync** at 03:32:11.407 UTC. At 03:32:14.407 UTC it sampled five
WALWrite and one WalSync waits; API 0 and API 2 then logged successful
commits of **166.781 ms** at 03:32:14.511 UTC and **160.559 ms** at
03:32:14.505 UTC. API 2 also logged one 101.546 ms commit at
03:32:11.423 UTC. The other two APIs logged no commits above the
100 ms threshold.

PostgreSQL 17 describes WALWrite (LWLock) as waiting for WAL buffers to be
written, and WalSync (IO) as waiting for a WAL file to reach durable storage.
These observations support a **WAL-path contention hypothesis**, but
sampled waits and client-observed commit times do not isolate the cause
or measure fsync duration. RDS reported track_wal_io_timing **off**, so
zero WAL write/sync timing deltas are not evidence of zero time spent.
The [previous 600 RPS rehearsal](../2026-09-20-600-instrument/README.md)
also saw a synchronized short commit spike and LWLock waits, but its
observer did not yet record wait-event names. The earlier
[750 RPS ten-minute stage](../2026-09-20-750-detached-failure/README.md)
failed strict availability with five hold 503 responses. Its failure
still governs capacity promotion.

The next step is to measure WAL write/sync time and PgBouncer/API phase
timings in the same subsecond window, then evaluate an explicit
hypothesis. Do not weaken synchronous durability or increase the pool
timeout based on this diagnostic alone. Any transaction or scaling
pattern change requires an ADR before implementation. A new 750 RPS
ten-minute safety run and 30-minute confirmation are still required
before claiming that rate as production capacity. The highest
repeatable clean 30-minute baseline remains 600 RPS.

Evidence: [stage gates](stage-result.json),
[generator summary](load-summary.json),
[post-TTL audit](durability.json),
[RDS waits](rds-waits-summary.json),
[rollback](rollback.json), [admission](admission.json), slow-DB timing
for [API 0](db-slow-0.json), [API 1](db-slow-1.json),
[API 2](db-slow-2.json), [API 3](db-slow-3.json), and bounded
[API error summary](api-errors-0.json). The direct RDS observer used
encrypted transport without CA verification solely for diagnostic
evidence collection. Raw samples, private manifests and credentials
remain outside the repository.

PostgreSQL references:
[wait event definitions](https://www.postgresql.org/docs/17/monitoring-stats.html)
and [WAL timing setting](https://www.postgresql.org/docs/17/runtime-config-statistics.html).
