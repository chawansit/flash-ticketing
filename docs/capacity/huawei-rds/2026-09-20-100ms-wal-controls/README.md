# 100 ms RDS WAL controls — 2026-09-20

Status: **Both three-minute diagnostics passed their safety gates.** Neither
qualifies 750 RPS as sustained production capacity. The earlier 750 RPS
10-minute stage still failed strict availability with five hold 503 responses;
the highest repeatable clean 30-minute baseline remains 600 RPS.

Both stages used the same Huawei API ECS, generator ECS, RDS instance, four API
replicas, admission candidate 5 with rollback to 4, and source commit
`d5ff3e37733fa0cb20c329a8313e143a5d5618fe`. Each used a fresh isolated
800-show/300-seat fixture, 95% conditional seat-map reads, 5% unique-seat
holds, four generator workers and no retries. The workflow verified readiness,
post-TTL durability, zero overlapping hold intervals, drained queues and
rollback. The RDS observer queried aggregate statistics every 100 ms; it did
not change any RDS parameter.

| Result | 600 RPS / 180 s | 750 RPS / 180 s |
|---|---:|---:|
| Completed responses | 108,000 | 135,000 |
| Acknowledged and audited holds | 5,400 | 6,750 |
| Unexpected responses, generator drops, transport errors | 0 | 0 |
| Worst-worker read p95 | 7.017 ms | 7.705 ms |
| Worst-worker hold p95 | 48.111 ms | 52.382 ms |
| Overlapping seat intervals / undrained queues | 0 / 0 | 0 / 0 |
| RDS samples / query errors | 2,079 / 0 | 2,080 / 0 |
| Max observer query / wake lag | 8.377 / 2.851 ms | 10.050 / 3.790 ms |
| Successful API commits logged above 100 ms | 4 | 31 |
| Longest logged commit | 126.664 ms | 322.913 ms |
| WAL bytes generated during observed window | 160,788,216 | 87,014,836 |
| WAL-buffer-full increments / checkpoint increments | 0 / 0 | 0 / 0 |

At **05:04:08 UTC**, the 600 RPS run logged 122–127 ms commits on all four
APIs. In the sampled surrounding seconds, RDS had up to seven `WALWrite`
LWLock waiters and one `WalSync` IO waiter. PgBouncer had zero waiting clients;
API pool acquisition was not backed up and event-loop lag was at most 2.9 ms.

At **05:12:10 UTC**, the 750 RPS run logged a cluster of successful commits,
with the longest at 322.913 ms. RDS sampled 8, 7, 11, 11 and then **15**
`WALWrite` LWLock waiters at consecutive 100 ms points from 05:12:10.483 to
05:12:10.883, each with one `WalSync` IO waiter. PgBouncer briefly had four
waiting clients and a maximum sampled wait of 2.35 ms; API event-loop lag was
below 1 ms in that second. No WAL-buffer-full event or checkpoint appeared in
this window or over either stage. The PgBouncer wait is too small to explain
hundreds of milliseconds of client-observed commit time on its own.

These repeated, synchronized commit spikes and WAL wait events support a
**WAL-path contention hypothesis**. They do not identify whether storage
fsync, WAL-file handling, RDS internals or another shared resource initiated
the stall. `track_wal_io_timing` was **off** on the live PostgreSQL 17.11
instance, and the supplied Huawei default-template exports for PostgreSQL 17
and 18 do not expose it. Their `track_io_timing=on` and `log_checkpoints=on`
entries are different settings; the exports describe defaults, not live values.
Zero `wal_write_time`/`wal_sync_time` with WAL timing off is inconclusive.
WAL and checkpoint counters are instance-wide and may include unrelated work.
The 100 ms observer can still miss shorter waits and adds a small read load.

Do not change `wal_buffers`, checkpoint thresholds, synchronous durability,
connection timeouts or admission on this evidence alone. The next external
measurement needed is provider-side, subsecond WAL flush/fsync or underlying
storage-latency telemetry around **2026-09-20 05:04:08 UTC** and
**05:12:10–05:12:11 UTC**, plus any RDS storage or failover events. Then
record a falsifiable hypothesis and an ADR before changing a persistence or
scaling pattern. Only after a justified mitigation should a fresh 750 RPS
10-minute safety stage and 30-minute confirmation precede any 800 RPS test.

Evidence: [600 stage](600/stage-result.json), [750 stage](750/stage-result.json),
[600 RDS waits](600/rds-waits-summary.json),
[750 RDS waits](750/rds-waits-summary.json),
[600 durability](600/durability.json), [750 durability](750/durability.json),
[600 rollback](600/rollback.json), [750 rollback](750/rollback.json), and
per-API slow-commit summaries in the corresponding directories. Full raw
samples and private manifests remain outside the repository.

PostgreSQL 17 references: [WAL and checkpointer counters](https://www.postgresql.org/docs/17/monitoring-stats.html),
[WAL timing setting](https://www.postgresql.org/docs/17/runtime-config-statistics.html).