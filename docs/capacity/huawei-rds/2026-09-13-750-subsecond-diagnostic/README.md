# Huawei RDS 750 RPS sub-second diagnostic  2026-09-13

Status: **Failed the strict 30-minute gate. Sub-second evidence identifies a shared connection-pool microburst; the corrected run had no seat-map warming.**

This experiment follows the earlier 750 RPS boundary failure without changing the API topology, PostgreSQL authority, Redis ownership rules, admission limit, database connection budget or workload retry policy. It adds measurement and corrects premeasurement bootstrap timing only. It is not a production maximum or an RDS saturation claim.

## Fixed topology and workload

- Two API replicas behind Nginx `least_conn`.
- Six application DB connections and admission allowance eight per replica.
- PgBouncer and RDS connection budgets unchanged.
- Huawei RDS PostgreSQL 17.11, 4 vCPU / 16 GiB / 100 GB.
- 800 target shows, 300 seats per show, 8,000 viewers, 95% conditional reads and 5% unique holds.
- Four generator workers, five-second client keep-alive expiry, no workload retry.
- The database contained two active 800-show fixture sets during this test. Only the newest 800 were targeted, while proactive reconciliation tracked the full active window.
- Temporary `sslmode=require`; CA and hostname verification remain a production prerequisite.

## Harness correction

The old coordinator allowed 30 seconds between worker creation and measured start, equal to the seat-map TTL. A diagnostic run therefore produced 455 `SEATMAP_WARMING` reads as bootstrap-refreshed keys expired at the measurement boundary. The retained candidate adds bounded concurrent ETag bootstrap and an explicit start delay. The corrected run used 15 seconds. All four workers bootstrapped 200/200 shows with zero retry, and the measured window had zero missing maps and zero `SEATMAP_WARMING`.

This changes only premeasurement setup. Workload requests still have no retry. Focused load-tool and observer validation passed 25 tests; Ruff passed.

## Corrected 750 RPS result

| Metric | Result | Gate |
|---|---:|---|
| Duration | 30 minutes |  |
| Scheduled | 1,350,000 |  |
| Completed | 1,349,995 | Fail: 5 late drops |
| Read 200 / 304 | 1,282,495 | Pass |
| Holds 201 | 67,496 |  |
| Hold failures | 3 `ADMISSION_FULL`, 1 `DATABASE_UNAVAILABLE` | Fail |
| Worst read p95 | 10.353 ms | Pass |
| Worst hold p95 | 57.594 ms | Pass |
| Transport errors | 0 | Pass |
| Worker start skew | 5.134 ms | Pass |

The five generator drops were 50.71856.036 ms late. Generator host CPU was 17.484% at p95 and 21.635% maximum; the load processes used 136.003% of one CPU at p95 and 169.0% maximum. CPU, memory and IO pressure were zero, steal was zero and iowait peaked at 0.126%. The generator was not CPU-saturated, but the fixed zero-drop gate still failed.

## Confirmed backend correlation

The 200 ms observer captured 9,000 exact-window samples per replica with no scrape errors. Both replicas reached:

- hold inflight 8/8;
- pool in-use 6/6;
- two concurrent pool acquirers.

At 10:57:18.249 UTC both replicas simultaneously reported that state. Twelve milliseconds later, one hold returned `DATABASE_UNAVAILABLE` after `db_enter=150.518 ms`, matching the configured 150 ms application pool timeout. Its arrival occupancy was six, so the failure was pool acquisition rather than admission rejection. This is direct evidence of a shared DB/pool service-time spike.

At 10:58:39.249 UTC both replicas had pool 6/6 with one acquirer. At 10:58:39.283 UTC one replica rejected a new hold at occupancy 8/8. Across the full run, per-replica admission counters recorded three rejections in total.

The two-second backend observer saw at most 13 database connections, 11 active connections and zero lock waiters. On the observed replica, commit average was 3.542 ms and pool acquisition average was 27.924 ms. It recorded no requested checkpoint and no PostgreSQL WAL sync/write time increase exposed by the available counters. These averages do not contradict the sub-second spike.

The corrected exact window had zero missing maps, no maps without TTL and a minimum TTL of 11 seconds. Reconciliation age still reached 44.501 seconds across the active window, but it did not cause target-show read errors after the start-delay correction.

## Correctness

The post-expiry audit matched all 67,496 acknowledged holds to exactly 67,496 idempotency, hold and order records. Broken links, active/overdue holds after drain, pending orders and overlapping held-seat intervals were zero. Unpublished outbox events, pending refresh work and dead letters were zero. Zero double-booking was preserved.

## Decision

The highest clean demonstrated operating point remains **600 RPS for 30 minutes** for this topology and workload. The corrected 750 RPS run fails independently on backend availability and generator scheduling despite meeting latency and correctness targets.

Before changing the 150 ms pool timeout, per-replica pool size, admission allowance or topology, create an ADR and run one controlled comparison. The next experiment should export Huawei RDS provider CPU, IOPS and storage latency around the correlated spike, then compare a bounded pool-wait margin without increasing aggregate connections. Raising admission or connection budgets without that evidence may move queueing into RDS and increase failure amplification.

## Evidence

- `summary.json` and `worker-*.json`: request accounting, latency, errors, bootstrap and scheduling.
- `generator-window-summary.json`: exact-window generator CPU, pressure and failure examples.
- `pressure-window-summary.json`: 200 ms per-replica admission/pool peaks.
- `backend-window-summary.json`: exact-window database, Redis and cumulative API metrics.
- `failure-correlation.json`: bounded request/pressure timing correlation.
- `durability.json`: post-expiry persistence, overlap and queue audit.
- `api-warmup.json`: premeasurement target-show readiness.

Raw observers remain on the ECSs and are omitted from Git. Private manifests, tokens and database credentials are excluded.
