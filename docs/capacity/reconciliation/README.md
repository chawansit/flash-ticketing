# Reconciliation scheduling measurements (external handoff)

Integration note: numerical results below were supplied by Claude on a different host and the original patch, not measured on this computer. The integrated implementation checks deadlines before each snapshot and counts successful acknowledgements separately. Throughput multiplied by TTL is not a safe active-show ceiling: the supplied 6,000-show run already breached TTL.

Local measurements of the bounded reconciliation scheduler introduced by
[ADR 0011](../../adr/0011-bounded-reconciliation-scheduler.md). These are not production
capacity figures. Read the limitations section before quoting any number.

## Environment

| Item | Value |
| --- | --- |
| Host | Single container, **1 vCPU**, 3,997 MB RAM |
| PostgreSQL | 16.15 (Ubuntu), local socket/TCP, default configuration |
| Redis | 7.0.15, `--save '' --appendonly no` |
| Python | 3.12.3, dependencies from `requirements.lock` |
| Harness | `scripts/benchmark_reconciliation.py` |
| Date | 2026-09-08 |

Everything below ran against real PostgreSQL and real Redis in one container. Database,
worker and cache therefore contend for the same single core. No Kafka, no API and no
offered HTTP load ran concurrently, so these are isolated scheduler measurements.

## Method

Each run creates a private schema, applies all migrations and seeds N events of 300 seats
with sale windows open. Three phases follow.

1. **Legacy full sweep** — the behaviour being replaced: `SELECT id FROM events` with no
   filter, then `snapshot()` for every row, measured as one uninterrupted pass.
2. **Saturated** — the scheduler driven with `RECONCILE_INTERVAL_SECONDS=1` so events fall
   due again immediately. This measures the scheduler ceiling.
3. **Steady state** — the same scheduler at the default 20-second interval, sampling
   backlog, overdue age and time since last reconciliation twice per second.

The harness refuses to derive capacity from a demand-limited run. With N tracked events and
interval I, no worker can exceed N/I reconciliations per second; if the measured rate reaches
90% of that ceiling the run reports `demand_limited: true` and emits no derived figures. The
first attempt at 50 shows was discarded for exactly this reason.

## Results

### Legacy full sweep (the inline work being removed)

| Shows × seats | Seat rows | Sweep wall time | Per-event p50 | p95 | Failures |
| --- | --- | --- | --- | --- | --- |
| 50 × 300 | 15,000 | 0.168 s | 3.15 ms | 5.17 ms | 0 |
| 1,000 × 300 | 300,000 | 3.088 s | 3.05 ms | 3.48 ms | 0 |
| 6,000 × 300 | 1,800,000 | **19.75 s** | 3.22 ms | 3.92 ms | 0 |

No sweep timed out on this machine, so the old loop is not reported as failing here. The
measured problem is different and structural: the sweep is O(total retained inventory) and
ran **inline** in the maintenance loop, so at 6,000 shows hold expiry and dirty-seat refresh
were blocked for 19.75 seconds per pass while the loop's own target period was 5 seconds.

### Scheduler, 1,000 shows × 300 seats

| Metric | 1 worker | 2 workers |
| --- | --- | --- |
| Saturated throughput | 237.96 events/s | 243.55 events/s |
| Saturated, demand-limited? | no | no |
| Work split across workers | 7,256 | 3,728 / 3,696 |
| Steady-state split | 2,000 | 999 / 1,001 |
| Steady-state peak backlog | 1,000 | 1,000 |
| Steady-state peak overdue | 4.07 s | 4.10 s |
| Steady-state peak age since last reconciliation | 20.12 s | 20.06 s |
| Never reconciled | 0 | 0 |
| Failures | 0 | 0 |
| Pass duration p95 (steady state) | 1.10 ms | 2.18 ms |

Two workers divided the queue almost exactly evenly with no event processed twice, which is
the fairness and mutual-exclusion result. Throughput did **not** improve, because the host
has one vCPU. **These runs say nothing about horizontal scaling.**

### Scheduler under overload, 6,000 shows × 300 seats, 1 worker

| Metric | Value |
| --- | --- |
| Saturated throughput | 209.06 events/s |
| Steady-state throughput | 222.71 events/s (not demand-limited: demand needs 300/s) |
| Peak backlog | 5,999 |
| Peak overdue age | 26.93 s |
| Peak age since last reconciliation | **45.28 s** |
| Never reconciled during the 40 s window | 0 |
| Failures | 0 |
| Worst consecutive failures | 0 |

This is the designed degradation and it is deliberately included rather than tuned away.
6,000 shows exceeds one worker's ceiling at a 20-second interval, so deadlines slip: the
oldest map went 45.28 s without a full snapshot, past the 30-second Redis TTL. Requirement:
**do not claim every map stays fresh at this size.** It does not.

What did hold under overload is fairness. Every one of the 6,000 shows was reconciled at
least once inside the 40-second window (`never_reconciled: 0`), so staleness degraded
uniformly instead of some shows never being served. That is the specific failure the
unbounded sweep produced when a timeout cut a pass short.

### Derived capacity

From the saturated runs on this machine only:

| Source run | Throughput | Shows within the 20 s interval | Throughput x 30 s (not TTL-safe capacity) |
| --- | --- | --- | --- |
| 1,000 shows, 1 worker | 237.96/s | 4,759 | 7,138 |
| 1,000 shows, 2 workers | 243.55/s | 4,871 | 7,306 |
| 6,000 shows, 1 worker | 209.06/s | 4,181 | 6,271 |

Throughput falls as inventory grows, so the conservative figure is the 6,000-show run:
roughly **4,200 concurrently active 300-seat shows per maintenance worker** at the default
interval on this hardware. The arithmetic 6,300 figure must not be used as a TTL-safe limit; the observed 6,000-show run breached TTL.

## Cost of bounded fairness

The legacy sweep reached 6,000 / 19.75 s ≈ 304 events/s. The scheduler reached 209–238
events/s on the same inventory: roughly **30% lower raw throughput**. The scheduler pays two
extra short transactions per event (bounded claim, token-fenced acknowledgement) to buy
fairness, crash recovery, multi-worker safety and failure isolation. That trade is the point
of the change, but it is a real cost and is not presented as a speedup.

Pass duration p95 under saturation was 535–565 ms against a 500 ms budget, confirming the
documented bound: overrun is limited to one in-flight batch (8 events), not one full sweep.

## Limitations

- Single container, one vCPU, one machine, one run per configuration. No repetition,
  no confidence intervals, no warm-up isolation.
- Database, Redis and worker share the core, so worker throughput and database service time
  are not independent here. A production deployment separates them.
- The two-worker run demonstrates correct work division only. Scaling was not measured and
  must not be inferred.
- No concurrent HTTP reservation load, Kafka traffic or hold-expiry pressure. Requirement 6
  (reconciliation must not starve hold expiry) is enforced by the wall-clock budget and
  covered by `test_pass_is_bounded_by_its_wall_clock_budget`; it was **not** measured under
  simultaneous offered load.
- Seat inventory is static, all seats AVAILABLE. Real maps carry holds and bookings, which
  changes row width and Redis payload size.
- Sale windows were uniformly open. Real schedules cluster, so instantaneous active-show
  counts will spike above the daily average.
- Derived show counts are arithmetic from measured throughput, not observed at that size.
  The largest inventory actually exercised was 6,000 shows / 1.8 M seat rows.
- ADR 0009 recorded cold full snapshots exceeding the 100 ms Redis adapter timeout at 10,000
  and 50,000 seats. This work does not address that; it bounds how many 300-seat maps are
  scheduled, not how large a single map may be.

## Reproduce

```bash
export TEST_DATABASE_URL=postgresql://ticketing:ticketing@127.0.0.1:5432/ticketing
export TEST_REDIS_URL=redis://127.0.0.1:6379/0
python scripts/benchmark_reconciliation.py --shows 1000 --seats 300 --seconds 30 --workers 1
python scripts/benchmark_reconciliation.py --shows 1000 --seats 300 --seconds 30 --workers 2
python scripts/benchmark_reconciliation.py --shows 6000 --seats 300 --seconds 40 --workers 1
```

Raw output is retained in `shows-1000-worker-1.json`, `shows-1000-worker-2.json`,
`shows-6000-worker-1.json` and `legacy-sweep-6000.json`.
