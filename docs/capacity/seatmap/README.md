# Cinema seat-map read optimization

Date: 2026-09-07. [ADR 0010](../../adr/0010-conditional-seatmap-reads.md),
[endpoint/client contract](../../seatmap-reads.md).

## Implemented

- Separate `/layout` and `/availability` representations. Layout contains existing
  seat IDs and prices, not invented physical coordinates, and supports one-hour public caching.
- Availability ETags checked atomically in Redis. Unchanged reads return **304** without
  loading seat fields. Compact 200 responses omit price and source-version internals.
- Cache-incarnation validators prevent old ETags matching a recreated map. Missing or
  interrupted caches return warming; Redis failures return 503, without SQL fallback.
- Existing full-map and delta endpoints remain compatible. No frontend, SSE, indexed
  delta reads, active-show scheduling or location/screen hierarchy was added in this step.

## Read-heavy comparison

A fixed set of **100 shows x 300 seats = 30,000 seats**, with **1,000 simulated viewers**
maintaining independent validators. Each 30-second stage schedules **95% reads and 5%
unique-seat holds**. Total offered RPS includes both operations; 50 total RPS schedules
47.5 read RPS. Fresh fixtures are retained. Each stage prewarms maps and viewer validators;
layout/bootstrap traffic is excluded. The client does not implement adaptive polling yet.

| Total offered RPS | Endpoint | Read p95 | Read 200 / 304 | Successful holds | Generator drops | Read transport errors |
|---|---|---:|---:|---:|---:|---:|
| 50 | Legacy full map | 79.0 ms | 1,425 / 0 | 75 | 0 | 0 |
| 50 | Conditional availability | **37.2 ms** | 284 / 1,141 | 75 | 0 | 0 |
| 100 | Legacy full map | 1,186.8 ms | 1,679 / 0 | 84 | 1,237 | 0 |
| 100 | Conditional availability | 199.7 ms | 716 / 1,996 | 144 | 144 | 0 |
| 200 | Legacy full map | 831.1 ms | 1,777 / 0 | 105 | 4,117 | 1 |
| 200 | Conditional availability | 231.6 ms | 871 / 1,595 | 129 | 3,405 | 0 |

At the clean 50-RPS point, read p95 is approximately **53% lower**, and read response
bodies shrink from **44.85 MB to 5.29 MB** (about **88% lower**, decimal MB). Both stages
complete 1,425 reads and 75 holds without errors/drops. About 80% of conditional reads
return 304. Header bytes, TLS, compression and layout bootstrap are not included.

Only the two 50-RPS stages pass the provisional read gate: p95 <=150 ms, zero generator
drops, no failed reads/holds. Higher stages fail despite better relative performance.
They cannot establish sustainable 100/200 RPS or the server-only maximum. A single
transport failure in the legacy 200-RPS run is retained. The script records comparison
results even when a stage fails its gate; exit success is not a capacity certificate.

## Method and limits

Both endpoints run on the same new application build, not different pre/post builds.
The legacy endpoint still loads/serializes the full map; its Redis HGETALL also sees the
new layout metadata, so this is a comparison of supported endpoints on this build.
The order alternates by rate: legacy/conditional at 50, conditional/legacy at 100,
legacy/conditional at 200. Inventory stays fixed across all six stages; reservations
use different seats. Earlier holds expire normally and background reconciliation continues.
No separate tests or benchmarks ran during measured phases.

Each observation sample also collects selected Prometheus series every 2 seconds:
`ticketing_db_.*`, `ticketing_worker_.*`, and `process_cpu_seconds_total`,
so latency changes can be correlated to lock waits, pool pressure, and worker utilization.


The generator is bounded to 64 in-flight requests and drops late arrivals instead of
building an unbounded queue. It shares the Windows i7-7700 development machine and
Docker resources with the services and other workloads. The setup also contains earlier
retained fixtures; maintenance experienced a Redis timeout during the experiment.
The observer does not isolate how much time is due to generator scheduling, API thread
queues, logging or Redis. These are short diagnostic comparisons, not repeatability
or production-HA evidence. Full inventory reconciliation and full/delta read costs
remain scaling work. No 800-2,000-screen production-capacity claim is made.

## Validation

**56 tests passed, zero skipped**, in the container suite (14.88 seconds); two existing
dependency deprecation warnings. New real Redis/HTTP tests cover 304 body/header semantics,
weak/list/wildcard validators, layout stability during hold/release, version/body atomicity,
cache loss, interrupted writes and outage handling. Existing 100-way seat contention,
payment idempotency and worker recovery tests remain passing. Ruff passed.

[Raw stage results](read-heavy.json) include per-status client/server timing, body bytes,
viewer/show counts and generator drops. [Post-run verification](verification.json) checks
durable holds against successful responses and service/queue health.

## Reproduce

Set development TEST_DATABASE_URL and TEST_REDIS_URL as in the runbook, then:

```powershell
.venv/Scripts/python.exe scripts/seatmap_load.py --rates 50 100 200 --seconds 30 --shows 100 --viewers 1000 --prometheus http://127.0.0.1:9090 --output new-read-comparison.json
```

It creates development show fixtures and retains them; no existing inventory is reset.

To include DB/pool/worker metrics, add `--observe`:

```powershell
.venv/Scripts/python.exe scripts/seatmap_load.py --observe --rates 50 100 200 --seconds 30 --shows 100 --viewers 1000 --prometheus http://127.0.0.1:9090 --output new-read-comparison-observed.json
```

After the run, summarize snapshots:

```powershell
.venv/Scripts/python.exe scripts/summarize_seatmap_load.py new-read-comparison-observed.json
```
The fresh-schema domain currently represents each scheduled show using `events`.
