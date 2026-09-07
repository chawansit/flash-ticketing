# Database profiling and incremental seat projections

Date: 2026-09-07. Implemented under [ADR 0009](../../adr/0009-incremental-seat-projection.md).

## Outcome

Routine updates now read only affected seat rows and patch a Redis hash with per-seat
source-version fencing. Full reconciliation remains every five seconds, and only full
reconciliation extends the 30-second cache TTL. SQL ownership is unchanged.

The final isolated **3,000-seat** adapter probe measured **47.97 ms full-refresh p95**
versus **7.14 ms incremental p95** for one changed seat (about 6.7x lower). Each mode
ran 20 times, alternating order, using real PostgreSQL and Redis. SQL reads drop from
3,000 seat rows to one for this operation. This is refresh latency, not seat-map HTTP
latency or a production-capacity multiplier.

## Increasing offered load

All stages used a fresh 3,000-seat event, 30-second request phase, the same single API
process, pool/admission configuration, local Docker allocation and bounded host generator.
The baseline already contains the new instrumentation but uses the old full-refresh cache.

| Offered reservation RPS | Baseline success p95 | Final success p95 | Baseline/final successes | Baseline/final 503 | Baseline/final generator drops |
|---|---:|---:|---:|---:|---:|
| 10 | 154.7 ms | 50.4 ms | 300 / 300 | 0 / 0 | 0 / 0 |
| 50 | 155.9 ms | 145.8 ms | 1,472 / 1,469 | 24 / 20 | 4 / 11 |
| 100 | 858.1 ms | 973.4 ms | 1,616 / 2,160 | 136 / 57 | 1,248 / 783 |

An intermediate implementation measured 89.3 ms at 50 RPS, with two 503s and no drops.
The final confirmation did not reproduce that result. Keep both; do not claim a
repeatable 43% reservation-latency improvement. The final safeguard adds interrupted
Redis-write detection and full-repair behavior. The final 100-RPS stage still fails
latency and offered-load acceptance despite completing more requests.

All nine stages passed their business-invariant and projection-drain checks. Those
checks do not certify performance. Final stages drained in 0.203 / 0.235 / 0.843 seconds.
The existing 100-way real HTTP contention test had exactly one durable winner; no
booking duplicates occurred in the full suite. Spread probes reserve seats without
paying, so their zero booked duplicates alone is not a booking-race test.

## What the new measurements show

| Final offered RPS | Sampled max DB connections | API pool size max | API transaction p95 bucket upper bound | API pool acquisition p95 bucket upper bound | Sampled max lock waiters |
|---|---:|---:|---:|---:|---:|
| 10 | 14 | 2 | 50 ms | 5 ms | 0 |
| 50 | 14 | 8 | 75 ms | 5 ms | 1 |
| 100 | 14 | 8 | 75 ms | 5 ms | 0 |

The one waiting query sampled in the final 50-RPS run was 1.083 ms old; this is not
its exact lock-wait duration. Final stages recorded no pool acquisition failures or
55P03 lock errors in the scrape windows. API SQL-execute p95 bucket upper bounds were
5 ms. Brief waits can be missed. The API pool grows then plateaus, while backend
connections remain bounded; this does not prove 10x successful throughput because
requests were rejected and generator arrivals dropped.

At 100 offered RPS, final consumer busy time was about 21.1 seconds and publisher
busy time 19.1 seconds across the scrape-aligned stage/drain window. Busy time includes
I/O and idle-work queries. Snapshot time was 3.8 seconds; refresh time was 3.1 seconds.
Do not add nested operation times. Metrics also expose process CPU separately.
These observations point to investigating API/generator scheduling and remaining
background work before increasing database pools; they do not establish one sole bottleneck.

See [metric definitions and PromQL](../../observability.md) for query duration, pool
waits, SQLSTATE counters, worker utilization and sampled database blocking.

## Larger inventory boundary

The final adapter probe at 10,000 seats completed all 20 patches but two of 20 full
refreshes timed out. Successful full-refresh latency statistics exclude those failures
and must not be presented as an SLO pass. An earlier probe failed initial warming at
10,000 seats. Both probes failed initial warming at 50,000 seats under the application's
100 ms Redis socket timeout. Unknown acknowledgement does not imply Redis rolled back.

This implementation improves routine writes, but large cold snapshots still need
bounded reconciliation work. Full seat-map reads also remain O(inventory). No seat-map
HTTP p95 or large-event capacity claim is made here. Do not simply raise timeout values
and call the scaling problem solved.

## Validation and evidence

- Final container suite: **52 passed, zero skipped**, 11.66 seconds; two existing dependency deprecation warnings.
- Earlier suite: 50 container tests passed; a subsequent 51-test host suite passed before the final partial-write safeguard.
- New tests cover duplicate/out-of-order patches, concurrent source-version updates, expired/missing cache, dirty-generation overlap, crash after Redis write, interrupted-write repair, missed-notification reconciliation, actual SQL NOWAIT conflicts and pool timeout/recovery.
- Ruff passed. Migration 003 is additive and has been applied locally.
- [Final health](health.json): readiness 200, no unpublished/unconsumed events, dirty refreshes, dead letters or duplicate booked seats.
- [Summary](summary.json), [initial adapter probe](projection-cost.json), [final adapter probe](projection-cost-final.json), [final images](final-images.json).
- `before-*.json.gz`, `after-*.json.gz`, `final-*.json.gz` retain losslessly compressed raw results with metric labels, scrape timestamps and observations. The summary script reads both JSON and gzip.

Windows i7-7700, four physical/eight logical CPUs, approximately 16 GiB host RAM;
Docker allocation follows the [previous environment](../optimized/environment.json).
Other workloads and load generation share the host. Nine new events and expired
fixtures remain in the main database; periodic reconciliation therefore scans increasing
total inventory across stages. No customer data was reset to improve results. Global
metrics include earlier fixtures and expiry work. These are short diagnostic runs,
not a controlled production soak or a statistical causal estimate of API speedup.
The isolated adapter benchmark creates and removes its own schema/cache keys and runs
after, not concurrently with, API load stages.

## Reproduce

Use the development database/Redis environment variables from the existing runbook.

```powershell
.venv/Scripts/python.exe scripts/capacity_test.py --profile spread --rate 50 --seconds 30 --seats 3000 --drain 60 --wait-projection --output new-result.json
.venv/Scripts/python.exe scripts/summarize_capacity.py docs/capacity/incremental
.venv/Scripts/python.exe scripts/benchmark_projection.py
```

For the adapter probe use a dedicated Redis test database (for example /1); its SQL
schema is isolated. For production sizing use a separate generator, stable background
inventory, longer repeated stages and explicit latency/error/queue-growth gates.
