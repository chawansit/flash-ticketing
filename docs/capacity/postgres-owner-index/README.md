# Active seat-owner index: matched Huawei validation

## Result

Commit `152414d` replaced the expiry path's two full scans of `event_seats`
with a partial `(hold_id, seat_id)` index. Under the same 400 RPS, 95%-read /
5%-hold, 30-minute workload used for the accepted `bea33ce` baseline,
PostgreSQL CPU fell from 86.34% to 30.78% of one core. Mean API transaction time
fell from 17.28 ms to 11.54 ms.

The candidate attempted 720,000 requests with zero generator drops. All 36,000
holds returned HTTP 201 and passed post-expiry durability and overlap checks.
One of 684,000 reads ended in a connection reset while waiting for response
headers, so the strict zero-unexpected-error gate failed and escalation to
500 RPS was stopped.

## Topology and workload

- Backend: Huawei `c6.xlarge.2`, 4 vCPU / 8 GiB.
- Generator: Huawei `c6.2xlarge.2`, 8 vCPU / 16 GiB.
- Private network, four generator processes, no client retry.
- 800 shows, 300 seats per show, 8,000 viewers.
- 400 offered HTTP RPS for 1,800 seconds: 95% conditional reads and 5% independent holds.
- One API worker, DB pool 12, admission 8, 120-second hold TTL and 5-second client/server keep-alive.
- API, PgBouncer, PostgreSQL, Redis, Kafka and background workers remained on the backend ECS.

## Matched comparison

| Measurement | Baseline `bea33ce` | Index `152414d` | Change |
|---|---:|---:|---:|
| Requests attempted | 720,000 | 720,000 | same |
| Read p95 | 13.052 ms | 9.937 ms | -23.9% |
| Hold p95 | 45.354 ms | 37.266 ms | -17.8% |
| Mean API DB transaction | 17.281 ms | 11.541 ms | -33.2% |
| Mean commit | 1.889 ms | 1.455 ms | -23.0% |
| PostgreSQL mean CPU | 86.339% | 30.776% | -64.4% |
| API mean CPU | 59.641% | 61.518% | +3.1% |
| Lock waiters | 0 | 0 | unchanged |
| Unexpected transport errors | 0 | 1 | strict failure |

Docker CPU uses 100% for one core. The samples do not establish a confidence
interval, but the query-plan and table-counter evidence identify the mechanism:
the pre-change owner select and update each planned a sequential scan at cost
5,112; after migration both plan an `event_seats_active_hold` scan at cost
8.14. During the candidate window, `event_seats` recorded 72,000 updates and
zero additional sequential scans.

## Database and cache evidence

The observer retained 872 in-window samples with no collection errors:

- maximum 13 database connections, four active and zero sampled lock waiters;
- maximum reconciliation age 20.597 seconds;
- maximum seven overdue holds and 0.232 seconds overdue age, all drained;
- zero missing seat maps and zero maps without TTL;
- 3,279,339 WAL records and 808,458,294 WAL bytes during the window.

Cumulative table deltas also expose remaining background cost. The simulator
performed 78,181 empty `payment_attempts` scans and maintenance performed
89,803 `seat_refresh_requests` scans. These are candidates for a later,
separately measured polling/index decision; they were not changed in this run.

## Transport failure

Worker 1 recorded one read failure at
`2026-09-12T13:00:10.124345Z`. It reused an existing connection, completed the
request send, then received `ConnectionResetError` errno 104 while waiting for
response headers after 19.34 ms. The API container had zero restarts, was
healthy, was not OOM-killed, and emitted no ASGI send-error/client-disconnect
counter or error log in the surrounding interval.

This evidence locates the reset below the ASGI application boundary. Equal
five-second client and server keep-alive expiry remains a plausible race, but
the current evidence does not prove cause. The failed result is retained and
will not be reclassified or hidden by retry.

## Correctness and decision

Post-expiry verification matched all 36,000 HTTP 201 acknowledgements to 36,000
idempotency records, holds and orders. Broken links, active holds, overdue
holds, pending orders and overlapping seat intervals were zero. Unpublished
outbox, pending refresh and dead-letter queues were zero.

ADR 0031 remains accepted because the index changes lookup complexity without
changing ownership semantics and its performance mechanism is directly
verified. The 400 RPS workload does not pass the strict production gate due to
the single transport reset. Do not run the planned 500 RPS stage until the
transport boundary is addressed and another 400 RPS control passes.

Generator summary, observer samples, CPU samples, table counters and the
durability report are retained in this directory. Credential manifests are
excluded.
