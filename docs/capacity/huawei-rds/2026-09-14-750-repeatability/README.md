# Huawei RDS 750 RPS repeatability control - 2026-09-14

Status: **Failed and stopped at the first strict-gate admission rejection.**

This fresh control repeated the final ADR 0038 topology and workload: four API
replicas, database pool three and hold admission four per replica, PgBouncer pool
24, 800 shows, 95% conditional reads, 5% unique-seat holds and four no-retry
generator workers. It tests whether the earlier clean 750 RPS result is
repeatable; it is not a maximum-capacity stage.

## Result

All 800 seat maps warmed on the first attempt. Measured load began at
03:57:17.863 UTC. One replica recorded one `ADMISSION_FULL` response at
03:57:20.505 UTC, about three seconds into the measured stage. The run was
terminated rather than allowed to continue through a failed zero-error gate.
Worker progress files had reached 225,000 scheduled requests with zero drops at
their last checkpoints; final request summaries do not exist because termination
was intentional.

The affected replica reached four admitted holds, three checked-out database
connections and one local acquirer. A PgBouncer sample at 03:57:19.898 UTC,
0.607 seconds before the rejection sample, showed one waiting client, fourteen
active servers, no idle servers and a 47.323 ms maximum wait. The nearest sample
afterward showed no queue.

| Replica | Arrivals | Admission rejects | Pool acquire avg | Connection hold avg | Transaction avg | Commit avg |
|---|---:|---:|---:|---:|---:|---:|
| API 0 | 2,332 | 1 | 0.090 ms | 28.265 ms | 28.194 ms | 4.376 ms |
| API 1 | 2,272 | 0 | 0.058 ms | 28.052 ms | 27.976 ms | 4.100 ms |
| API 2 | 2,286 | 0 | 0.037 ms | 28.128 ms | 28.055 ms | 4.110 ms |
| API 3 | 2,343 | 0 | 0.156 ms | 28.557 ms | 28.484 ms | 4.474 ms |

Across 1,771 PgBouncer samples, one sample had a waiting client and server
activity peaked at fourteen. PostgreSQL observation recorded eighteen connections
at peak, eight active connections, zero lock waiters and zero missing seat maps.
This supports a transient request-distribution and pooler burst rather than
sustained RDS saturation.

## Correctness after the stop

After the hold TTL elapsed, the partial audit matched 11,621 idempotency records
to 11,621 distinct holds and orders. Broken links, active and overdue holds,
pending orders and overlapping held-seat intervals were zero. Outbox, seat-map
refresh and dead-letter queues were empty.

The prior 750 RPS run remains a valid demonstrated result, but this control shows
that admission four per replica does not provide a repeatable zero-error boundary.
ADR 0039 records the next bounded candidate before deployment. No 800 RPS stage
is allowed until 750 RPS passes both safety and 30-minute confirmation gates.

## ADR 0039 admission-five safety candidate

The admission-five candidate completed all 450,000 scheduled requests with zero
generator drops, transport errors or admission rejections. Worst-worker
read/hold p95 was 8.955/55.158 ms. The workload gate nevertheless failed on three
`RESOURCE_BUSY` responses: 22,497 holds returned 201 and three returned 409.
Those seats belonged to the previously exercised 800-show fixture, so this run
does not isolate the admission change from historical synthetic-seat state.

The exact-run post-TTL audit matched all 22,497 acknowledged holds to their
idempotency, hold and order records. Broken links, active or overdue holds,
pending orders and overlapping intervals were zero, and all queues drained.
Admission was restored to four per replica as required by ADR 0039. The next
candidate requires a newly created 800-show fixture rather than another seat
offset within the existing shows.
## Evidence

- `api-warmup.json`: 800/800 maps ready on the first pass.
- `partial-summary.json`: redacted API, PgBouncer and PostgreSQL window summary.
- `generator-progress.json`: last per-worker progress checkpoints and operator-stop status.
- `durability-drained.json`: post-TTL relational, overlap and queue audit.

Raw sub-second observers remain on the private ECSs and are omitted from Git.
Private manifests, tokens, database connection strings and credentials are
excluded.
