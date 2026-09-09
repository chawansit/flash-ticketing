# Huawei private-network capacity results

Status: **30-minute run completed; 352 RPS failed the zero-error acceptance gate.**
This is a measured limit under the tested workload, not a verified production maximum.

## Sustained result

Measured window: **2026-09-08 16:54:19–17:24:20 UTC** (`soak3-352`).
Four independent generator workers offered 352 RPS for 1,800 seconds.

| Measurement | Result |
|---|---:|
| Total completed HTTP requests | 633,600 |
| Availability reads, all HTTP 200/304 | 601,920 |
| Successful holds, HTTP 201 | 31,660 |
| Rejected holds, HTTP 503 | 20 / 31,680 (0.0631%) |
| Errors across all requests | 0.00316% |
| Generator drops / transport errors / task errors | 0 / 0 / 0 |
| Worst-worker read p95 | 24.657 ms |
| Worst-worker hold p95 | 71.163 ms |
| Worker start skew | 3.967 ms |
| Read ≤150 ms and hold ≤300 ms p95 gates | Passed |
| Zero-unexpected-response gate | **Failed** |

Percentiles are worst-worker values, not averaged or exact merged percentiles.
The hold percentile includes fast failed responses; per-status distributions are
preserved in each worker result. All four workers completed full request accounting.

[Worker summary](soak3-352/summary.json), [worker 0](soak3-352/worker-0.json),
[worker 1](soak3-352/worker-1.json), [worker 2](soak3-352/worker-2.json),
[worker 3](soak3-352/worker-3.json).

## Topology and workload

- Backend: c6.xlarge.2, **4 vCPUs / 8 GiB**; API, PostgreSQL, PgBouncer, Redis,
  Kafka and workers share one ECS.
- Generator: c6.2xlarge.2, **8 vCPUs / 16 GiB**, a separate ECS.
- Direct private IPv4 HTTP between the hosts; SSH is administration only.
- 800 shows × 300 seats, 8,000 logical viewer validators; 95% conditional
  availability reads and 5% unique-seat holds. No payment throughput in this load.
- Four generator processes partition shows/viewers, each offering 88 RPS with
  at most 64 in-flight tasks. A common future start coordinates measurement.
- Viewer identities are not 8,000 simultaneously open HTTP requests.
- Application runtime and settings match the prior isolated cloud baseline:
  one API process, hold admission limit 8, DB pool 12, PgBouncer 40, Redis 256 MiB
  noeviction, seat-map TTL 30 s, reconciliation interval 20 s, batch 8 and budget 500 ms.
- Ubuntu 24.04, development-mode synthetic token issuance, no customer data.
  This does not qualify production security, HA, a load balancer, or managed services.

See [ADR 0014](../../adr/0014-private-cloud-capacity-benchmark.md) and the
[previous cloud baseline](../huawei-single-node/README.md).

## Backend timing and failure evidence

[Collected analysis](soak3-analysis.json) derives from
[Prometheus samples](soak3-prometheus.json) and
[two-second observations](soak3-observations.json).

| Backend measurement | Observed result |
|---|---:|
| API DB query mean / p95 bucket upper bound | 1.228 ms / 5 ms |
| API DB transaction mean / p95 bucket upper bound | 19.324 ms / 50 ms |
| API pool acquisition mean / p95 bucket upper bound | 0.0236 ms / 5 ms |
| Maximum sampled database connections | 13 |
| Maximum sampled lock waiters | 0 |
| Missing seat maps in 890 in-window samples | 0 |
| Maximum sampled reconciliation age | 28.018 s |
| Maximum sampled overdue hold cleanup delay | 0.754 s |

Histogram bounds are coarse, not exact percentile measurements. DB query timing
includes client/network/pooler time. Two-second sampling can miss brief lock waits.
All five monitored targets were up in every exported 15-second sample.
Reconciliation batch work occupied about 27.2% of the maintenance worker's elapsed
wall time; snapshot work about 18.3%. These operations may nest and include I/O:
do not sum them or interpret them as CPU utilization. Resource snapshots are in
[the container resource log](soak3-resources.log).

[API evidence](soak3-api-evidence.json) contains all 20 measured HTTP 503 responses
and 33 holds exceeding 300 ms. Some successful holds took around 700 ms during
short rejection bursts. Immediate rejection, absence of a corresponding business
error outcome counter, and the middleware implementation are consistent with
`ADMISSION_FULL` (limit 8). The generator did not retain response error bodies,
so this remains a supported inference, not direct error-code confirmation.
No database-pool saturation or missing-map incident was established in this run.

## Durability and correctness

Executed after load, [read-only verification](soak3-durability.json) matched each
worker run separately: all **31,660 acknowledged holds** have distinct persisted
holds and orders with matching ownership and relationships. No active/overdue
run holds or pending run orders remained. Global snapshots showed zero unpublished
outbox events, pending refresh requests and dead letters. Verification occurred
hours after completion when the task resumed; it is not a precise expiry-latency
measurement. The observer provides the in-run cleanup-delay samples above.

[End-to-end tests](soak3-correctness.log): **2 passed**, executed against the real
cloud API/PostgreSQL/Redis/Kafka stack after load:

1. 100 simultaneous HTTP contenders for one seat: exactly one successful and
   durable hold/order.
2. Payment with three callback deliveries: order fulfilled with one ticket.

These demonstrate the tested integrity scenarios; they do not prove every possible
failure interleaving or high-rate payment throughput. Ruff checks passed for the
five changed/new benchmark and verification scripts. The full application suite
was not rerun for this measurement-only change.

## Earlier stages and invalid attempts

| Stage | Read p95 | Hold p95 | Outcome |
|---|---:|---:|---|
| Single-process 100 RPS, 180 s | 5.411 ms | 12.329 ms | Passed |
| Single-process 200 RPS, 180 s | 6.190 ms | 16.591 ms | Passed |
| Single-process 400 RPS, 180 s | 160.016 ms | 173.788 ms | 12,618 generator drops; not a backend maximum |
| Four-process 400 RPS, 180 s | 26.303 ms | 75.573 ms | Four availability 503s; failed |
| Four-process 300 RPS, 180 s | 10.925 ms | 35.030 ms | Passed |
| Four-process 352 RPS, 180 s | 13.881 ms | 52.115 ms | Passed |
| Four-process 352 RPS, 1,800 s | 24.657 ms | 71.163 ms | 20 hold 503s; failed |

At the short 400-RPS failure, reconciliation age reached 30.806 s and an observer
sample found nine missing maps, consistent with refresh falling behind the 30 s TTL.
That is a different observed failure from the sustained run's hold rejections.
`baseline-100.json` and `stage-100.json` refer to the same completed run; do not double count.

Host restarts interrupted the first 200-RPS attempt and the first long attempt
(`soak-352`); neither is a completed passing stage. `soak2-352` then failed bootstrap
with availability 503 before timed load, after Redis restarted empty. Readiness
alone does not guarantee all seat maps are warm. Its four read 503s are excluded
from `soak3-352`, whose measured reads all succeeded. Failed/partial artifacts are
retained separately. The successful-duration run began only after all fixture maps
were observed present. No runtime tuning was applied during measurement.

## Interpretation and next work

**352 RPS is demonstrated throughput with occasional hold rejection, not a
zero-error production capacity commitment.** The 300-RPS result is only three
minutes; it needs its own sustained run before being treated as a lower qualified
operating point. Do not extrapolate these results to 100,000 RPS or to 200-location
simultaneous on-sale traffic: this workload is uniformly distributed, without a
simulcast hot-show skew, retry waves, payment load, or HA failover.

Next, capture explicit admission outcomes/in-flight occupancy and correlate short
slow-hold bursts with DB/host metrics; benchmark one change at a time. Merely raising
admission concurrency can transfer congestion to the database. Separately address
cache freshness margin and run skewed simulcast tests. Architectural changes need
an ADR before implementation. [Claude review](../../reviews/claude-simulcast-review.md)
records recommendations and caveats; those runtime changes have not been applied.

## Cleanup and reproducibility

Synthetic credentials were removed from both hosts and the API container; they
are excluded from this directory. The benchmark stack was stopped, synthetic
volumes preserved, and the temporary generator-only forwarding restriction removed
only after shutdown. [Shutdown log](soak3-stopped.log). API automatic restart remains
disabled so a future restart requires guarded preparation. No cloud resources were
purchased or security-group rules changed.

Harnesses: `scripts/http_load_generator.py`, `scripts/parallel_cloud_load.py`,
`scripts/private_cloud_stages.py`, `scripts/export_cloud_metrics.py`, and
`scripts/verify_cloud_holds.py`. Use fresh private manifests, wait for fixture
readiness, preserve separate output directories, and run durability checks after
expiry. Bootstrap is outside timing. Retain failed results without changing gates.
