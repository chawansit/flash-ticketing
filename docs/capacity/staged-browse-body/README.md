# Staged capacity: 400 RPS sustained, 500 RPS rejected requests

**400 RPS passed a 30-minute zero-error run. 500 RPS failed the five-minute
zero-error gate with 45 admission rejections.** This is the highest passing tested
operating point, not an exact production maximum.

Backend source remains `6d1cde1`, with the existing 4-vCPU/8-GiB ECS and separate
8-vCPU/16-GiB generator. No backend code/configuration or resource changes.
Workload remains 800 shows, 300 seats each, 8,000 logical viewers, 95% reads / 5%
unique-seat holds. Four generator processes; no retries.

## Five-minute stages

| Offered RPS | Completed requests | Read p95 | Hold p95 | Admission rejections | Drops | Gate |
|---|---:|---:|---:|---:|---:|---|
| 400 | 120,000 | 10.167 ms | 45.053 ms | 0 | 0 | Passed |
| 500 | 150,000 | 37.666 ms | 94.060 ms | 45 | 0 | Failed |
| 650 | Not run | — | — | — | — | Stopped after failure |
| 800 | Not run | — | — | — | — | Stopped after failure |

Read p95 <=150 ms and hold p95 <=300 ms are necessary but not sufficient: the
zero-error gate rejects any unexpected response or generator drop. All 45 failures
were ADMISSION_FULL, not generator drops. 500-RPS rejection rate was 0.6% of hold
attempts (0.03% of all requests). These are worst-worker percentiles, not exact
merged percentiles. A five-minute pass does not establish sustained capacity.

400 RPS: 2026-09-09 12:27:15–12:32:16 UTC; 500 RPS: 12:33:02–12:38:04 UTC.
Stages used separate seat ranges starting at 0 and 30. The short-stage sequence
retains normal expiry activity from the earlier stage. Before the sustained run,
all 13,455 accepted holds were verified persisted and expired, with zero pending
orders, broken links, unpublished outbox, pending refresh or dead letters.

[400 summary](capacity-400/summary.json), [500 summary](capacity-500/summary.json).
The 400-RPS, 30-minute validation completed successfully using offset 150 and fresh
credentials after expiry drain; details below.

## Harness validation

The coordinator now supports exact aggregate rates not divisible by four; 650 RPS
would be 163/163/162/162. Backend behavior is unchanged. This allocation is recorded
in [ADR 0014](../../adr/0014-private-cloud-capacity-benchmark.md). The 650/800 stages
were intentionally not executed after 500 failed.

[Coordinator tests](coordinator-tests.log): two CLI tests passed, validating actual
assigned rates and disjoint show/viewer partitions for 400 and 650. One local pytest
cache-directory permission warning did not affect execution. These are mocked
network tests of the harness contract, not cloud performance evidence. Ruff passed.


## Sustained 400-RPS validation

Measured **2026-09-09 12:41:20–13:11:21 UTC** (19:41–20:11 Bangkok), with unchanged
backend source/configuration. All four generators offered 100 RPS for 1,800 seconds.
The shared-start skew was 26.15 ms, within the unchanged 100-ms gate.

| Measurement | Result |
|---|---:|
| Completed HTTP requests | **720,000** |
| Availability reads | **684,000** |
| Successful holds | **36,000** |
| Unexpected responses / transport errors / task errors | **0** |
| Generator drops | **0** |
| Worst-worker read p95 | **9.428 ms** |
| Worst-worker hold p95 | **41.906 ms** |
| Worker exit codes | **0 / 0 / 0 / 0** |
| Sustained gate | **Passed** |

Reads were 327,990 HTTP 200 and 356,010 HTTP 304. All 36,000 holds returned HTTP
201. Client and [server counts](capacity-soak-400-log-analysis.json) match, and all
worker accounting checks pass. This workload means approximately **380 availability
reads and 20 hold attempts per second**, not 400 bookings/payments per second.

[Phase-filtered observations](stage-analysis.json):

| Observation | 400 RPS / 5 min | 500 RPS / 5 min | 400 RPS / 30 min |
|---|---:|---:|---:|
| Observer samples | 150 | 149 | 889 |
| Missing maps / observer errors | 0 / 0 | 0 / 0 | 0 / 0 |
| Peak DB connections | 14 | 14 | 14 |
| Sampled lock waiters | 0 | 0 | 0 |
| Maximum reconciliation age | 20.633 s | 20.584 s | 20.454 s |
| Maximum overdue hold-cleanup delay | 0.270 s | **6.253 s** | 0.496 s |

500 RPS increased rejection and expiry-cleanup delay despite acceptable request
p95. These observations establish a failed workload gate, not an exclusive root
cause. Brief lock waits can occur between samples. The raw observer spans stages,
drain and post-run checks; derived figures use only each measured interval.

## Durability and validation

Before the sustained run, [stage durability](capacity-stages-durability.json)
verified 13,455 accepted holds and empty queues at 12:40:39 UTC. This removed
carryover from the failed higher-rate stage before long-run validation.

At 13:13:33 UTC, after the final 120-second TTL drain,
[final durability](capacity-final-durability.json) matched all **49,455 accepted
holds across the three runs** with distinct persisted holds/orders and idempotency
records. No broken links, active/overdue run holds or pending run orders remained.
Global unpublished outbox, pending refresh and dead-letter counts were zero.

[Two real end-to-end tests](capacity-e2e.log) passed after load: 100 contenders for
one seat with one winner, and duplicate payment callbacks yielding one ticket.
The full application suite was not rerun in this measurement-only task; the prior
[84-test validation](../validated-browse-body/README.md) remains separate evidence.
The new coordinator received the two CLI contract tests described above.

## Reproduction, source and cleanup

Use the existing c6.xlarge.2 backend (4 vCPU / 8 GiB) and c6.2xlarge.2 generator
(8 vCPU / 16 GiB), with direct private-IP traffic. API, PostgreSQL, PgBouncer,
Redis, Kafka and workers remain on the single backend. Settings remain admission 8,
DB pool maximum 12, hold TTL 120 seconds, reconciliation interval 20 seconds,
batch 8 and pass budget 500 ms; map TTL is unchanged at 30 seconds.

[Backend hashes/configuration](staged-config.json) match application commit `6d1cde1`.
Generator coordinator commit `5bb9e6b` supports exact non-divisible rates. Its
observed SHA-256 is `105709446aefd1bfc022471be01b8078677982f163d6854f46efbdc66e933a29`;
HTTP generator remains `27153d0c5dcb9095766016b77b4a9d5510965b3b250971adcd04b9476a94a91b`.
Backend files and coordinator were verified against Git using CRLF line endings.
No backend code/configuration, instance sizing, TTL or admission change was made.

Use fresh private manifests, confirm all maps are warm, run 400 then 500 RPS for
300 seconds each, and stop escalation at the first failed gate. Keep failed output.
After expiry/queue drain, issue fresh credentials and run the highest passing stage
for 1,800 seconds using separate seats. The deterministic seat allocation budgets
were checked before execution; the sustained range began at seat 150.

[Cleanup](capacity-stopped.log): all benchmark containers stopped, temporary API
firewall rule removed after stopping, private manifests removed from both hosts
and the API container. Private environment files and synthetic data volumes remain.
Observer/resource/vmstat jobs were intentionally stopped after evidence capture;
the observer's cleanup exit 137 is outside the measurement window, not a load failure.
The downloaded archive was SHA-256 verified before extraction. Log terminal padding
was normalized without changing result content.

## Evidence and interpretation limits

- [400 short summary](capacity-400/summary.json), [500 short summary](capacity-500/summary.json),
  [400 sustained summary](capacity-soak-400/summary.json); each directory includes all four worker results/logs.
- Server analyses: [400 short](capacity-400-log-analysis.json), [500 short](capacity-500-log-analysis.json),
  [400 sustained](capacity-soak-400-log-analysis.json).
- Prometheus: [400 short](capacity-400-prometheus.json), [500 short](capacity-500-prometheus.json),
  [400 sustained](capacity-soak-400-prometheus.json).
- [Raw observations](staged-observations.json), [resources](staged-resources.log), [vmstat](staged-vmstat.log).

The tested zero-error operating point rises from 352 to **400 HTTP RPS** for this
uniform read/hold mix. The exact boundary above 400 is unknown; 500 was the next
failed tested rate. Neither 650 nor 800 was attempted after failure. Do not treat
500 as a hard transport ceiling: it completed requests but rejected some holds.
No 5–10× scaling, hot-show/simulcast skew, retry storm, checkout/payment throughput,
HA failover or all-200-location production capacity was qualified. Sequential runs
also retain database history and cloud performance variation; lower latency here
than the previous 352-RPS run is not evidence of a new backend optimization.
