# Huawei RDS 750 RPS sub-second diagnostic — 2026-09-13

Status: **750 RPS passed for 30 minutes on the measured four-API topology after bounded admission and upstream keep-alive changes.**

This report extends the original two-API diagnostic through controlled comparisons. It establishes a demonstrated clean operating point for this fixed workload and topology. It is not a production maximum or an RDS saturation claim.

## Fixed workload and final topology

- Huawei RDS for PostgreSQL 17.11, 4 vCPU, 16 GiB memory and 100 GB storage.
- Four API containers behind Nginx `least_conn` on one 4-vCPU backend ECS.
- Three application DB connections and hold admission four per API: 12 DB connections and 16 admitted holds in aggregate.
- PgBouncer transaction pooling with a backend pool of 24; no reserve pool.
- Nginx upstream keep-alive cache 128 with a five-second idle timeout; Uvicorn idle timeout ten seconds.
- Four load-generator workers on a separate 8-vCPU ECS, five-second client connection expiry and no workload retry.
- 800 shows, 300 seats per show, 8,000 viewers, 95% conditional seat-map reads and 5% unique holds.
- Temporary `sslmode=require`; CA and hostname verification remain a production prerequisite.

## Original failure and corrected instrumentation

The original two-API 750 RPS run completed 1,349,995 of 1,350,000 scheduled requests. It had five generator late drops, three `ADMISSION_FULL` responses and one `DATABASE_UNAVAILABLE` response. Read and hold p95 remained within target and all 67,496 acknowledged holds were durable with zero overlap.

The original pool-acquire metric observed the connection context after it returned to the pool, so its roughly 28 ms average represented most of the checked-out connection lifecycle. The corrected instrumentation records pool acquisition and connection hold separately. Controlled follow-ups showed application-pool acquisition averaging about 0.02–0.04 ms, while connection hold and transaction time averaged about 26–27 ms. PgBouncer observation separately exposed the actual shared queue.

Huawei provider metrics for 10:55–11:00 UTC showed CPU at 10.19% maximum, write I/O latency at 0.77 ms maximum, 115.07 write IOPS maximum, 0.9 MiB/s maximum combined throughput, 2.37% connection usage and zero connection failures. These one-minute samples rule out sustained RDS saturation during the original spike, while short transients remain possible. See [Huawei RDS provider metrics](huawei-rds-provider-metrics.md).

## Controlled comparisons

| Candidate | Result | Read p95 | Hold p95 | Failure evidence |
|---|---:|---:|---:|---|
| Two APIs, PgBouncer 12, 10 min | Failed | 15.644 ms | 75.704 ms | 16 `ADMISSION_FULL`; PgBouncer wait peaked at 146.685 ms |
| Four APIs, pool/admission 3 each, PgBouncer 12, 10 min | Failed | 8.148 ms | 49.770 ms | 12 `ADMISSION_FULL`; PgBouncer wait peaked at 93.666 ms |
| Four APIs, pool/admission 3 each, PgBouncer 24, 10 min | Failed | 8.143 ms | 51.238 ms | 22 `ADMISSION_FULL`; PgBouncer had no sampled queue |
| Admission 4 each, reused seat range, PgBouncer 24, 10 min | Failed | 8.073 ms | 50.703 ms | One `RESOURCE_BUSY` on a previously used synthetic seat |
| Admission 4 each, fresh seat range, PgBouncer 24, 10 min | Passed | 8.118 ms | 48.721 ms | Zero errors and drops |
| Same topology, prior Nginx upstream cache, 30 min | Failed | 8.480 ms | 50.909 ms | Two read 502 responses with no application request ID |
| Nginx upstream timeout 5 s, safety run, 10 min | Passed | 8.236 ms | 50.443 ms | Zero errors and drops |
| Nginx upstream timeout 5 s, confirmation, 30 min | **Passed** | **8.251 ms** | **51.050 ms** | **Zero errors and drops** |

The reused-seat failure was retained rather than hidden. Repeating with a fresh, disjoint seat range removed that test-data collision and produced 22,500 successful holds with no error. This established a clean admission candidate before the long run.

The first admission-16 long run completed all scheduled requests with no admission rejection, generator drop or transport error, but Nginx returned two read 502 responses. Neither had an application request ID or matching API failure. With proxy retry still disabled, ADR 0038 added a five-second Nginx upstream idle timeout below Uvicorn's ten-second timeout. Nginx configuration validation passed before deployment.

## Final 750 RPS confirmation

The final stage ran from 16:59:51.838 to 17:29:51.984 UTC.

| Metric | Result | Gate |
|---|---:|---|
| Duration | 30 minutes | Pass |
| Scheduled / completed | 1,350,000 / 1,350,000 | Pass |
| Read responses | 1,282,500 | Pass |
| Holds 201 | 67,500 | Pass |
| Unexpected HTTP / transport errors | 0 / 0 | Pass |
| Generator drops | 0 | Pass |
| Worst-worker read p95 | 8.251 ms | Pass |
| Worst-worker hold p95 | 51.050 ms | Pass |
| Admission rejections | 0 | Pass |

PgBouncer recorded one 1.387 ms waiting sample among 9,001 exact-window samples. Server activity peaked at 19 of 24 and average cumulative wait was 0.305 microseconds per server assignment. RDS connections peaked at 20, active connections at 13 and sampled lock waiters remained zero.

Across the four APIs, corrected pool acquisition averaged 0.028–0.035 ms, connection hold averaged 26.734–26.857 ms, transaction time averaged 26.662–26.784 ms and commit averaged 3.732–3.816 ms. Event-loop lag averaged 0.873–0.887 ms; the largest sampled current lag was 63.686 ms. No replica rejected admission.

After the hold TTL elapsed, the audit matched all 67,500 acknowledged holds to 67,500 idempotency records, distinct holds and distinct orders. Broken links, active or overdue holds, pending orders and overlapping held-seat intervals were zero. Unpublished outbox events, pending seat refreshes and dead letters were also zero.

## Capacity conclusion

The demonstrated clean point is now **750 RPS for 30 minutes** for the exact workload and topology above. That is 1,282,500 seat-map reads and 67,500 durable hold transactions in one run with target latency, zero unexpected errors, zero double-booking and drained queues.

RDS was not close to its documented connection limit, and sampled CPU/storage evidence does not show sustained RDS saturation. The result therefore does not identify maximum capacity. A higher stage must begin at 750 RPS as the control and increase one step at a time while retaining the same strict availability, latency, correctness and queue-drain gates.

## Evidence

Each retained run directory contains compact request summaries and worker results. Server-side runs also retain exact-window metric summaries and post-expiry durability audits. The four-API control includes the 100-contender preflight, which produced exactly one durable winner for one seat.

Raw sub-second observers remain on the ECSs and are intentionally omitted from Git. Private manifests, tokens, database connection strings and credentials are excluded.
