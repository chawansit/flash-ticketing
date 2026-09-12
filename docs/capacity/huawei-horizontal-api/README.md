# Fixed-budget horizontal API validation on Huawei Cloud

## Result

Two API instances behind Nginx passed 500 and 600 RPS for 30 minutes while the aggregate PostgreSQL pool and hold-admission budgets remained twelve. Both stages had zero unexpected responses, transport errors and generator drops. Post-expiry audits found exact persistence, zero broken links, zero overlapping held-seat intervals and drained outbox, reconciliation and dead-letter queues.

This removes the single-process API saturation observed at 600 RPS. It does not establish production maximum capacity or high availability because API, Nginx, PostgreSQL, Redis, Kafka and workers still share one 4-vCPU backend VM.

## Topology and workload

- Backend: Huawei `c6.xlarge.2`, 4 vCPU / 8 GiB.
- Generator: Huawei `c6.2xlarge.2`, 8 vCPU / 16 GiB.
- Two API containers behind one Nginx container; API pool/admission six each.
- Aggregate API DB pool and admission budgets: twelve.
- 800 shows, 300 seats per show, 8,000 viewers, 95% conditional reads and 5% independent holds.
- Four generator processes, no retries, client/server keep-alive 5/10 seconds and hold TTL 120 seconds.

## Sustained gates

| Stage | Requests | Unexpected | Drops | Read p95 | Hold p95 | Durable holds | Overlaps | Result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| 500 RPS, 2 API | 900,000 | 0 | 0 | 9.026 ms | 33.923 ms | 45,000/45,000 | 0 | pass |
| 600 RPS, 2 API | 1,080,000 | 0 | 0 | 12.969 ms | 47.014 ms | 54,000/54,000 | 0 | pass |

## Resource and transaction evidence

| Measurement | 500 RPS | 600 RPS |
|---|---:|---:|
| API 1 mean CPU | 44.668% | 54.368% |
| API 2 mean CPU | 44.563% | 54.277% |
| PostgreSQL mean CPU | 34.625% | 41.203% |
| Redis mean CPU | 15.633% | 18.447% |
| Nginx mean CPU | 6.625% | 7.584% |
| Mean pool acquire, API range | 10.105–10.200 ms | 15.308–15.397 ms |
| Mean transaction body, API range | 8.586–8.673 ms | 13.261–13.348 ms |
| Mean query, API range | 0.580–0.586 ms | 0.910–0.917 ms |
| Mean commit, API range | 1.396–1.405 ms | 1.918–1.919 ms |
| Maximum sampled lock waiters | 0 | 0 |

Docker CPU uses 100% for one core. The observers sampled each API independently; PostgreSQL/WAL values appear in both observer streams and are not summed. Zero sampled lock waiters does not exclude waits shorter than the sample interval.

At 600 RPS the two APIs used about 108.6% of one core in aggregate, versus 94.7% for the failed single-API run. The extra process and Nginx cost CPU, but distributed scheduling removed all 421 HTTP 503 responses and 68 generator drops while sharply reducing p95 latency.

## Four-replica correctness preflight

Four API replicas start with pool three, hold admission three and simulator validation concurrency two per process. The aggregate pool and admission budgets remain twelve. A synchronized 100-request wave against one seat returned one 201, eleven seat-busy 409 responses and 88 bounded admission 503 responses. Failed-response p95 was 44.205 ms. The immediate PostgreSQL audit found one idempotency record, one hold, one order and one seat owner; the durable owner matched the acknowledged winner.

This passes the zero-double-booking preflight. The high admission-rejection share is expected from a burst of 100 requests against only twelve aggregate hold slots and means this configuration is unsuitable for absorbing synchronized contention without an upstream waiting-room or rate-control layer. Sustained four-replica capacity validation remains pending a fresh private manifest.

## Regression validation

After the horizontal override correction, 69 unit tests and 57 integration tests passed. The integration suite includes persistence-failure rollback, process contention, cache reconciliation and recovery behavior. Both suites reported only the two existing dependency deprecation warnings. Ruff passed with the Windows Docker mount executable-bit rule excluded; no Python source changed in this commit. Compose config validation passed for pool/admission three and simulator concurrency two per API replica.
## Evidence scope

Machine-readable summaries, per-instance observer streams, Docker CPU samples, durability reports and the non-secret contention summary are retained in this directory. Credential manifests, public addresses and generator worker files are excluded.
