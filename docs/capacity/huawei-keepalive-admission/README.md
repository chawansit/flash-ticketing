# Keep-alive margin and hold admission: Huawei long-run validation

## Result

Commit `1a68518` added an opt-in ten-second server keep-alive while retaining a
five-second client pool expiry. At admission eight, 400 RPS passed 30 minutes
with 720,000 completed requests, zero unexpected responses, transport errors or
drops, and exact post-expiry durability.

The matched 500 RPS admission-eight run completed all 900,000 requests without a
transport failure, but three holds received `ADMISSION_FULL` in the same
millisecond at occupancy 8/8. Admission twelve was then recorded in ADR 0033
and tested with the same one-worker, pool-twelve topology. It completed 900,000
requests with zero unexpected responses, transport errors or drops. All 45,000
accepted holds persisted and expired correctly with zero overlapping seat
intervals and drained queues.

## Topology and workload

- Backend: Huawei `c6.xlarge.2`, 4 vCPU / 8 GiB.
- Generator: Huawei `c6.2xlarge.2`, 8 vCPU / 16 GiB.
- Private network, four generator processes and no client retry.
- 800 shows, 300 seats per show and 8,000 viewers.
- 95% conditional seat-map reads and 5% independent seat holds.
- One API worker, DB pool 12, hold TTL 120 seconds and client/server keep-alive
  5/10 seconds.
- PostgreSQL, PgBouncer, Redis, Kafka and background workers shared the backend.

## Gate results

| Stage | Requests | Unexpected | Drops | Read p95 | Hold p95 | Durability | Result |
|---|---:|---:|---:|---:|---:|---|---|
| 400 RPS, admission 8 | 720,000 | 0 | 0 | 12.817 ms | 38.715 ms | 36,000/36,000 | pass |
| 500 RPS, admission 8 | 900,000 | 3 admission 503 | 0 | 17.914 ms | 54.937 ms | 44,997/44,997 | fail |
| 500 RPS, admission 12 | 900,000 | 0 | 0 | 16.694 ms | 57.666 ms | 45,000/45,000 | pass |
| 600 RPS, admission 12 | 1,079,932 completed | 421 HTTP 503 | 68 | 108.968 ms | 245.420 ms | 53,642/53,642 | fail |

Every durability audit reported zero broken links, active or overdue holds,
pending orders and overlapping load-held seat intervals. Unpublished outbox,
pending refresh and dead-letter counts were zero after each drain.

## Admission comparison at 500 RPS

| Measurement | Admission 8 | Admission 12 |
|---|---:|---:|
| Mean DB pool acquire | 20.834 ms | 17.554 ms |
| Mean transaction body | 18.728 ms | 15.691 ms |
| Mean query | 1.303 ms | 1.086 ms |
| Mean commit | 2.000 ms | 1.759 ms |
| PostgreSQL mean CPU | 33.626% | 37.829% |
| API mean CPU | 79.658% | 79.097% |
| Maximum sampled active DB connections | 4 | 5 |
| Maximum sampled lock waiters | 0 | 0 |
| Maximum reconciliation age | 20.490 s | 20.512 s |

Docker CPU uses 100% for one core. Observer values are sampled; zero sampled lock
waiters does not prove that no sub-sample wait occurred. The admission-eight
rejections occurred at `2026-09-12T14:43:02.801-803Z`: each arrived at
occupancy 8/8 and returned in 0.08-0.12 ms. Raising admission admitted this
bounded wave without increasing the DB connection budget.

The admission-twelve hold p95 was 4.97% higher, while read p95, pool acquire,
transaction, query and commit averages were lower. This single pair does not
establish statistical confidence, but it passes every predeclared gate and the
resource evidence does not show pool or PostgreSQL saturation.

## Keep-alive conclusion

The ten-second server/five-second client ordering completed 3.60 million
requests across these stages without a transport error. The prior equal
five/five control recorded one reset in 720,000 requests. This evidence clears
the current capacity-test transport gate and supports the ten-second setting for
this topology; it does not prove that resets are impossible or define timeout
ordering for an external production load balancer.

## Decision and next gate

ADR 0033 accepts admission twelve as the single-process default and supersedes
the admission-eight default portion of ADRs 0020 and 0029. DB pool maximum
remains twelve. Horizontal scaling must budget aggregate admission and database
connections explicitly.

The 600 RPS stage was executed for 30 minutes and failed the capacity gate. It
produced 352 hold admission 503 responses, 69 server concurrency 503 responses
(64 reads and five holds) and 68 generator drops. No transport error occurred.
API CPU averaged 94.672% of one core while PostgreSQL averaged 44.809%; sampled
active DB connections peaked at four and lock waiters remained zero. All 53,642
accepted holds passed durability and overlap checks and queues drained.

Do not run 750 RPS on the single-instance topology. The verified operating point
is 500 RPS for this exact workload and topology. The next experiment should split
API CPU across two instances behind a load balancer while keeping aggregate DB
pool and admission budgets at twelve.

Machine-readable summaries, observer/CPU samples, database table counters and
durability reports are retained below. Credential manifests and host details
from generator worker files are excluded.
