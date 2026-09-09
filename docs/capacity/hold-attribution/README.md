# Hot-seat failed-request attribution

Analysis of the 9 September 2026 admission experiment; no new cloud load or booking change. All **4,000 hot-seat requests** were joined to retained server logs by request ID, with exact status/error-code agreement. The next diagnostic target is the interval before handler entry and the unmeasured response path, not a larger database pool or a speculative Redis shortcut.

## Findings

| Measured server phase | Admission 8 | Admission 16 |
|---|---:|---:|
| ADMISSION_FULL count | 1,921 | 1,768 |
| ADMISSION_FULL app p95 | 0.11 ms | 0.11 ms |
| SEAT_BUSY count | 65 | 198 |
| SEAT_BUSY app p95 | 510.62 ms | 490.92 ms |
| SEAT_BUSY pre-handler dispatch p95 | 369.67 ms | 414.43 ms |
| SEAT_BUSY Redis shield entry p95 | 2.41 ms | 2.87 ms |
| SEAT_UNAVAILABLE count | 12 | 32 |
| SEAT_UNAVAILABLE app p95 | 518.12 ms | 491.33 ms |
| SEAT_UNAVAILABLE pre-handler dispatch p95 | 424.36 ms | 413.91 ms |
| SEAT_UNAVAILABLE database body p95 | 7.96 ms | 8.31 ms |

Percentiles are separate distributions; do not sum them. Small cohorts, especially the 12 unavailable-seat responses, limit inference. This analysis covers four bursts only, not the uniform workload.

`ADMISSION_FULL` returns before authentication, Redis and PostgreSQL. Its missing phase fields mean **not executed**, not a measured zero. The slow total client response for these requests must not be described as slow database rejection. The existing experiment's generator lane wait p95 was 508–628ms across the four waves; that waiting occurs before a request is sent. Protocol queue and client residual timings remain available per request.

`SEAT_BUSY` fails at Redis shield acquisition and does not enter PostgreSQL. For example, baseline request `df023d62-46ac-424e-8266-75c0b5f9fb00` took **513.41ms** in the app: **371.365ms dispatch**, **3.429ms rate limit**, **0.625ms Redis entry**, and **137.991ms unattributed app time**. These are components of the same request. A faster database path cannot help this request because it never entered the database.

Baseline `SEAT_UNAVAILABLE` request `8d9ad68f-fecc-4870-8adb-0d768c8bd72a` took **518.12ms**: dispatch was **424.363ms**, database body **2.588ms**, database entry **2.871ms**, and exit **0.755ms**. The body includes idempotency and authoritative seat checks; it is not an isolated query or lock-wait metric.

## What remains unmeasured

The current dispatch timer starts after admission and ends at the synchronous hold handler. It combines body parsing, validation, dependency processing (including authentication), worker-thread scheduling and event-loop resumption. **It does not isolate JWT verification or prove that the thread pool is saturated.** Likewise, `database_body` combines idempotency with other SQL. No separate authentication or idempotency latency is inferred from these logs.

The per-request residual subtracts the existing disjoint phase intervals from app duration. It can include exception handling, response processing and scheduling between stages; it is not direct evidence of one particular queue. Rounded measurements can produce tiny negative residuals; the analyzer preserves them rather than inventing time.

Before selecting a performance pattern, measure authentication execution separately, dependency/handler worker dispatch and response/exception resumption; add a nested idempotency timer without double-counting it in database-body totals. Collect thread-limiter occupancy and event-loop lag at these boundaries. Then repeat matched admission-8 bursts. This is the next proposed diagnostic experiment, **not implemented or measured in this report**. Any resulting execution-model or conflict-path change requires an ADR before implementation.

## Evidence and reproduction

- [Admission 8 attribution](admission-8.json), [admission 16 attribution](admission-16.json): separate outcome distributions and five slowest app requests per outcome.
- Minimal server records: [8](admission-8-server.jsonl), [16](admission-16-server.jsonl), containing only request ID, status, error code, duration and phase timings.
- Client data and original controls: [ingress experiment](../ingress-admission/README.md).
- [Analyzer](../../../scripts/analyze_hold_contention.py), [tests](../../../tests/unit/test_hold_attribution.py).

Example from the repository root (expand both waves' worker files):

```sh
python scripts/analyze_hold_contention.py --clients docs/capacity/ingress-admission/generator/ingress-8-hot-*/worker-*.json --logs docs/capacity/hold-attribution/admission-8-server.jsonl --output docs/capacity/hold-attribution/admission-8.json
```

Repeat for admission 16. The shell example uses POSIX wildcard expansion; on Windows expand paths before invoking Python. Missing joins, duplicate IDs and inconsistent outcomes fail the analysis instead of silently dropping evidence.

Executed validation: analyzer matched 2,000 requests per setting; **2 unit tests passed in 0.06s** for missing/duplicate/mismatched records and absent-phase handling; Ruff passed. No application tests or cloud load were rerun because application behavior and deployment were unchanged. Existing ADR 0020 remains applicable; this offline analysis selects no new architectural pattern. Benchmark services remain stopped.
