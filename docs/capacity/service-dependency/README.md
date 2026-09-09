# Inline service dependency: controlled experiment

**Decision: candidate not adopted; synchronous service dependency restored.** The one-line async candidate removed a worker transition and had lower measured latencies, but both five-minute uniform runs had one read `ReadError`. The candidate therefore failed the predeclared zero-error gate. The results do not prove the error was caused or fixed by the candidate.

## Results — 9 September 2026

| Metric | Baseline: sync service | Candidate: async service |
|---|---:|---:|
| Uniform workload | 400 HTTP RPS × 300s | 400 HTTP RPS × 300s |
| Attempted requests | 120,000 | 120,000 |
| Holds acknowledged 201 | 6,000 | 6,000 |
| Read transport errors | 1 ReadError | 1 ReadError |
| Generator drops | 0 | 0 |
| Read p95, worst worker | 14.93 ms | 13.47 ms |
| Hold p95, worst worker | 58.44 ms | 51.83 ms |
| Uniform zero-error gate | **FAIL** | **FAIL** |
| Hot-seat 201 / 409 / 503 totals | 2 / 93 / 1,905 | 2 / 80 / 1,918 |
| Failed total p95, first / second hot wave | 646.09 / 618.58 ms | 582.06 / 603.34 ms |
| Server app p95, 409 cohort | 569.36 ms (93 requests) | 415.38 ms (80 requests) |
| Pre-handler dispatch p95, 409 cohort | 463.63 ms | 285.33 ms |

Hot-wave order was **baseline / candidate / candidate / baseline**. Each wave had 1,000 contenders and exactly one live PostgreSQL owner matching the HTTP winner. The candidate returned 13 more admission rejections across 2,000 contenders, so it is not an across-the-board improvement. Both still failed the <=200ms total failed-response target.

Uniform traffic was 95% availability reads and 5% distinct-seat holds: 114,000 read attempts and 6,000 holds per version. Each read error occurred in worker 3; the other reads returned 200/304. No unexpected HTTP status or generator drop was reported. Read percentiles include conditional 304s; uniform values are worst-worker percentiles, not pooled values. The percentile aggregates recorded durations across statuses, including the failed read; passing a percentile does not override the failed zero-error gate. Existing baseline API logs contained no ASGI exception/traceback marker, which does not rule out a transport problem.

## What changed and what was verified

The [candidate patch](candidate.patch) only changes `def service(request: Request)` to `async def service(request: Request)`. Its body still returns `request.app.state.reservations`. Authentication and reservation handlers remain synchronous; all Redis, PostgreSQL, ownership, idempotency and TTL behavior stays unchanged.

The same optional dispatcher/ingress probes were enabled for both versions. All **4,000 hot requests** matched server request IDs, statuses and error codes. All 95 admitted baseline requests had authentication, service and handler dispatch records. All 82 admitted candidate requests had authentication and handler records, with **no service worker dispatch**. This verifies the intended worker transition was removed, rather than merely losing request coverage.

The measured speedup is directional evidence from two waves per version, not statistical certification. Cohort sizes differ and the total includes generator waiting; do not add or subtract percentile columns. Probe logging adds overhead. Uniform tests ran baseline first, candidate second; database/cache history was not reset. The closing baseline wave followed the candidate's integration test. Background workers remained enabled, but the 2-second observer was collected around the uniform intervals rather than identically across every hot wave. These controls and limitations preclude a maximum-capacity or general production-improvement claim.

## Topology and execution

Huawei backend: 4 vCPU / 8 GiB, with API, PostgreSQL, Redis, Kafka and background workers on the same ECS. Separate generator: 8 vCPU / 16 GiB over private IP. Fixture: 800 shows × 300 seats. Admission 8, pool maximum 12, hold TTL 120s, existing HTTP server/parser and generator unchanged.

Hot requests used four generator processes and 128 warmed connection lanes, fresh seats S260 (baseline), S261/S262 (candidate), S263 (baseline), no retries. Distinct-seat uniform offsets were 80 and 120. Baseline uniform ran approximately 15:41:50–15:46:50 UTC; candidate 15:49:02–15:54:02 UTC.

Cloud overrides mounted either baseline or candidate API source at its installed module path in the same image. Runtime introspection confirmed sync/async as appropriate and matching source hashes. The test container also mounted candidate API source when running the application suite. The baseline was restored before the closing wave and remains the repository default.

## Correctness, tests and cleanup

Immediate ownership checks passed for all four contested seats. Post-expiry verification covered **24 worker run IDs**, including workers with zero successes: all **12,004 acknowledged holds** had matching idempotency records, holds and orders. There were no broken links, remaining active/overdue holds or pending orders. Final outbox, refresh and dead-letter queues were zero.

Candidate validation: **44 local unit tests passed in 9.45s**, including the new real-route regression; **84 existing cloud tests passed in 13.35s**, after load measurement. Ruff passed. These tests establish functional checks, not a passing load gate. See [local validation](local-validation.md) and [cloud output](backend/service-candidate-tests.log). The candidate regression is retained as an experiment artifact and requires applying the patch before execution; it is not installed in the default test suite after rollback.

The API dependency was restored to sync locally and remotely, all benchmark containers stopped, private manifests removed from both hosts/API, and temporary firewall rule removed. Probe enablement in the experiment override was returned to off. ECSs and synthetic volumes remain available.

## Evidence and reproduction

- [ADR 0022](../../adr/0022-inline-service-dependency.md), [comparison JSON](comparison.json), [reproduction analyzer](summarize.py).
- [Baseline uniform](generator/service-baseline-uniform/summary.json), [candidate uniform](generator/service-candidate-uniform/summary.json), with failed worker evidence preserved.
- [Baseline runtime](backend/service-baseline-config.json), [candidate runtime](backend/service-candidate-config.json), [exact experiment override](backend/compose.service.yaml).
- `generator/` contains all four hot waves, both uniform runs and worker outputs; `backend/` contains matched hot request/probe records, uniform Prometheus/observer exports and all four live-owner checks. Full API logs remain on the backend ECS.
- [Durability](backend/service-durability.json), [cleanup](backend/service-cleanup.log), [candidate regression](candidate_regression.py).

Run `python docs/capacity/service-dependency/summarize.py` to reproduce accounting and stage-coverage checks. For a new cloud comparison, use the existing private manifest workflow, the same diagnostic entry point and generator scripts, the retained source-selection override and fresh seats/output directories. Apply candidate.patch only to the candidate source; use `SERVICE_API_SOURCE` to choose the mounted file. Verify live ownership before expiry and persistence after drain. The override is a historical experiment artifact, not a production deployment recommendation.

## Next action

Instrument read transport failures with bounded timestamps, connection-reuse state, HTTP phase and exception details on the generator, then correlate with server/network evidence. Determine whether this is connection reuse, transport, server response handling or another cause; the current generic `ReadError` does not distinguish them. Preserve both failures. Only after diagnosing the error should the retained candidate be re-evaluated against the original zero-error gate.

Follow-up: [read transport diagnostics](../read-transport/README.md) implemented; two baseline runs did not reproduce the error. Root cause remains unresolved and this candidate remains unadopted.
