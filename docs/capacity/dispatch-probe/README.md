# Worker dispatch diagnostic experiment

On 9 September 2026, two warm 1,000-contender waves on the existing Huawei ECS pair showed that **actual authentication work was fast, while worker submission and event-loop resumption took tens of milliseconds**. This supports investigating unnecessary worker transitions; it does not establish thread-pool exhaustion or a new production capacity.

## Measurements

All 2,000 client requests matched server outcomes. Of these, 88 were admitted: each has exactly one authentication, service-dependency and hold-handler dispatch record. The remaining 1,912 were rejected before these stages.

| Stage, 88 samples each | Submit to worker entry p95 | Work execution p95 | Worker finish to await resumption p95 |
|---|---:|---:|---:|
| Authentication | 48.65 ms | 0.131 ms | 82.89 ms |
| Service dependency | 81.49 ms | 0.005 ms | 79.98 ms |
| Hold handler | 62.13 ms | 9.36 ms | 60.70 ms |

These are separate distributions and must not be added. The limiter had **40 tokens**, with at most **7 borrowed** and **0 tasks waiting at the submission snapshots**. Submission-to-entry includes limiter admission, worker wake-up and OS scheduling. Finish-to-resume measures delivery back to the awaiting task; neither measurement identifies the exact event-loop or OS cause. Snapshots cannot rule out waits between samples.

Hold idempotency executed 20 times, p95 **1.958 ms**, maximum **2.464 ms**. Requests rejected by the Redis shield never reached this phase. Idempotency is nested inside database-body and handler execution and is not an additional disjoint interval.

The service dependency simply returns the reservation service. Its tiny measured execution time and comparatively long scheduling boundaries make it the first candidate for a carefully controlled execution-model experiment. JWT verification should remain enabled; these results do not justify bypassing authentication or increasing worker/pool limits.

## Workload and correctness

Same backend 4 vCPU / 8 GiB and separate generator 8 vCPU / 16 GiB, private IP, admission 8, pool maximum 12, hold TTL 120s, 800 shows × 300 seats, background workers active. Each wave used four processes, 128 warm connection lanes, 1,000 distinct viewers and a fresh seat (S250 then S251), no retries. Only the opt-in diagnostic entry point changed; booking implementation remains the previous deployed image.

| Wave | 201 | 409 | 503 | Failed total p95, including generator waiting |
|---|---:|---:|---:|---:|
| S250 | 1 | 41 | 958 | 553.27 ms |
| S251 | 1 | 45 | 954 | 569.23 ms |

Both waves passed HTTP accounting, warm connection reuse and required ingress timing. Immediate PostgreSQL checks found exactly one active owner matching each winner. After expiry, all eight worker run IDs matched expected persisted records, including workers with no success; both acknowledged holds expired with no broken links or pending orders. The final outbox, refresh and dead-letter snapshot was zero.

The <=200ms failed-total-response goal remains unmet. The new probe adds logging and metrics overhead; these two diagnostic waves are not a matched before/after optimization study. No uniform-load or sustained-capacity claim is added.

## Implementation and validation

The optional ingress Compose override mounts the probe and enables it only when `DISPATCH_PROBE=1`. The normal application deployment is unchanged. The probe wraps the pinned FastAPI dispatcher references, retaining the original dispatcher and synchronous functions. It records fixed-stage metrics and payload-free structured logs, preserves return values/exceptions and keeps nested measurements separate from existing hold phases. Unsupported dispatcher layout or duplicate installation fails explicitly.

Local real thread-limiter/protocol/admission tests: **7 passed in 5.76s** (two existing deprecation warnings). Tests cover a blocked real worker, result and exception preservation, non-hold passthrough, nested trace isolation and installation/restoration. Ruff passed after import-only corrections. Existing cloud application suite: **84 passed in 12.25s**, run after measurement with the diagnostic API active. No new production performance improvement is claimed.

All benchmark services were stopped, private manifests on both hosts and inside the API removed, temporary firewall rule removed, and remote probe setting returned to off. ECSs and synthetic data are preserved.

## Evidence and reproduction

- [Summary](summary.json), [runtime versions and source hashes](dispatch-config.json), [raw diagnostic records](dispatch-records.jsonl).
- [First wave](dispatch-hot-0/summary.json), [second wave](dispatch-hot-1/summary.json), with raw worker results in each directory.
- [Owner S250](dispatch-owner-250.json), [owner S251](dispatch-owner-251.json), [expiry verification](dispatch-durability.json).
- [Cloud tests](dispatch-cloud-tests.log), [cleanup](dispatch-cleanup.log), [ADR 0021](../../adr/0021-thread-dispatch-diagnostics.md).

Run `python docs/capacity/dispatch-probe/summarize.py` to regenerate summary.json and validate complete per-request stage coverage. To repeat cloud measurement, use the existing private fixture/token-export workflow and optional ingress Compose override with `DISPATCH_PROBE=1 EXPERIMENT_ADMISSION=8`. Run the existing contention harness with `--mode warm --workers 4 --contenders 1000 --require-ingress` and fresh seat/output values. Verify live ownership before expiry and durable counts after drain. Credentials are not included.

## Next experiment

Write an ADR before changing the service dependency to execute directly on the event loop; it performs only an in-memory lookup. Preserve synchronous Redis/PostgreSQL work and all authentication/idempotency behavior. Compare instrumented baseline and candidate in repeated order-balanced hot-seat waves plus the same 400 RPS uniform workload, then verify ownership and expiry. Broader event-loop/OS profiling is still needed if scheduling delay remains; this experiment does not measure the full exception/response path after await resumption or event-loop lag directly.
