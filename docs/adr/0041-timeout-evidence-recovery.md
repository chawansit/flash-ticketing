# ADR 0041: Recover evidence after a timed-out load SSH call

Status: Accepted for implementation

## Context

The first unattended 750 RPS safety stage on 2026-09-19 reached a fresh 800-show fixture and completed the generator workload. One worker reported a single `503 DATABASE_UNAVAILABLE`, so the strict workload gate failed. The generator wrote its summary at 16:13:05 UTC, but the operator SSH call remained open until its 840-second timeout at 16:16:54 UTC. ADR 0040's fail-closed finalizer restored admission, but the exception skipped evidence collection and the exact post-TTL audit. These checks then had to be run manually.

A failed stage must never be promoted. Its durable and overlap evidence is still needed to distinguish an availability failure from a data-integrity failure.

## Decision

A timeout or nonzero exit from the load SSH call marks the stage as failed but does not skip the remaining bounded evidence path. The orchestrator stops observers, copies any completed worker output, waits for hold expiry, and attempts the exact durability audit. Missing or incomplete output is recorded as an evidence failure. The finalizer still restores admission and removes private manifests. No HTTP request is retried and no higher load level is started.

Before rollback, the backend helper records a bounded, redacted list of API error responses from the candidate containers. It does not retain complete request logs, authorization headers or private manifests. The final stage result distinguishes workload failure, evidence failure and rollback failure.

This amends ADR 0040 only for load-phase timeout/nonzero-exit recovery. Its fresh-fixture, strict-gate and rollback decisions remain in force.

## Alternatives considered

- Abort directly to rollback after a load timeout. Rejected because completed worker results and post-TTL integrity checks would be lost.
- Treat a completed summary as success after SSH timeout. Rejected because the transport failure itself violates unattended-stage reliability.
- Retry failed HTTP requests or repeat the same load until it passes. Rejected because that hides the measured availability failure.
- Keep full API logs. Rejected because their volume is high and they can contain sensitive request context.

## Consequences

A failed stage can take several additional minutes to finish its audit and queue-drain checks. Evidence collection is best-effort and bounded; absent evidence never becomes a passing gate. The error excerpt adds a small public diagnostic artifact, while raw observer files remain on the ECS.

## Failure and recovery behavior

If the generator exits nonzero, the stage records the exit code and continues to evidence. If its SSH command times out, the local SSH process is terminated, the timeout is recorded, and completed generator files are collected. If files are absent or partial, the audit is attempted only when its required worker results exist; otherwise the durability gate is false. Any observer, copy or audit failure is recorded without skipping admission rollback and private cleanup. If rollback cannot be verified, the overall result remains failed and requires operator repair before another stage.

## Validation evidence

Implementation requires unit tests for nonzero load exit, timed-out load with completed evidence, missing worker evidence, and independent cleanup after evidence failure. The 2026-09-19 live stage is failure evidence only; it does not establish a passing capacity level.

Implementation validation: eight focused timeout/error-extraction tests and
eight related load-gate tests passed (16 total). Ruff passed for the changed
Python files. The API error extractor was smoke-tested against live health logs
and emitted only allowlisted fields. Live timeout recovery remains to be proven
on a later stage; the 2026-09-19 stage used the preceding implementation.
