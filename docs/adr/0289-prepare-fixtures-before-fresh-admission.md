# ADR0289: Prepare paid fixtures before fresh transition admission

## Status

Accepted for implementation under the standing work envelope. Refines the CCE lifecycle preparation order; supersedes its fixture-after-admission ordering for new scopes. Retains ADR0288 statistics ordering and all existing freshness, safety, runtime, financial and budget gates.

## Context

ADR0288's fresh five-minute cloud test passed every gate with25,200 completed tickets and zero initial/final customer errors. Its subsequent hour attempt adr0151-d301c0c07d23 aborted before dispatch: the local inventory qualification occurred299.108019seconds after capture, just below the unchanged300-second limit. The remote pipeline observer then exited with ValueError: Observation is stale or future dated. The surrounding observer-startup wait reported TimeoutError. Immutable startup logs prove the child failed in prepare_two_host_scaling.validate_inventory before SQL sampling began.

Bulk fixture creation,302,400 viewer tokens and their transfer currently happen after the pre-transition inventory capture and safety checks. That preparation consumes admission evidence freshness. This is a harness ordering failure, not a measured application, Redis, Kafka or RDS capacity regression. The hour scope is FAILED_RESTORED with zero paid runs started; cleanup, integrity and exact restoration passed.

## Decision

Create the owned paid fixture and private manifest before refreshing the pre-transition inventory and activating worker migration. Preserve stage.prepare after executed safety checks, and preserve every pre-dispatch check. Capture current machine/image/configuration evidence only after bulk fixture preparation; do not rewrite captured_at, reuse a spent fixture or extend the300-second freshness gate.

Reuse the owned audit helper and fixture producer. Creation may write isolated test inventory but dispatch remains prohibited until native safety, identity, diagnostics and queue checks pass. The prepared fixture is retained immediately for failure cleanup. If creation is ambiguous, preserve the producer identity and fail closed as before.

Record fixture-preparation elapsed time before admission for later diagnosis. This changes setup order only; application image, workload, customer recovery, resource sizes, connection budgets, offered rate and all quality gates stay fixed.

## Alternatives

- Raise the inventory age limit or replace its timestamp: rejected; stale evidence must not appear fresh.
- Repeat a full structural capture after worker migration: the former ECS API topology no longer exists; that would require a new native inventory model.
- Add infrastructure or optimize unrelated application code: no evidence of a runtime bottleneck in this aborted attempt.
- Rebuild the runner: unnecessary; reorder existing lifecycle operations.

## Consequences

Slow fixture generation no longer consumes the fresh transition admission window. Safety checks and bounded helper transfer still take time and can independently exhaust that window; such a failure must remain visible. The preparation uses the existing normal background runtime before switching to native workers, so all post-transition safety/queue checks remain mandatory. This is not a capacity improvement claim.

## Failure and recovery behavior

Fixture preparation failure prevents fresh admission, activation, observer startup and paid dispatch. Previously created fixtures retain their exact ownership and existing independent retirement/secret cleanup. Failures in safety, admission, migration or observation still stop dispatch and invoke full cleanup/restoration. Spent scopes remain consumed; any next experiment needs a fresh identity.

## Validation evidence

Executed cloud evidence: ADR0288 short PASSED_RESTORED; hour attempt FAILED_RESTORED with zero paid traffic. Retained child traceback names the unchanged freshness validator; local inventory age was299.108019seconds. No failed result is upgraded.

Before a new cloud run, test fixture-before-refresh ordering, unchanged safety-before-stage.prepare and observer-before-dispatch, and cleanup when preparation fails. Keep original freshness rejection tests. Full hourly qualification and additional capacity remain unmeasured.

Executed verification:204 targeted fixture ordering, lifecycle, freshness rejection, statistics, source binding, metric capture, hourly and entry tests passed. The regression simulates400s of bulk preparation followed by fresh capture and130s of safety work, proving the preparation delay precedes admission; failed fixture creation or admission prevents activation/dispatch and retains cleanup. Existing301s-old and future-dated inventory rejection tests passed unchanged. One separately registered fresh84/s hour may follow under the standing envelope and5400s ceiling, using the passed ADR0288 short application runtime; this setup-only correction requires no additional paid short comparison or infrastructure change.
