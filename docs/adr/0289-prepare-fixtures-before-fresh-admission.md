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

Executed fresh hourly validation `adr0151-0ffc00f25bf2`: fixture creation completed before admission in 55.75 seconds; fresh inventory and all pre-dispatch checks passed. The original result was RECOVERY_REQUIRED. Independent terminal recovery then closed the scope as FAILED_RESTORED; neither the original raw result nor its failed gates were rewritten. The experiment including recovery consumed 4,877.609 seconds, below its 5,400-second ceiling.

The hour scheduled 302,400 journeys at 84/s, dispatched 297,619 and issued 297,218 unique paid tickets inside the offered hour (82.561/s), below the 300,000 target. Another 401 tickets completed after the hour. There were 4,781 generator drops, 602 journeys with initial errors, 617 retry attempts and zero final failures among dispatched journeys. Worst-shard journey p95 was 12,795.400 ms. This is a failed qualification, not a production capacity claim. See [hourly result](../capacity/cce/fixture-admission-hourly-result-2026-10-12.json).

The original scheduled-count financial gate expected 302,400 and remains false. The independent post-TTL audit separately verified all 297,619 executed journeys: exactly matching orders, successful payments, bookings and tickets; no relationship errors, duplicate booking or payment loss; queues drained; exact normal runtime restored; test namespace/helpers/private transport inputs removed and generator idle. See [independent recovery](../capacity/cce/fixture-admission-hourly-terminal-recovery-2026-10-12.json). Recovery correctness does not satisfy the missing offered workload or hourly throughput gates.

Statistics continuity passed with a 1.088645-second maximum gap, validating ADR0288 scheduling in this hour. Slot history remained incomplete: terminal counters recorded 640 failures, but 105 sequence records were absent from retained snapshots; terminal driver counters agreed and no conflicting duplicate records were found. Preserve that failure and do not synthesize missing occupancy evidence. See [offline slot-history diagnosis](../capacity/cce/fixture-admission-hourly-slot-history-2026-10-12.json).

Compared with the earlier numerical-target hour, the application image, source, configuration, pod resources, workload and connection budgets matched. Status reads rose from 2.133 to 4.251 per dispatched journey; hold pre-send p95 rose from 2.793 to 292.767 ms while application hold p95 stayed near 8 ms. This observational comparison changes diagnostic/setup ordering and does not isolate one cause. Generator transport, primary ingress, delayed event processing and database occupancy remain candidates; existing evidence lacks generator CPU/event-loop attribution and complete failure histories. Next, isolate that measured delay before one targeted correction and a fresh fixed-load comparison. No load increase, automatic spent-scope replay or extra infrastructure is justified by this result. See [retained comparison](../capacity/cce/fixture-admission-hourly-comparison-2026-10-12.json).
