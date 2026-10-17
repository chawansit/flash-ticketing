# ADR0348: Bind combined worker inventory renewal

## Status

Accepted for local correction and qualification. Extends ADR0347 admission integration; no new cloud run or retry authorized by this decision. The stopped ADR0347 pair remains failed/restored with zero paid stages.

## Context

ADR0347 published/pulled its exact image, deployed19 ready pods and passed both safety financial cohorts, but inventory renewal rejected the ADR0345 worker role-layout adapter before paid dispatch: Exactly declared native writer scale required. Existing preparation/guard coverage missed this final observer-start integration. Source-bound qualification alone was insufficient.

## Decision

Declare the existing15-worker ADR0345 adapter in inventory renewal and require its exact active ADR0347 pair binding plus existing immutable worker-bundle validation. Keep same-run identity, all4API/15worker receipts, fresh live re-observation and unchanged background checks mandatory. Declare the already supported ADR0324 count consistently in the contract. Bind the changed renewal contract/source explicitly in the combined comparison profile. Retain native worker admission receipts locally before invoking renewal so future admission rejection cannot erase them during transition cleanup.

Exercise actual renewal using fully validated synthetic control/candidate runtime bundles and reject missing/mismatched pair/source/image/UID evidence. Exercise actual observer.start failure retention using the existing offline session fixture. Replay retained original inventory chronology; synthetic runtime proof is not cloud verification. Preserve original failure/raw hashes.

## Alternatives

Skip renewal, accept arbitrary15-worker bundles, extend inventory freshness, ignore identity validation or reuse an old passing control: rejected. Change worker/resource counts or bootstrap restoration: unnecessary. Automatically repeat the stopped pair: excluded by ADR0347 stop/consumption guards.

## Persistence, locking, messaging, idempotency, TTL and scaling

No application persistence/lock/message/idempotency/TTL/scaling change. PostgreSQL financial transactions and callback/simulator changes remain ADR0346. Four APIs,15workers,24backends,16delivery slots and existing freshness remain. Only local admission evidence retention/order and the exact recognized role adapter change.

## Failure and recovery

Missing/wrong pair or changed live worker/API/background identity still rejects before traffic. Retained worker evidence survives admission failure and normal mandatory cleanup. The original cloud failure remains FAILED_RESTORED; paid throughput is unmeasured. No automatic paid rerun, resize, gate relaxation or main merge.

## Validation evidence

Original run adr0151-5363d2b5b94c: zero paid stages/dispatched journeys; both safety financial audits and all cleanup/restoration passed. Executed20 focused renewal checks, then212 final renewal/observer/worker/profile/metric/compatibility checks passed with1 host-dependent symlink check skipped;20 are overlapping cases. Ruff passed. Actual generated manifests/plan/binding and real-publication reservation in isolated bookkeeping passed. Tests use explicitly synthetic validated worker runtime bundles and actual observer.start failure-retention; this is not cloud verification of the correction. [Compact local evidence](../capacity/cce/combined-inventory-renewal-local-2026-10-17.json). Owned logs: tmp/adr0348-focused-tests-final.log, tmp/adr0348-preparation-final.log, tmp/adr0348-control-admission-final.log. No further cloud experiment executed. The stopped pair/allowance was not reset; fresh approval is required before another pair.
