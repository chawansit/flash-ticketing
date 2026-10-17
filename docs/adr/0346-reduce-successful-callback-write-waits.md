# ADR0346: Reduce successful callback write waits

## Status

Accepted for default-off implementation and local qualification only. Extends existing callback persistence without superseding its financial/locking/idempotency rules. Does not change or publish the qualified ADR0344 image or authorize any paid load.

## Context and300/s budget

The ADR0343 observer-lifetime means were16.282ms claim,78.231ms successful HTTP delivery and16.434ms acknowledgement. These are not a matched paid-window cycle measurement. For the existing one-ticket/one-callback workload,16delivery slots at300callbacks/s require average occupied cycle time at most53.333ms. If78.231ms delivery persisted, even zero claim/ack overhead would imply only about204.5callbacks/s. This is a conditional concurrency budget, not measured maximum capacity. The simulator correction alone cannot establish300/s. API CPU3.55/4cores and acquisition24.26ms also show little headroom at the failed load.

The successful callback already holds order, payment, hold and sorted seat locks and reads a fresh database clock before its financial decision. It then sends independent payment/booking/seat/hold/order/outbox writes sequentially. Their results do not drive subsequent successful-BOOK decisions. Existing API read, payment-initiation and reservation-boundary pipelines had mixed/rejected results; their flags remain unchanged.

## Decision

Add PAYMENT_CALLBACK_WRITE_PIPELINE, default off. On the synchronous API payment database, enable the existing driver's inner pipeline only around writes after a BOOK decision. Preserve all preceding reads, lock order/NOWAIT behavior, post-lock clock/expiry validation and callback identity insertion. Flush/check the write pipeline before returning to the outer transaction; commit durably before the webhook reports success. Other outcomes, refunds, semantic duplicates, asynchronous receipts, API read and payment-initiation paths remain sequential/unchanged. Reject this flag with asynchronous confirmation rather than imply the asynchronous worker implements it.

Compare actual callbacks off/on locally in alternating trials and run financial, concurrency, rollback, expiry and signed-HTTP recovery regressions. Keep only a locally demonstrated improvement; do not extrapolate a local percentage into cloud throughput. This is a separate candidate from ADR0344; a combined/new image and source registration would require explicit preparation/verification before cloud use. Keep the already qualified image and receipts intact.

## Alternatives

Increase simulator slots: previous32-slot experiment worsened contention; not a repair to service cost. Increase API/backend resources: outside the unchanged budget. Extend cache age, reduce workload, acknowledge before commit or suppress financial/outbox writes: rejected. Pipeline dependent locking/clock reads or all refund paths: unnecessary correctness/protocol risk. Re-enable earlier rejected pipelines: unsupported by their evidence.

## Persistence, locking, messaging, idempotency, TTL and scaling

PostgreSQL remains authoritative. Order→payment→hold→sorted-seat locks and the fresh post-lock expiry check remain in place. One callback still commits exactly the same financial rows and transactional outbox events. Callback/payment IDs and same/different-ID duplicate semantics remain unchanged. No new messages, store, lease, TTL, polling interval, worker, thread, client or backend is introduced.16delivery slots,10simulator DB clients,24physical backends and four API CPU cores remain the envelope. No network call occurs inside the financial transaction.

## Failure and recovery behavior

Any queued SQL/pipeline/commit error rolls back the existing outer transaction and propagates through existing API/worker recovery. Never report success for an uncommitted financial result. Lost responses retain the existing callback replay identity/lease behavior. Default off or restore the immutable parent on regression. Cleanup/reconciliation and failed quality remain separate. No automatic paid rerun, resize or gate relaxation.

## Validation evidence

Before implementation, inspected retained ADR0343 phase counters and actual callback/API source. Executed24 native financial/rollback/concurrency/expiry/signed-HTTP recovery and benchmark checks plus5 preparation guards:29 passed. Ruff passed. Six alternating64-callback trials per arm (384 successful callbacks each) measured median13.3175ms off versus10.3392ms on,22.364% lower local transaction time with identical statement counts. This excludes HTTP delay/cloud contention and is not a cloud throughput gain. Logs: tmp/adr0346-local-tests.log; benchmark: tmp/adr0346-callback-comparison.json.

Actual preparer scripts/prepare_callback_write_pipeline_image.py built local image flash-ticketing-shared:adr0346-19da6fb554d1; network-disabled verification passed all22 runtime module hashes and callback default-off/synchronous authentication checks. Immutable-parent packaging takes only the reviewed callback method/config/wiring plus ADR0344 worker, excluding pre-existing checkout authentication/payment-initiation drift. Component financial tests ran on checkout; exact-image verification checks packaging/imports rather than repeating the financial cohort. [Compact local receipt](../capacity/cce/callback-write-pipeline-local-2026-10-17.json) retains identities and limits. No registry publication, deployment, paid allowance, cloud load or new database reconciliation occurred. Existing ADR0344/0345 receipts remain unchanged.
