# ADR0368: Batch due callback claims within free delivery slots

## Status
Accepted2026-10-19 for implementation and local validation under the requested payment-error fix. Cloud progression still requires exact immutable-image and resource validation, followed by fresh180 and conditional200.

## Context
Run4575dd20f256 had98payment503errors and1payment502. Retained payment-slot records show commits held for500–640ms, coinciding with PostgreSQL WalSync/WALWrite waits. The300s observation contains122232simulator transactions, versus160827API transactions. The simulator opens one fenced claim transaction and one acknowledgement transaction per callback;16independent claim threads also poll empty work. RDS is user-reported8vCPU/16GB CloudSSD with advertised50kIOPS/350MiB/s. This does not prove a sizing shortage. Read-only inspection confirms synchronous_commit/fsync on and commit_delay0; runtime role lacks SET privilege for commit_delay. No database tuning or resize is selected.

## Decision
Add default-off SIMULATOR_BATCH_CLAIM. For the selected simulator only, let its scheduling loop claim at most the number of free slots, capped by the unchanged16delivery slots, using one transaction and the existing atomic UPDATE-from-locked-candidates pattern. Preserve due-time ordering, SKIP LOCKED, fresh post-lock15-second leases and an opaque lease token. A batch uses one token fenced together with each payment ID. Commit claims before any HTTP delivery. Submit callbacks to the existing bounded executor; a completed slot can be refilled independently without waiting for other deliveries.

Keep callback payload/signature/ID, HTTP deadline, acknowledgement SQL and per-callback durable acknowledgement unchanged. Duplicate callback processing stays idempotent in the API. Crash after claim or lost acknowledgement leaves the existing lease retry path. No change to payment SQL, locking, financial idempotency, cache TTL, Kafka topology, connection/admission bounds,500ms acquire timeout, resources or measurement criteria. New immutable image also preserves UUIDv7 and projection fixes.

## Alternatives
Increase acquisition timeout or relax clean-run rules: rejected. Resize or change RDS tuning without a measured need: not selected. Batch acknowledgements too: deferred to avoid introducing an acknowledgement barrier or queue. Server commit_delay tuning: not available to runtime role, and benefit is not established. Keep one claim transaction per callback: retains avoidable transaction/backend demand.

## Consequences
At full16-row claim availability, simulator claim transactions reduce from16to1 while all16HTTP slots remain independent. Sparse arrival may yield smaller batches; lower database demand is a hypothesis for cloud improvement, not a capacity claim. One failed claim rolls back its bounded group before delivery; one failed callback cannot acknowledge another row. The default-off legacy path remains available for rollback.

## Failure and recovery
Reject invalid limits, malformed claims and unavailable slots. Preserve pending futures and drain them on stop; never release a claimed lease without its exact acknowledgement token. Any cloud quality/observer/financial failure stops progression. Restore normal application, queues and owned resources; never reset consumed rounds or promote historical failures.

## Validation evidence
Actual published-image PostgreSQL suite51passed/1optional callback comparison skipped. Tested bound/eligibility/SKIPLOCKED, concurrent disjoint claims, fresh leases, committed lease visibility beforeHTTP, lost-response/stale-token duplicate recovery, group rollback and independent refill/stop drain. Sixteen due callbacks used32legacy versus17candidate simulator transactions,46.875%fewer; callback financial writes remained unchanged. Broad runner136passed/2optional skipped, final capture41passed, actual22-helper/22-runtime rehearsal, extended transport, strict19pod resource/manifests and isolated real-parent admission passed; Ruff/local owned cleanup passed. Initial missing local test dependencies corrected and initial local resources cleaned before the final51-case run. Registry manifest3096299e05160707aacc41309a483b39d5fcdfb21ec62d0ac8db823ee9828390 verified after authenticated pull. No new cloud throughput improvement claimed by these local checks.
