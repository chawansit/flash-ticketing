# ADR0316: Test larger existing reservation batches at fixed four-writer capacity

Status: Rejected for adoption; bounded comparison executed and restored.

## Context

The four-writer reference ADR0315 issued 30,370 tickets within five minutes, but dropped 71 journeys. Both generator shards reached their 250 active-journey limits around second 269, with oldest active journeys already 16Ã¢â‚¬â€œ17 seconds old. No completed-task accounting leak was present in retained drop snapshots. Failure-time captures show payment requests in commit and general status reads waiting in BEGIN, alongside WALWrite and DataFileRead waits. Checkpointer counters did not change across the inspected stall. This correlates transaction/IO slowdown with the customer backlog; the underlying storage cause is not established.

The deployed application already supports reservation batches between one and eight commands, savepoint-isolated domain failures, atomic claim/replay handling and durable commit before Redis acknowledgment. The reference used a maximum of four commands and averaged approximately 3.28 commands per successful PostgreSQL batch.

## Decision

Use the existing bounded cloud runner for one corrective comparison: maximum reservation-writer batch size four to eight, with four writers unchanged. Select only through an exact ADR0316 contract and the authenticated, independently reconciled ADR0315 reference. Keep the same immutable image, all other role settings/counts, API 2+2 slots, 24 physical database connections, admission 20, generator concurrency 500, gateway/client recovery, 102 offered journeys/s and 300-second offered window. No additional resources, image build, retry change or waiting-room implementation.

Only native reservation-writer environments change RESERVATION_WRITER_BATCH_SIZE from 4 to 8. All other native roles remain identical. Capture and restore the original ECS environments unchanged. This supersedes ADR0315's four-command batch limit only for this selected experiment.

## Alternatives

Adding a fifth writer increases database concurrency during a measured database stall. Raising generator concurrency changes the customer-backlog tolerance and the comparison. Changing RDS durability or suppressing initial errors weakens the evaluation. A broader transaction rewrite is deferred until the existing bounded batching option is measured.

## Consequences

Larger available batches can reduce transaction/commit frequency and amortize connection and round-trip overhead, especially during backlog. Sparse traffic must not wait to fill a batch. Larger transactions may hold locks longer or increase tail latency; improvement is a hypothesis, not guaranteed. Per-command writes, WAL records and financial guarantees remain required.

## Failure and recovery

Retain zero generator drops, current latency gates, at most 0.01% final customer failures, zero duplicate charge/double-booking/payment loss, in-window target of 30,000 unique paid-issued tickets, complete queue drain and restoration. Stop escalation on failure. Preserve the failed original result and reconcile all actual dispatched orders independently before another test. Rollback restores original ECS workers and batch size four; use a fresh experiment identity.

## Validation evidence

[Retained stall attribution](../capacity/cce/headroom-stall-attribution-2026-10-13.json). Executed 54 PostgreSQL17/Redis7 integration tests covering atomic holds, replay, batch failure isolation and post-commit Redis failure; owned local test containers removed. Exact native-writer-only setting tests and full current binding validation passed. A controlled RDS18 cloud comparison remains required; no batch-eight paid traffic has started.

Executed cloud result: adr0151-3f899bb32f0e, original raw aa6875dbd7ab0669159a60a28c21e97e81f143792f347acc71249f7daeb58c92. Scheduled 30,600 journeys; dispatched 27,975, dropped 2,625 and confirmed 27,974. Of 121 initial-error journeys, 120 recovered; one command_http_502 journey remained failed. Final failure rate was 0.00357462%, but the zero-drop and in-window capacity gates failed. The original known-outcome classifier also did not accept that command error; no retrospective qualification is claimed.

Inside-window issuance fell from 30,370 to 27,532 tickets. Reported journey p95 increased from 3.353 to 14.329 seconds. Successful writer batches fell from 9,307 to 6,016, while mean writer connection-hold time rose from 78.571 to 134.404 ms. The larger batch did not improve this measured run; per-command persistence work and tail database contention remain relevant.

Independent read-only closure reconciled 27,974 paid-issued orders and one expired unpaid order, all seven relationships and both safety payments. No duplicate booking or payment loss was found. Queues drained, owned CCE pods/helpers/credentials were removed, and original ECS identities were restored. The ledger closed FAILED_RESTORED; capacity_qualified remains false. Normal ECS restoration retains three writers/batch four; four writers/batch four remains the preferred experimental candidate, not a newly qualified production deployment.

Final executed validation: 225 focused unit tests, 54 isolated PostgreSQL17/Redis7 integration tests and changed-Python Ruff passed. No further paid load followed the failed gate. One sequential comparison does not establish that batching alone caused the tail stall.

[Failed comparison and independent recovery](../capacity/cce/reservation-batch-comparison-2026-10-13.json).
