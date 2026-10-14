# ADR0320: Pipeline reservation transaction boundaries

## Status

Rejected after local protocol validation; do not deploy this candidate. No cloud load or capacity improvement was measured. Extends ADR0225 with a separate default-off option; does not supersede its independent-write pipeline or adopt the rejected ADR0319 payment context.

## Context

The retained shared-pool four-writer reference issued 30,370 tickets inside 300 seconds but dropped 71 journeys. The ADR0319 payment-only candidate issued 29,832 with 320 drops. Its writer batch mean rose from 78.687 to 109.800 ms; batch occupancy also changed from about 3.28 to 3.67 commands. This comparison does not establish the cause of the slowdown. Query attribution shows increased execution costs and WAL-related waits; WAL timing was disabled and checkpoint counters did not change within the inspected 30-second buckets. Do not attribute an underlying storage failure from these observations.

The accepted independent-write pipeline still leaves transaction setup and savepoint boundaries outside pipeline mode. Explicit BEGIN, transaction-local timeout setup and the first dependent lookup can share a protocol flush without changing their server execution order. [Psycopg pipeline documentation](https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html) documents fetch and synchronization boundaries; local tests must establish actual behavior with our savepoints rather than assume a throughput benefit.

All API snapshots in ADR0319 were collected, but bursts of 19, 22 and 32 acquisition failures between one-second samples exceeded a 16-record ring. Exactly 25 of 131 failure records were missing across three APIs. Keep the original diagnostic failure visible. This decision does not alter the ring, sampling frequency or evidence gates.

## Decision

Add RESERVATION_TRANSACTION_PIPELINE, default off, usable only with RESERVATION_WRITE_PIPELINE. Apply it exclusively to the reservation-writer store. When enabled, invoke the existing database transaction pipeline around the bounded batch, retaining each command's existing savepoint and nested independent-write pipeline. Foreground holds, payment initiation, callback handling and order status remain unchanged. Fetches still resolve every dependent query before decisions; the fresh database clock remains after sorted seat locks.

Prepare any candidate from the exact ADR0298 immutable shared image, changing only the constructor/batch method, its configuration field and reservation-writer wiring. Reject unrelated source or dependency drift, including the rejected payment-context method. Before a paid comparison, require actual local protocol/correctness evidence and immutable image verification. Retain four APIs with 2+2 client slots, four writers/batch four, shared 24 backends, all other roles, 500 generator concurrency and unchanged workload/gateway/recovery gates. Use a fresh bounded experiment identity.

## Alternatives

Increase writer batch size: ADR0316 worsened results. Repartition backends: ADR0317/0318 failed. Reduce commit durability, remove savepoints, ACK before commit or move expiry checks before locks: rejected for correctness. Add compute or backend slots: confounds the fixed-budget comparison. Increase the diagnostic ring in the same application candidate: would introduce another application factor; address separately if needed. Reapply rejected payment/read pipeline candidates: no demonstrated system benefit.

## Consequences

The hypothesis is fewer sequential protocol waits while holding a writer connection. SQL statements, writes, commit count and batch limits remain unchanged. Nested pipeline synchronization may reduce or eliminate the expected saving; measure it before cloud adoption. There is no claimed RPS gain or production qualification. Existing defaults remain off and previous accepted writer behavior remains selectable.

## Failure and recovery behavior

Preserve atomic Redis hold intake, authorized idempotent replay, sorted NOWAIT seat locks, post-lock expiry checks and payment correctness. Domain failures roll back their savepoint; database/protocol/commit failures roll back the whole batch and leave stream commands replayable. Redis durability publication and ACK occur only after outer commit. Fail the option combination early if independent-write pipelining is disabled. On regression, disable the new option or restore the immutable parent; preserve failed evidence, reconcile actual payments/tickets, drain queues and restore runtime. No retries hide overload and no missing diagnostic records count as complete.

## Validation evidence

Executed before implementation: retained trace/query attribution inspection, exact failure-sequence coverage calculation, and official Psycopg documentation review. Application correction, protocol comparison, regression tests, image publication and cloud load have not yet executed. Append only executed evidence.


## Executed local validation and rejection

Built and source-verified all 22 copied/installed/imported modules of the isolated image with unchanged dependency inputs. Executed 97 PostgreSQL17.6/Redis integration cases and 22 focused configuration/connection checks; queued SQL failure, commit failure, atomic holds, deterministic domain isolation and replay checks passed. Owned test resources were removed. A first protocol probe could not run on Windows because libpq tracing is Linux-only; it was repeated successfully inside the exact Linux application image.

Eight alternating local four-command trials per arm measured median batch time of 60.920 ms for the existing inner-write pipeline and 67.419 ms with outer transaction pipelining. ReadyForQuery messages decreased36to20, while explicit Flush messages increased4to33. Message counts are not interchangeable with network round-trip counts. The candidate did not demonstrate lower local batch occupancy. Reject before paying for a cloud comparison. Restore the application/configuration/test-matrix changes; preserve the exact experimental source overlay and local evidence for reproducibility. This is no RDS performance claim.

[Local evidence](../capacity/cce/writer-transaction-pipeline-local-2026-10-14.json). Next evaluate a single removed dependent lookup under ADR0321.
