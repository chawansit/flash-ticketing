# ADR0225: Pipeline reservation command writes

Status: Proposed; default disabled, cloud activation pending a bound comparison

## Context

The ADR0224 index comparison restored monitoring coverage but dropped 777 of 25,200 offered journeys. Every dispatched journey completed. Three reservation writers were 99.94% busy during the offered window, PostgreSQL batches averaged 132.33 ms for approximately four commands, and command age averaged 4.43 s. Individual SELECT/INSERT/UPDATE means were around 2 ms while commit averaged 4.72 ms. Writer CPU was only 0.336 aggregate cores: serialized database work is a measured bottleneck. This does not establish that network latency explains every millisecond.

## Decision

Add a default-disabled RESERVATION_WRITE_PIPELINE flag. Enable it only for reservation writers. After existing replay, idempotency, sale, expiry and sorted NOWAIT seat-lock validations succeed, queue the unchanged hold, order, seat, item, outbox, idempotency-response and command writes in one psycopg pipeline per command. Exit and synchronize that pipeline before returning the command outcome or releasing its existing savepoint. Keep the same outer transaction, bounded batch size, commit-before-Redis-marker/ACK ordering, TTL, unique constraints and retry behavior. No batch-wide speculative validation, new connection, new writer or wider lock ownership is introduced.

This extends [ADR0063](0063-bounded-reservation-persistence-batching.md); its savepoint isolation and batch commit semantics remain accepted. [Psycopg pipeline documentation](https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html) specifies synchronization and server-error behavior. Query execute histograms inside an enabled pipeline measure enqueue/driver work rather than full server round trips; compare the existing complete postgres_batch and connection-hold measurements for elapsed persistence, and do not treat smaller individual query metrics as faster execution proof.

## Alternatives

- Increase the batch from four to eight: amortizes commit and savepoints but retains sequential command SQL latency and longer locks.
- Add writers or connections: may amplify WAL contention; current writer CPU is low and database waits already exist.
- Rewrite all writes into a stored procedure or multi-statement query: larger correctness and deployment change; pipeline the existing ordered statements first.
- Weaken hold durability or return successful payment before commit: rejected.

## Consequences

Fewer write-side network synchronization points may shorten lock and connection hold time. Server execution and WAL work remain, and the additional order index has maintenance cost. No cloud throughput gain is assumed. The flag defaults off and must be isolated in a frozen writer image before a future comparison; unrelated local application changes must not enter that image.

## Failure and recovery behavior

Pipeline exit must observe SQL errors before returning an outcome. SQL/transport errors propagate to the existing outer rollback; no Redis durable marker or stream ACK occurs. Domain failures before write submission retain their existing per-command savepoint behavior. Commit-before-ACK ambiguity replays by command/idempotency identity. A pipeline write failure cannot leave earlier writes durable independently. Disable the flag to restore the existing transport behavior; no schema rollback is needed. Do not automatically fall back after an ambiguous write.

## Validation evidence

Implemented default-disabled writer-only activation. Executed 60 native PostgreSQL 17.6 / Redis 7.4.5 tests on the final source in 22.23 s, covering both modes, contention, SQL/commit failure and replay. Executed 46 affected runner/recovery/configuration/metrics regression tests in 48.94 s. AST comparison confirms every validation, SQL literal, parameter and return is unchanged inside the transport wrapper. Owned test containers were removed. [Local evidence](../capacity/flash-sale-opening/writer-write-pipeline-local-2026-10-08.json). The initial missing local Redis tag and successful first verification are retained and accounted for; no failed cloud stage is rewritten. Required checks cover 100 contenders/one winner, mixed domain failures, injected SQL failure after queued writes, outer commit failure, commit-before-marker replay and complete queue drain in both modes. After local qualification, bind one future 84/s, 300-second comparison to ADR0224 with unchanged index, images for other roles, budgets and gates. Failed financial/count gates block hourly qualification. No cloud activation under this decision has occurred.
