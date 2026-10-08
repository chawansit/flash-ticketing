# ADR0225: Pipeline reservation command writes

Status: Proposed; default disabled, bounded cloud comparison executed, full target qualification failed

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

The bounded comparison uses the proven paid runner with one fresh candidate identity against the retained ADR0224 reference: 84 offered journeys/s for 300 seconds, unchanged 1+3 API placement, connection budgets, consumer image and simulator settings. Verify the already-valid order index without DDL; activate the flag only in the three writer containers. Bind a reproducible ADR0163 export plus the three-file transport patch, image identity and complete per-role import hashes. Missing index, source/setting drift, or failed recovery proof stops dispatch. This registers the existing orchestration for this factor; it does not introduce a new load harness.

## Failure and recovery behavior

Pipeline exit must observe SQL errors before returning an outcome. SQL/transport errors propagate to the existing outer rollback; no Redis durable marker or stream ACK occurs. Domain failures before write submission retain their existing per-command savepoint behavior. Commit-before-ACK ambiguity replays by command/idempotency identity. A pipeline write failure cannot leave earlier writes durable independently. Disable the flag to restore the existing transport behavior; no schema rollback is needed. Do not automatically fall back after an ambiguous write.

## Validation evidence

Implemented default-disabled writer-only activation. Executed 60 native PostgreSQL 17.6 / Redis 7.4.5 tests on the final source in 22.23 s, covering both modes, contention, SQL/commit failure and replay. Executed 46 affected runner/recovery/configuration/metrics regression tests in 48.94 s. AST comparison confirms every validation, SQL literal, parameter and return is unchanged inside the transport wrapper. Owned test containers were removed. [Local evidence](../capacity/flash-sale-opening/writer-write-pipeline-local-2026-10-08.json). The initial missing local Redis tag and successful first verification are retained and accounted for; no failed cloud stage is rewritten. Required checks cover 100 contenders/one winner, mixed domain failures, injected SQL failure after queued writes, outer commit failure, commit-before-marker replay and complete queue drain in both modes. After local qualification, bind one future 84/s, 300-second comparison to ADR0224 with unchanged index, images for other roles, budgets and gates. Failed financial/count gates block hourly qualification. At that local checkpoint, no cloud activation had occurred; the later bounded comparison is recorded below.

Frozen writer image prepared from the ADR0163 export with only the three-file transport patch. Executed another 60 native tests against that exact frozen source in 23.42 s and 78 profile/envelope/index regression checks in 428.02 s. Complete source, dependency, installed-import and inherited-image checks passed. The newly registered profile verifies the index without DDL and rejects writer flag/image/import drift before dispatch. [Frozen qualification evidence](../capacity/flash-sale-opening/writer-write-pipeline-frozen-local-2026-10-08.json). At that checkpoint, cloud comparison remained pending.

The first cloud qualification stopped before paid dispatch because the diagnostic allocation selector omitted its contract type. Backend 100-way safety, authorization, payment replay, one paid ticket after TTL, queue drain and restoration all passed. The selector is corrected and the real diagnostic allocation path now has a regression check. Executed 77 affected selector/recovery/envelope tests in 119.59 s. ADR0218 resolves only this exact safety-only failure with a separate hash-bound receipt; the original failure remains unchanged. [Failed qualification](../capacity/flash-sale-opening/writer-write-pipeline-safety-abort-2026-10-08.json), [recovery receipt](../capacity/flash-sale-opening/writer-write-pipeline-safety-recovery-2026-10-08.json). That consumed scope was not reopened; a fresh qualification and paid comparison followed.

Fresh safety qualification passed, then the unchanged 84/s for 300 seconds comparison scheduled 25,200 journeys, dispatched and fulfilled 25,196, with zero dispatched customer errors. Four generator drops failed the original target/count gates. Durable reservation worst-shard p95 fell from 8,273.36 to 2,036.15 ms; hold-to-ticket p95 fell from 9,648.02 to 5,425.29 ms. Complete writer batch mean fell from 132.33 to 83.41 ms, while commands per batch changed from 4.0013 to 3.5515. Payment-to-ticket p95 rose from 1,616.08 to 3,970.27 ms and status checks per journey rose from 2.5755 to 3.7911. These measurements support the transport correction but do not qualify the full target or prove hourly capacity. [Cloud comparison](../capacity/flash-sale-opening/writer-write-pipeline-probe-2026-10-08.json).

All four drops occurred at one shard's configured 250-journey cap; three snapshots still counted completed tasks, while one contained 250 unfinished tasks. Preserve the true cap and investigate stale occupancy separately from callback-tail pressure. An independent read-only audit verified all 25,196 dispatched payments/bookings/tickets and relationships after TTL, zero duplicate bookings, all queues/Kafka drained, generator idle and exact original runtime restored. Executed 91 affected recovery/envelope tests in 8.44 s before that audit. [Independent recovery](../capacity/flash-sale-opening/writer-write-pipeline-recovery-2026-10-08.json). The original failed result and consumed scopes remain immutable; no hourly qualification or CCE deployment occurred.

Before publication, the final seven real-runner registration/identity regressions passed in 417.80 s and 24 recovery regressions (including the exact writer cohort and rejection cases) passed in 0.93 s. Ruff, naming, credential scan and exact staged/journal receipt hashes passed. These local checks do not convert the failed cloud target into a pass.
