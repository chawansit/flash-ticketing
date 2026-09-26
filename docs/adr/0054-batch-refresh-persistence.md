# ADR 0054: Batch refresh leases and acknowledgements

Status: Rejected for deployment after failed ten-row and two-row safety stages.

## Context

The three-maintenance-worker 1,000 RPS diagnostic drained refresh work but failed under sustained load. The RDS observer saw as many as 17 simultaneous waiters dominated by `WALWrite` and `WalSync`, and API commit latency exceeded 300 ms during admission bursts. The 15-minute run dropped 14,939 scheduled requests and exhausted 2,167 retries.

Each refresh currently commits one transaction to acquire a lease and another transaction to acknowledge completion. At roughly 59 refreshes per second this creates about 118 write-transaction commits per second before hold, expiry, outbox and inbox writes. The Redis write is correctly performed outside a PostgreSQL lock, and the generation and lease-token fences must remain intact.

## Decision

Claim up to two due event refreshes in one PostgreSQL transaction with one shared, unique batch lease token. Write each event snapshot to Redis outside the transaction. Acknowledge every successful event in one completion transaction using its captured generation, event identifier, lease token and claim timestamp. Preserve per-event generation fencing and cooldown behavior.

Process every claimed row even when one Redis write fails. Commit acknowledgements for successful rows, leave failed rows leased for the existing 30-second recovery window, and then surface the first failure. Keep `refresh_one` as a one-row compatibility wrapper. Make the worker batch size configurable from 1 through 100 with a default of 2.

## Alternatives considered

- Add more maintenance replicas: rejected because three replicas drained the queue but increased WAL contention and degraded the API.
- Extend the audit window: rejected because it hides insufficient throughput.
- Hold one PostgreSQL transaction open while writing Redis: rejected because Redis latency would extend row locks and connection occupancy.
- Remove the durable lease: rejected because worker death could permit concurrent projectors without a recovery boundary.
- Batch hold, expiry and refresh writes together: rejected because they have different correctness and recovery scopes.

## Consequences

The normal batch reduces refresh lease and acknowledgement commits by up to twofold. Row updates and Redis operations remain per event. A larger lease group means one worker death can defer up to two events until lease expiry, bounded by the existing 30-second lease. The configuration limit prevents unbounded recovery delay or transaction size.

## Failure and recovery behavior

If claim commit fails, no row is leased. If Redis fails for one event, successful events are acknowledged and failed events retain dirty generations and expire their leases for replay. If the completion transaction fails, none of its acknowledgements commit and every claimed event is replayable after lease expiry. A change arriving during projection increments generation; completion acknowledges only the captured generation and leaves the newer generation dirty.

## Validation evidence

Integration tests must cover multi-event claim and completion, partial Redis failure, generation changes during projection, stale lease tokens and replay after failure. The full suite and formatting checks must pass before deployment.

Cloud validation starts with one maintenance worker, two Kafka consumers and a five-minute 1,000 RPS safety stage so batching is isolated from replica scaling. It must preserve zero double-booking and exact acknowledged durability, end with zero queues, reduce WAL wait pressure and keep latency within the established targets. A longer stage is prohibited until every safety gate passes.


Implemented validation on 26 September 2026: focused Ruff checks passed; 131 unit tests passed; seven focused PostgreSQL/Redis integration tests passed, including partial batch failure, concurrent generation change, stale lease fencing and replay after a lost acknowledgement. The complete suite ran against local PostgreSQL and Redis with 189 passed, two skipped and two dependency deprecation warnings. No cloud capacity result is claimed yet.


The first cloud safety stage used batch size 10, one maintenance worker and two consumers at revision `53dc656`. It failed and prohibits promotion. Of 300,000 scheduled requests, 17,564 were dropped by the generator. There were 9,846 first-attempt admission failures, 6,210 exhausted hold retries, read p95 of 762.981 ms and hold p95 of 1,580.281 ms. All 7,934 acknowledged holds were durable, overlap was zero, queues drained and rollback succeeded.

The RDS trace shows the tradeoff: maximum interesting waiters fell from 17 in the unbatched three-worker run to 8 and WAL volume was 318.8 MB, but `wal_buffers_full` increased from zero to 474. A ten-row batch therefore reduced commit concurrency while creating damaging WAL bursts. Batch size 10 is rejected. The next isolated candidate is size 2 with one maintenance worker and two consumers; it must repeat the five-minute safety gate before any longer test.


The batch-size-two safety stage at revision `b8597aa` also failed. It dropped 18,723 of 300,000 scheduled requests, produced 9,212 first-attempt admission failures and 5,989 exhausted hold retries, and recorded read/hold p95 of 754.670/1,625.747 ms. Its 8,025 acknowledged holds were durable, overlap was zero, queues drained and rollback succeeded. Refresh generation and completion both reached 16,050, with final Kafka lag and pending refresh at zero.

Unlike batch 10, this run recorded zero `wal_buffers_full`, but a timed checkpoint completed with 719,564 ms of checkpoint write time during the observation window and WAL waiters still peaked at 12. The result cannot isolate batch size from accumulated RDS/checkpoint pressure, but every service gate failed. Both batching candidates are rejected for deployment. Runtime code is restored to the previously tested unbatched implementation while this ADR remains as negative evidence. Any future comparison must begin after an idle baseline and run an unbatched control immediately before the candidate.
