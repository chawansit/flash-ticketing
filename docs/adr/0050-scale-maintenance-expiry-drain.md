# ADR 0050: Scale the maintenance worker for expiry-drain capacity

Status: Accepted for a controlled two-worker capacity experiment; one worker remains the rollback configuration.

## Context

At 800 RPS with a 95% conditional read / 5% unique hold workload, the existing one-worker maintenance loop saw at most 79 overdue holds and 1.9 seconds oldest overdue age during the 30-minute run. At 1,000 RPS, approximately 50 holds per second became due after the 120-second default hold TTL. The same observer recorded overdue holds growing from 780 at minute 4.1 to 5,958 at minute 14.5, with a peak of 6,425 and an oldest age of 128.5 seconds. Database lock waiters in those samples remained zero. The fixed 180-second audit after the 15-minute run found 2,539 overdue holds and 751 pending seat-refresh requests. A later audit found all queues drained and no duplicate booking or broken link.

The current maintenance service has one replica and executes refresh and expiry operations serially. Each expiry uses a PostgreSQL transaction; refresh claims an event row with a lease. This evidence is consistent with a worker-throughput limit but does not prove that PostgreSQL can support unlimited maintenance concurrency.

## Decision

Test two maintenance replicas on the same ECS while keeping the API, PgBouncer, workload, hold TTL, and 180-second audit gate unchanged. The capacity-stage deploy records and verifies the original maintenance replica count, scales to two, and rolls back to the original count even if the stage fails. Keep the database-backed claim semantics: expiry selects an order using `FOR UPDATE SKIP LOCKED`; refresh selects and leases a request using `FOR UPDATE SKIP LOCKED` and a lease token. No new Redis lock or best-effort duplicate suppression is introduced.

Measure overdue holds and queue state during load and at the existing post-TTL audit. Count the result as passing only if request, durability, overlap, queue-drain, and rollback gates all pass. Compare with the one-worker 1,000 RPS baseline. A 30-minute run is required before calling 1,000 RPS a sustained validated level.

## Alternatives considered

- Increase the 180-second drain allowance: rejected as it would hide the observed backlog instead of increasing processing capacity.
- Increase the hold TTL: rejected because it changes the customer reservation contract and does not raise expiry throughput.
- Rewrite expiry as a bulk transaction immediately: deferred until a two-worker experiment shows whether concurrency is sufficient; a bulk write changes lock duration and failure scope.
- Add more API replicas or admission permits: rejected for this experiment because HTTP requests already passed and more holds may worsen expiry backlog.
- Give refresh and expiry separate processes immediately: deferred until measurement shows which operation is limiting throughput.

## Consequences

A second worker consumes CPU and database connections and may increase WAL or lock pressure. It may also reduce the time spent on refresh per worker. The experiment is bounded to two replicas and the original count is restored automatically. The load generator's HTTP retry remains opt-in and reported separately.

## Failure and recovery behavior

If scaling or worker readiness fails, the stage stops before load and restores the original maintenance count and API admission. If a worker stops mid-expiry, PostgreSQL rolls back the transaction and another worker can claim the row. If a refresh worker stops, the lease expires and another worker can retry. A failed audit remains failed even if a later audit proves eventual drain.

## Validation evidence

Before implementation: the 800 RPS and 1,000 RPS observer traces above, the original and later 1,000 RPS read-only audits, and zero-overlap checks. After implementation: stage orchestration tests, shell syntax check, a 1,000 RPS five-minute safety run, then a 15-minute comparison run if every safety gate passes. Both must show the original maintenance count restored after the run.

The controlled two-maintenance-worker comparison ran at revision `2cad236` on 22 September 2026. The first five-minute 1,000 RPS safety run with four generator workers failed load fidelity (584 generator drops) and admission (17 first-attempt rejections, three exhausted retries); its 14,965 acknowledged holds were durable, overlap was zero, and all queues drained. The matched eight-generator-worker five-minute run delivered all 300,000 scheduled requests with no drops or late deliveries; 65 first-attempt admission rejections produced 55 recovered and ten exhausted holds. Its 14,990 acknowledged holds were durable, overlap was zero, and queues drained. The 15-minute eight-generator-worker diagnostic delivered all 900,000 scheduled requests, with 20 first-attempt admission rejections, ten recovered and ten exhausted. Read/hold worst-worker p95 were 13.335/78.029 ms. All 44,990 acknowledged holds were durable; no overlapping held-seat intervals or overdue active holds remained. At the fixed audit, however, 237 seat-refresh requests were pending, so the queue-drain gate failed. The observer had seen a peak of 54 overdue active holds with oldest age 0.994 seconds, compared with 6,425 and 128.5 seconds for one worker, but this does not rescue the failed refresh gate. Both candidate stages restored one maintenance worker and admission four. A later read-only queue probe returned zero pending refresh, outbox, dead letters and overdue holds; it does not alter the audit verdict.
