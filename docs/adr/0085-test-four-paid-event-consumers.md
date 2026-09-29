# 0085 — Test four paid-event Kafka consumers before changing fulfillment transactions

- **Status:** Accepted as a bounded diagnostic experiment only; the deployed rollback topology remains one consumer.
- **Context:** A 45 paid-journeys/s, one-callback stage with two consumers failed because 432/2,700 scheduled journeys could not enter the generator's bounded 500 in-flight set. The two-member Kafka group accumulated up to 912 records of lag across all six partitions; PAID orders awaiting tickets peaked at 223. PostgreSQL lock waiters were zero in sampled data, but the observation does not prove that extra consumers will help or that the database has unlimited headroom. The 30/s stage passed with two consumers. ADR 0082 compared one and two, while proposed ADR 0084 describes a future transaction-level optimization; this experiment isolates consumer parallelism first.
- **Decision:** Permit an opt-in four-consumer candidate in the existing checkout stage runner. Repeat the same isolated 45/s, 60-second, one-callback workload, four API replicas, three DB connections and twelve waiters per API, eight simulator threads, six Kafka partitions, admission, fixture size, polling and zero generator retries. Confirm all four group members and collect partition lag, consumer busy time, RDS/DB wait observations, buyer outcomes and post-TTL exact audit. The stage may pass only with zero generator drops, zero unexpected HTTP errors, all 2,700 distinct paid tickets by deadline, no duplicate bookings, drained queues and successful rollback. Restore one consumer afterward regardless of result. Do not extrapolate to one-hour capacity.
- **Alternatives:** Implement paid-event batching first changes transaction design and parallelism at once, obscuring attribution. Increase generator in-flight capacity only masks slow issuance and can increase request pressure. Add Kafka partitions is unnecessary when all six already carried lag and four consumers fit within the current count. Issue tickets in the payment webhook increases synchronous transaction coupling.
- **Consequences:** Four consumers may reduce per-consumer partition load but also increase potential client DB connections and PgBouncer contention. CPU and WAL work per event are unchanged. Four of six Kafka partitions will have one owner each and two owners will handle two; expect uneven work. This is an experiment, not a promoted production setting.
- **Failure/recovery behavior:** Any strict gate failure stops escalation. Kafka offset commits remain after durable inbox/ticket transactions; replay remains idempotent. If a consumer restarts, Kafka rebalances partitions and processing resumes. The runner retires synthetic fixtures, audits accepted payments after TTL and restores the original one-consumer deployment; partial rollback is treated as failure.
- **Validation evidence:** The [two-consumer failed stage and partition lag](../capacity/flash-sale-opening/paid-ticket-kafka-lag-2026-09-29.json) motivate the comparison. The four-consumer stage has not run at decision time. Record revision, exact workload, strict verdict, Kafka lag, post-TTL audit and rollback before evaluating the candidate.

## Executed evidence

The one-minute 45/s candidate passed: 2,700/2,700 distinct paid tickets
by deadline, zero generator drops or retries, Kafka lag peak 142 and
hold-to-ticket p95 5.00 s. Post-TTL audit verified zero duplicate
bookings and empty queues. The runner restored one consumer. A following
60/s diagnostic with the same candidate failed: 828/3,600 scheduled
journeys dropped, hold-to-durable p95 12.49 s and hold-to-ticket p95
21.04 s. Kafka lag peaked at 206, PAID orders waiting for ticket at 36,
and all 2,772 accepted payments had unique tickets after TTL. Rollback
again restored one consumer. Four consumers remain an experimental
candidate, not a production promotion. See the
[aggregate comparison](../capacity/flash-sale-opening/paid-ticket-four-consumers-2026-09-29.json).
