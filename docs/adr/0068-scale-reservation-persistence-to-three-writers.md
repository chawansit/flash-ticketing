# ADR 0068: Scale Redis reservation persistence to three writers

Date: 2026-09-28
Status: Accepted for a bounded three-writer cloud experiment

## Context

ADR 0059 validated two reservation writers for a 1,000 RPS workload with 5% writes. ADR 0067 removed continuous idle discovery repair and materially improved the later 1,000 RPS, 6% write stage: generator drops fell from 10,191 to 673, read p95 fell to 33.852 ms and hold p95 fell to 60.337 ms.

The stage still failed exact durability. It accepted 10,755 provisional commands, made 10,640 durable and completed 115 as HOLD_EXPIRED. Failed commands averaged 136.671 seconds old. The two writers processed all stream work, but average persistence batch occupancy was only 1.043 of the configured maximum four and PostgreSQL batch time averaged 101.779 ms. PostgreSQL commit averaged 2.650 ms, the RDS observer saw at most five interesting waiters, all queues drained and ownership overlap remained zero.

Offered write traffic is about 60 commands per second. Two sequential writers are therefore operating at the measured boundary even though PostgreSQL commit and queue cleanup remain healthy.

## Decision

Run the next isolated 1,000 RPS, 6% write capacity stage with three reservation-writer replicas. Keep the batch-size limit at four, hold TTL at 120 seconds, DCS replica acknowledgement, four API replicas, PostgreSQL schema and all strict durability gates unchanged.

Use eight generator processes for the measurement. This changes only load-generation concurrency and removes the four-process generator scheduling limit already documented by ADR 0059. It does not alter the production service topology.

Three writers are an experiment, not a production promotion. Accept the topology only if every delivered provisional acknowledgement becomes exactly one durable PostgreSQL command, ownership overlap is zero, all queues drain at the fixed audit, latency gates pass and rollback succeeds.

This decision supersedes ADR 0059 only for the candidate writer count used by the 6% write experiment. ADR 0059 remains the accepted two-writer evidence for 5% write traffic.

## Alternatives considered

- Keep two writers and raise the hold TTL. This hides inadequate persistence throughput and changes the customer contract.
- Add a micro-batch wait. Current batch occupancy suggests it could reduce transaction overhead, but it adds intentional persistence delay and new timing behavior. Writer scaling is already implemented and is the smaller controlled step.
- Jump to four writers. Three should provide measured headroom over the approximately 60 command-per-second input while adding less database and Redis concurrency.
- Reduce writes to 5%. That would repeat the already validated ADR 0059 workload rather than test the requested 6% mix.
- Accept terminal expiry after HTTP 202. The capacity gate requires exact provisional-to-durable accounting.

## Consequences

The candidate adds one sequential PostgreSQL transaction producer and one Redis consumer-group member. It may increase WAL synchronization, row-lock contention and refresh work. Eight generator processes use the same aggregate 1,000 RPS and do not increase target traffic.

If three writers pass, the result establishes only this isolated three-minute stage. It does not establish sustained production capacity or authorize a higher RPS.

## Failure and recovery behavior

A stopped writer leaves pending entries in the Redis consumer group for another writer to reclaim. PostgreSQL uniqueness and idempotency records protect replay. Any expiry, missing durable command, overlap, undrained queue, observer failure or rollback failure rejects the candidate.

The unattended runner restores PostgreSQL reservation mode and zero reservation writers on every exit. No data migration is required to return to the previous topology.

## Validation evidence

The baseline is revision a258443 at 1,000 RPS for 180 seconds with 6% writes, four generators, two reservation writers and batch size four:

- 179,327 of 180,000 requests were sent; generator drops were 673.
- Read and hold p95 were 33.852 ms and 60.337 ms.
- 10,640 of 10,755 commands were durable; 115 expired.
- Average command age was 23.741 seconds and failed-command age averaged 136.671 seconds.
- PostgreSQL batch and commit averages were 101.779 ms and 2.650 ms.
- All queues drained, overlap was zero and rollback passed.

The three-writer, eight-generator activation stage is pending.
