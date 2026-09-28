# ADR 0065: Filter empty reservation streams during bounded discovery

Date: 2026-09-28
Status: Accepted for filtering; registry-retention detail superseded by ADR 0066 after failed cloud activation

## Context

ADR 0058 keeps one Redis Stream per event and an advisory global registry. ADR 0064 replaced a permanent first-5,000 cutoff with fair rotating windows. That fixed starvation and drained the previously unreachable 1,314 entries to zero, but it did not remove historical empty stream keys or registry members.

The first clean 1,000 RPS, 6% write, batch-size-4 stage after ADR 0064 passed all HTTP gates: 180,000 requests, zero drops, zero transport errors, zero first-attempt failures, read p95 24.541 ms and hold p95 45.450 ms. It failed durability. Two writers made 6,482 commands durable, marked 3,237 as `HOLD_EXPIRED`, and left 1,551 global stream entries after the 150-second post-load wait. Processed command age averaged 96.067 seconds and failed commands averaged 195.614 seconds. PostgreSQL batch time averaged 82.091 ms, commit time averaged 2.515 ms, and RDS observed at most five interesting waiters.

The registry contained more than 5,600 historical stream names before this stage. Every fair window therefore made writers issue group, reclaim and read operations against thousands of empty streams. Fairness prevents permanent exclusion, but empty-stream polling consumes enough writer time for valid 120-second holds to expire before persistence.

## Decision

Keep the per-event streams, advisory registry, fair rotating windows and hard transaction-wide command batch bound. After selecting each bounded discovery window, issue a non-transactional pipelined `XLEN` probe for at most 5,000 candidate streams and retain only streams whose length is greater than zero for reclaim/read work.

Advance the fair window cursor by the complete candidate-window size, including empty candidates. If a window contains no nonempty streams, mark that window complete immediately so the next writer iteration can inspect the next fair window without walking its members in small read batches. Do not delete stream keys, consumer groups or registry members in this decision.

A pending consumer-group message remains present in the stream until `XACK` and `XDEL`, so `XLEN > 0` includes both new and pending commands. Registry membership and bounded `SCAN` remain redundant discovery paths.

This decision extends ADR 0064. It does not change Redis-first acknowledgement semantics, PostgreSQL authority, hold TTL, at-least-once delivery, batch sizing or horizontal scaling decisions.

## Alternatives considered

- Delete empty streams and remove registry members after acknowledgement. This reduces historical state but introduces producer/cleanup races and cross-slot ownership questions that require a separate lifecycle decision.
- Increase writer replicas or batch size. This spends more database connections and does not remove the dominant empty-stream discovery work.
- Raise the 120-second hold TTL. This hides persistence lag from customers and changes the business reservation policy.
- Use a global active-stream queue. It adds another authoritative-looking index and requires atomicity or repair behavior across Redis Cluster slots.
- Flush DCS before each benchmark. This is unsafe for shared state and would hide the production degradation caused by historical streams.

## Consequences

Discovery adds up to 5,000 `XLEN` operations per refreshed window. They are sent in one bounded pipeline, reducing network round trips while keeping Redis server work explicitly capped. Writers avoid consumer-group and read operations for empty streams, so throughput should depend primarily on active event streams rather than all historical fixtures.

A command appended just after its stream was observed empty can wait until a later fair rotation. The producer adds the stream to the advisory registry on every accepted command, and bounded `SCAN` is the recovery path, so this is bounded delay rather than loss. Command-age metrics and lag admission remain required.

Historical empty keys still consume Redis memory. Lifecycle deletion remains future work and requires its own ADR.

## Failure and recovery behavior

- If the pipelined length probe fails, the writer iteration fails without acknowledging any command. The normal Redis reconnect path clears discovery state and retries from advisory registry plus `SCAN`.
- If a stream disappears between `XLEN` and group/read operations, Redis group creation recreates an empty stream; no command is acknowledged or lost.
- If a producer appends after an empty result, a later fair window discovers it. The global clean-start preflight prevents a capacity stage from beginning while any such entry remains.
- Writer crash and DCS failover retain the ADR 0064 reset and consumer-group reclaim behavior.
- Rollback restores the preceding source revision and PostgreSQL reservation mode; existing entries remain recoverable by the fair discovery implementation.

## Validation evidence and activation gates

Before another measured stage:

1. Unit tests must prove empty candidates are excluded, a later nonempty stream is reached after an empty full window, pending/new entries remain eligible, and connection reset clears discovery state.
2. Existing hard command batch-bound and fair-rotation tests must pass.
3. The full isolated Docker Compose suite and Ruff must pass.
4. Huawei bounded recovery must return the current global reservation backlog to zero.
5. A fresh 1,000 RPS, 6% write, batch-size-4 stage must start at zero entries/pending and pass request, durability, overlap, queue-drain and rollback gates before any longer promotion.

The failed clean stage is diagnostic evidence and does not establish production capacity.

Implementation validation completed on 2026-09-28:

- Focused discovery, hard-batch and queue-preflight tests passed: 6 tests.
- Ruff passed for the changed implementation and tests.
- The isolated Docker Compose suite passed: 234 tests, with 2 dependency deprecation warnings and no failures.
- Bounded Huawei recovery reduced the failed stage backlog from 1,551 entries to zero; pending ended at zero, batch size returned to 1, recovery writers stopped and all four API containers remained healthy.
- The fresh post-implementation 1,000 RPS capacity stage remains the production-activation gate.


Cloud activation evidence on 2026-09-28:

- The clean 1,000 RPS stage sent 176,868 of 180,000 scheduled requests and dropped 3,132; no response or transport error occurred.
- Read and hold p95 were 73.049 ms and 108.032 ms.
- Durable commands improved to 10,492 of 10,623 provisional acknowledgements; 131 commands failed with HOLD_EXPIRED.
- Reservation streams ended at zero entries and zero pending, double-booking remained zero, but 482 refresh rows remained at the fixed audit instant before later draining.
- Production activation failed. ADR 0066 supersedes the decision to retain every empty advisory registry member.
