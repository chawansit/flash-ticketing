# ADR 0066: Compact empty reservation-stream registry members

Date: 2026-09-28
Status: Accepted for implementation; production activation pending clean cloud validation

## Context

ADR 0065 filters empty streams from each bounded writer window. Its first clean Huawei stage improved durable persistence from 6,482 of 10,800 provisional holds to 10,492 of 10,623 provisional holds. All 10,623 commands left the Redis streams, but 131 reached PostgreSQL after their 120-second hold had expired. Average command age fell from 96.067 to 30.454 seconds. PostgreSQL commit time remained low at 2.761 ms and RDS observed at most five interesting waiters.

The stage still failed. The generator dropped 3,132 scheduled requests, 131 commands expired, and 482 seat-refresh rows were present at the fixed audit instant before later draining. Read and hold p95 remained within targets at 73.049 and 108.032 ms, and double-booking remained zero.

The advisory registry still contains thousands of historical empty stream names. ADR 0065 avoids reclaim/read work for them but repeats up to 5,000 pipelined `XLEN` probes per fair rotation. Filtering is therefore a useful recovery bound, not a stable steady-state lifecycle.

## Decision

After a bounded ADR 0065 length probe, remove members observed with `XLEN == 0` from `reservation-stream-registry` using one bounded, best-effort `SREM` command. Do not delete stream keys, consumer groups, command state or any reservation data.

Every accepted or replayed command continues to add its stream name to the advisory registry. Incremental keyspace `SCAN` remains the repair path when registry updates fail or race. Fair window rotation, the 5,000-candidate bound, the transaction-wide command batch bound and PostgreSQL authority remain unchanged.

Registry compaction is advisory. Failure to compact must not stop persistence of nonempty streams. This decision supersedes ADR 0065 only where ADR 0065 said registry members would not be deleted; its nonempty filtering and all other decisions remain active.

## Alternatives considered

- Delete empty Redis stream keys and consumer groups. This has stronger lifecycle and producer-race consequences and is not required to remove registry discovery cost.
- Add more writers or database connections. The clean evidence shows historical discovery work and command age are the immediate issue; scaling writers would multiply repeated probes.
- Keep the full registry and reduce the window. This smooths each probe but repeats the same historical work forever.
- Introduce a new authoritative active-stream queue. The existing registry is explicitly advisory and can be repaired by `SCAN`, so another index is unnecessary.
- Raise the hold TTL. This changes customer reservation behavior and hides persistence lag.

## Consequences

The first writer rotation after deployment may perform bounded cleanup of historical members. Subsequent rotations should be proportional to active or recently active streams plus the small bounded `SCAN` contribution. Redis memory used by old empty stream keys remains; this decision only compacts the advisory registry.

A race can remove a registry member after a producer appended and registered a new command. The command remains in its per-event stream. Incremental `SCAN` reintroduces the stream, and the global capacity preflight detects nonzero stream entries before a measured stage. This can delay processing but cannot acknowledge or delete a command.

## Failure and recovery behavior

- `SREM` failure increments the existing registry-error metric and persistence continues with the nonempty streams already selected.
- Writer crash before or after compaction leaves command entries untouched; consumer-group reclaim and fair discovery recover them.
- DCS failover clears local discovery state and rebuilds it from the remaining advisory registry plus `SCAN`.
- Rollback restores ADR 0065 behavior. Removed advisory members remain recoverable through `SCAN`; no data restoration is required.

## Validation evidence and activation gates

1. Unit tests must prove empty members are compacted, nonempty members remain, and compaction failure does not hide or block nonempty commands.
2. The hard batch, fair rotation, connection-reset and queue-preflight tests must pass.
3. Ruff and the full isolated Docker Compose suite must pass.
4. Before load, Huawei evidence must show reservation entries/pending at zero and a substantially smaller registry after bounded idle compaction.
5. A fresh 1,000 RPS, 6% write, batch-size-4 stage must pass request fidelity, zero expiry/durability mismatch, overlap, queue-drain and rollback gates.

The ADR 0065 cloud stage is diagnostic evidence and does not establish production capacity.

Implementation validation completed on 2026-09-28:

- Focused discovery, compaction-failure, hard-batch and queue-preflight tests passed: 7 tests.
- Ruff passed for the changed implementation and tests.
- The isolated Docker Compose suite passed: 235 tests, with 2 dependency deprecation warnings and no failures.
- Huawei registry shrink and the fresh 1,000 RPS cloud stage remain production-activation gates.
