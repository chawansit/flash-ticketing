# ADR 0067: Throttle empty reservation discovery and repair scans

Date: 2026-09-28
Status: Accepted; cloud activation improved request performance but failed exact durability

## Context

ADR 0066 successfully compacted the advisory registry to zero historical members before load. Its activation stage drained all queues and made 10,180 of 10,182 provisional commands durable, with only two HOLD_EXPIRED outcomes and zero double-booking. Average command age fell to 16.691 seconds.

The request workload still failed: only 169,809 of 180,000 requests were sent, generator drops reached 10,191, read p95 rose to 121.681 ms and hold p95 rose to 175.596 ms. No HTTP or transport errors occurred.

Code inspection found that the discovery refresh interval applies only while the writer has a nonempty local stream list. With an empty registry and no reservation traffic, each writer iteration repeats bounded SCAN, pipelined XLEN and best-effort SREM immediately. Two writers therefore create continuous Redis repair traffic throughout deploy, fixture preparation and seat-map warmup, exactly when the reservation queue is empty. Incremental SCAN is a repair path, but it currently runs at the fast registry refresh cadence.

## Decision

Apply the existing stream refresh interval whether the current local stream list is empty or nonempty. Keep advisory registry refresh at its one-second default so a newly accepted command is discovered promptly.

Run incremental keyspace SCAN on a separate 60-second default repair interval. The first writer refresh and every connection reset may scan immediately; later refreshes use the registry fast path and only perform bounded SCAN when the repair interval expires. Preserve the four-step scan bound, ADR 0064 fair rotation, ADR 0065 XLEN filtering and ADR 0066 best-effort registry compaction.

This decision changes discovery repair cadence only. It does not change acknowledgement, TTL, PostgreSQL authority, command batch size, writer count or Redis Cluster slot layout.

## Alternatives considered

- Disable SCAN completely. Registry updates are advisory and can fail, so a repair path remains necessary.
- Keep SCAN every second but reduce COUNT. This still creates continuous work while idle and increases repair-cycle duration.
- Increase the registry refresh interval to 60 seconds. That delays ordinary accepted commands even when their registry update succeeded.
- Add more writers. It would multiply the idle repair traffic and database connection demand.
- Use the DCS read-only endpoint for seat-map reads. Replica lag can violate current availability semantics and is unrelated to idle writer repair.

## Consequences

Idle writers perform a small registry lookup at most once per stream refresh interval and a bounded keyspace repair scan at most once per repair interval. Normal accepted commands remain discoverable through the registry within about one second. A failed registry update can wait up to the repair interval plus a bounded scan cycle before discovery.

The repair interval becomes part of worst-case recovery time and must remain below operational lag alarms. Preflight still scans global stream entries directly and blocks a measured stage if an undiscovered command remains.

## Failure and recovery behavior

- Connection reset clears both refresh clocks, allowing immediate registry and SCAN recovery after DCS failover.
- Registry failure leaves commands in their streams; the next scheduled bounded SCAN repairs discovery.
- Writer crash loses only local cursors and clocks. Consumer-group reclaim behavior is unchanged.
- Rollback restores ADR 0066 behavior; no data migration is required.

## Validation evidence and activation gates

1. Unit tests must prove empty discovery is throttled, registry refresh remains prompt after the interval, SCAN waits for its independent interval, and reset permits immediate repair.
2. Existing fair rotation, nonempty filtering, compaction failure and hard batch-bound tests must pass.
3. Ruff and the full isolated Docker Compose suite must pass.
4. A fresh 1,000 RPS, 6% write, batch-size-4 stage must pass request fidelity, durability, zero overlap, queue-drain and rollback gates.

The ADR 0066 activation stage is diagnostic evidence and does not establish production capacity.

Implementation validation completed on 2026-09-28:

- Focused discovery tests passed: 8 tests.
- Ruff passed for the changed implementation and tests.
- The first full isolated Docker Compose run had one Redis socket-timeout failure in
  the immediate-reclaim integration test; that test passed when rerun in isolation.
- A second full isolated Docker Compose run passed: 236 tests, with 2 dependency
  deprecation warnings and no failures.
- The fresh Huawei stage at revision a258443 sent 179,327 of 180,000 requests and
  dropped 673 at the generator. It had no transport errors or admission rejections;
  read and hold p95 were 33.852 ms and 60.337 ms.
- The writers made 10,640 of 10,755 provisional commands durable. The remaining 115
  completed as HOLD_EXPIRED, so exact durability failed. All queues drained, overlap
  was zero and rollback passed.
- Compared with the ADR 0066 activation stage, generator drops fell from 10,191 to
  673 and both request latency percentiles improved substantially. This validates the
  discovery-throttle behavior but does not establish 1,000 RPS production capacity.
- ADR 0068 defines the next bounded writer-scaling experiment for the remaining
  persistence-throughput limit.
