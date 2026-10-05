# ADR0167: Bounded large projection contention measurement

- Status: Accepted for local diagnostic implementation; correction not selected
- Date: 2026-10-06

## Context

The engineering-tree 18,000-seat projection intermittently exceeded its existing 100 ms Redis socket timeout. A standalone rerun passed. This issue is excluded from the frozen admission comparison and remains unresolved. ADR0165 reclamation did not improve measured customer outcomes, so higher cloud load is stopped while failures are investigated.

## Decision

Measure the unchanged engineering projection in one owned, immutable-image local Redis container, limited to one CPU and 256 MiB, exposed on loopback only. Use fresh isolated event keys, 18,000 seats, fixed bounded full publications, zero versus four concurrent availability readers and one bounded patch writer. Keep the production adapter's 100 ms socket timeout and no publication retries. Separate Python argument/layout encoding time, payload bytes, client command elapsed time and Redis slow-log server execution time. Record failures and late acknowledged state independently.

Use diagnostic reads only after contenders stop to audit complete map visibility, source-version monotonicity, incarnation stability, stale-publication fencing and final freshness. A longer diagnostic-read timeout does not alter the measured adapter or count a failed publication as successful. Clean only the exact container and owned volume after verified labels/image identity. Keep all timing and failure evidence. Select at most one isolated correction after measurements; write a new decision before any pattern change. This experiment is local diagnostics, not production capacity or a resolution based on one standalone pass.

## Alternatives

Changing deadlines first could mask the cause. Measuring only total API time cannot separate client encoding, transport and server cost. Using shared cloud Redis could perturb live state and would require a separate exact cloud profile. None selected.

## Consequences

Provides attribution under bounded local contention. Docker and Python scheduling can affect the result, so it does not prove Huawei performance. No financial transaction, seat hold authority, freshness policy or publication atomicity changes.

## Failure and recovery

Preserve timeouts and errors without retries. Stop contenders on the bounded deadline, join before audits, and destroy only the verified owned container and volume. Keep explicit cleanup/recovery evidence. Do not discard partial data, extend measured deadlines or begin cloud load.

## Validation evidence

Executed bounded local runs with Redis6.4.0 retries verified0. Original timeout reproduced; all completed-phase freshness/atomic visibility audits passed and owned resources were removed. See [measurement comparison](../capacity/flash-sale-opening/large-projection-contention-comparison-2026-10-06.json). ADR0168 candidate improves Lua cost but was not adopted after client/read regression. The original issue remains unresolved; no cloud capacity stage is claimed.
