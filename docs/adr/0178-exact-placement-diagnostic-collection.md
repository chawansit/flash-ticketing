# ADR0178: Exact placement diagnostic collection

## Status
Accepted and locally qualified under the standing work envelope; fresh cloud comparison pending.

## Context
ADR0177 qualification 6b3bf436e1c6 passed its 2+2 arm but rejected 1+3 during final log collection. Both post-TTL financial audits and exact restoration passed; no measured paid stage ran. The shared slow-phase collector and its admission-program builder still assumed two APIs per host. Earlier placement tests mocked collection and did not exercise the allocation restriction.

## Decision
Derive exact per-host API counts from the already source-qualified ADR0174 or ADR0177 placement marker, requiring the declared control 2+2 or candidate 1+3 allocation. Collect all four immutable API identities at their actual hosts. Preserve the default two-per-host contract for other profiles. Permit a three-replica generated log program only with an explicit exact count; reject unknown counts, duplicates, missing identities, wrong host allocation and malformed placement policy before collecting logs.

Per-replica bytes, records, stream deadlines, source/image/start identity, temporal coverage and metric-counter checks remain unchanged. Four replicas retain the same aggregate collection budget. No application, connection-budget, load, financial, latency or recovery gate changes. This corrects the fixed-2+2 assumption for placement profiles; it does not supersede ADR0171 evidence requirements, ADR0174 topology or ADR0177 diagnostic scope.

## Alternatives
- Drop final diagnostics from dry qualification: rejected; would hide integration defects.
- Collect only two secondary APIs: rejected; omits a measured replica.
- Allow arbitrary host counts globally: rejected; could silently change profile coverage.

## Consequences
The declared 1+3 profile can collect complete diagnostics. The correction establishes harness coverage only, not increased backend capacity. Fresh code bindings and fresh scopes are required; the failed scope cannot be replayed or converted to a pass.

## Failure and recovery behavior
Stop before further load while the envelope is recovery-required. Verify both recorded safety/post-TTL results and live original runtime, idle generator and full queues through read-only recovery. Preserve the failed report, consumed counters and original terminal status when recording verified restoration. Unknown ownership or failed restoration blocks all new experiments. Then locally qualify the real collection path and use a fresh standing-envelope reservation.

## Validation evidence
Executed 32 new placement regressions and resolved all four recorded real cloud inventories without cloud calls. The combined runner suite passed 227 tests and exposed one historical cleanup-hook assertion loaded before its correction; that failed evidence is retained. After correction, the complete collector subset passed 52 tests, including cleanup ordering. Ruff, repository naming and diff checks passed. Read-only live recovery verified original runtime, idle generator and full queue drain in 17.156 seconds. See [the sanitized validation report](../capacity/flash-sale-opening/exact-placement-diagnostic-validation-2026-10-06.json). No capacity improvement is claimed.
