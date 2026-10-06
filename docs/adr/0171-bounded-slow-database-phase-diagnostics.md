# ADR0171: Bounded slow database phase diagnostics

- Status: Accepted; bounded cloud control and nonzero diagnostic collection qualified
- Date: 2026-10-06

## Context

ADR0170 control completed 18,000 paid-and-issued tickets at 60 journeys/s for 300 seconds, with zero customer errors, acquisition failures, drops or duplicate bookings. Financial, queue-drain and restoration gates passed. ADR0166 collection lifecycle was qualified, but the intermittent payment timeout was not reproduced. The frozen ADR0163 application already emits slow_db_phase for commit and pool_return durations at least 100 ms. Those records lack request IDs; the existing API observer records acquisition, commit and connection-hold durations plus native pool gauges and sampled database lock waiters.

## Decision

Extend the bounded read-only evidence path to retain existing slow phase records together with acquisition failures in one log scan per verified API. Reuse the ADR0166 identity verification, byte/line/time limits and failure sanitizer. Preserve its historical failure-only default. Bound each replica separately to 128 acquisition failures and 512 slow phase records; neither class can displace the other. Reject malformed relevant events, truncation, identity drift or incomplete failure counter coverage. Retain only aware timestamps, fixed phase/outcome labels and bounded durations for slow phases; no SQL, DSNs, request payloads, arbitrary messages or exception text.

Add bounded offline temporal context from the existing observer: at most 64 MiB, 2,000 rows, four exact API identities, monotonic timestamps and process/counter continuity. For each event retain at most five nearby samples within two seconds, fixed pool gauges, sampled lock waiters and interval means for acquisition, commit and connection hold. Label missing context explicitly. Failure correlation must have valid bracketing samples; slow records outside observer coverage remain explicitly uncorrelated. No invented request correlation and no causal assertion from timestamp proximity. Missing pool-return histograms are not interpreted as zero; existing slow return logs are the available evidence.

Use a fresh control-only profile under a separate ledger, one safety qualification followed only on success by one 60 journeys/s, 300 s control and its safety check. All application images, sources, machine sizes, placement, budgets, transaction behavior, polling, deadlines and retry policy stay unchanged. Existing consumed scopes stay closed. This decision permits implementation and local qualification; a fresh ledger must be established under current user authorization before cloud execution. No higher load or automatic replacement.

## Alternatives

Changing transaction logging in the application would require new images and alter the baseline. Keeping arbitrary Docker logs would expose data and lack bounds. Extending metrics without request evidence cannot explain a specific stall. Repeating the control without retaining its existing slow-phase logs would leave the same gap. None selected.

## Consequences

Diagnostics add work after the offered window, not to the reservation or payment path. Temporal evidence can distinguish commit tails from pool-return delays and show adjacent connection waits, but cannot identify a gateway request or establish RDS disk/WAL causality. API-only logs do not attribute background-worker stalls. A clean control qualifies collection, not intermittent root cause or greater capacity.

## Failure and recovery

Reserve fresh counters before dispatch. Stop failed qualification/control; retain available diagnostics and mark incomplete evidence as a failed gate. Continue mandatory financial/post-TTL audits, full queue drain, owned cleanup and exact restoration independently. Keep ownership lock if restoration is ambiguous. Never replay consumed scope or suppress failure with new customer retries.

## Validation evidence

Executed 253 local regression tests passed in 31.13 seconds, including malformed relevant records, independent class bounds, privacy filtering, real generated parser execution, replica/time/process/counter continuity, temporal bracketing, retained partial evidence, control-only identity and historical runners. A malformed non-object slow event initially escaped filtering; the path was corrected before the passing run. Changed-file Ruff and repository naming checks passed. Default preparation made zero cloud calls/customer dispatches. All 478 rows of the ADR0170 cloud observer trace were validated offline. [Local validation](../capacity/flash-sale-opening/slow-database-diagnostics-local-validation-2026-10-06.json). Cloud execution is pending; ADR0170 remains the latest measured control.

## Executed cloud checkpoint

Fresh safety qualification and one unchanged 60 journeys/s, 300 s control passed, with exact restoration and generator idle. All 18,000 dispatched journeys became customer-confirmed, paid-and-issued tickets; customer failures, drops and duplicate bookings were zero. Post-TTL durability and full queues passed. Complete evidence retained two payment-role admission rejections and 48 slow commits (maximum 407.95 ms), with bracketing temporal context for all records. No slow pool-return event at or above 100 ms was captured. Two callback HTTP 503s occurred; all callbacks and paid tickets ultimately completed. Native-pool timeout counters were zero.

The longest commits finished within about eight milliseconds across all four APIs on both hosts. That supports investigating a shared downstream stall, without identifying RDS WAL, replication, proxy or network as its cause. Adjacent proxy samples showed zero waiting clients; sparse samples do not exclude short waits. No application optimization or higher capacity was demonstrated. [Executed control](../capacity/flash-sale-opening/slow-database-diagnostic-control-2026-10-06.json).

## Configuration metadata discipline

After closing the measured scope, normalize the diagnostic plan to its exact control-only schema and validate every role setting against the frozen control. Inherited unused candidate and earlier observer-status metadata are not runtime instructions and must not appear in future diagnostic plans. Preserve the exact executed plan and binding in the owned evidence directory and Git revision; this metadata guard requires local validation and does not rewrite or replay the completed experiment.

The post-run metadata guard passed 256 local regression tests in 54.38 seconds and changed-file Ruff. The exact as-executed plan remains in each owned run directory; its file SHA-256 and canonical JSON binding digest are recorded separately in the cloud report. No cloud replay followed the metadata correction.
