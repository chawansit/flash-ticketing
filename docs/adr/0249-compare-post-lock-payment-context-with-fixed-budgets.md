# ADR0249: Compare post-lock payment context with fixed budgets

## Status
Accepted for implementation and local qualification. Immutable image receipt, fresh reservation and paid execution are pending. No old experiment is reopened.

## Context
ADR0248 reduced isolated payment context lookup work from three statements to one without changing order locking or payment semantics. Twenty idle read-only RDS pairs measured 4.1415 -> 1.3683 ms. Sixty-nine financial regression tests passed using the accepted explicit-BEGIN transaction method. This does not establish customer throughput improvement. The latest paid control failed and the previous pool-partition candidate remains blocked.

Incomplete slot diagnostics also caused the observer to discard otherwise valid CPU/pool metrics. The ring overflow cause remains unproven because invalid comments were not retained. Changing the gate to excuse missing evidence is not acceptable.

## Decision
Extend the existing registered CCE short comparison only for the post-lock payment context factor. Reuse the accepted immutable control image. Build a candidate from its exact 22-module source map and dependency inputs, replacing only initiate_payment. Preserve the accepted explicit-BEGIN transaction implementation, callback/hold/worker code and all flags. Validate installed/imported/bytecode sources, inherited configuration and registry digest before deployment.

Both arms use four 1-vCPU/1-GiB APIs, two general and two payment connections per API, PgBouncer 24 server connections, acquisition budget 20, unchanged 84 offered journeys/s for 300 seconds, payment settings and zero customer retries. Use fresh registered scopes and exact image/runner bindings. A failed control stops the candidate; no hourly progression occurs without all short gates passing. One hour remains separately bound and requires 300000 unique paid-and-issued tickets inside the hour.

Use the same observer correction in both arms: preserve parsed basic API metrics when the slot comment is invalid/incomplete, recording a bounded sanitized header and payload digest. Do not retain arbitrary payload fields. Keep strict slot validation and the final completeness gate unchanged: an incomplete ring still fails. Missing API metrics or unavailable slot data cannot be treated as success.

## Alternatives
Run the rejected transaction-startup image: not the accepted baseline. Combine observer SQL changes or pool rebalance with this candidate: obscures attribution. Suppress incomplete-slot failures: weakens verification. Build from the whole draft branch: includes unrelated/rejected defaults. Create a new runner: unnecessary; retain proven fixture, safety, financial audit, queue drain and restoration.

## Consequences
One customer-path factor changes; common diagnostics change equally in both arms. The isolated lookup gain may be small relative to commit/WAL stalls. Compare completed paid tickets, customer errors, latency, CPU and database waits; HTTP RPS alone is not capacity proof. Code/image identity and unchanged budgets are mandatory, not claims of performance benefit.

## Failure and recovery behavior
Stop on drift, failed preflight, failed control or quality/correctness/observation gates. Preserve original evidence and consumed scope. Complete ownership checks, post-TTL financial reconciliation, zero-double-booking, global queue drain, runtime restoration and credential cleanup even after failures. Leave the normal topology intact. Do not increase resources or relax gates to obtain a pass.

## Validation evidence
Pending: local malformed/incomplete snapshot retention tests, exact one-method source and image proof, same-pair control admission/replay tests, registered runner checks and the paid comparison. ADR0248 component evidence remains separate. No new cloud load or capacity improvement is claimed.
