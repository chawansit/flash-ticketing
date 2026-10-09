# ADR0249: Compare post-lock payment context with fixed budgets

## Status
Implemented and locally qualified. Fresh cloud control failed and was independently restored under ADR0250. Candidate execution and performance benefit remain unmeasured. No old experiment is reopened.

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

Executed local qualification: 330 affected runner, admission, lifecycle, observer and work-envelope tests passed; one unrelated environment-dependent test was skipped. The added tests exercise ADR0249's exact pair, unchanged pools/resources/workload, fresh control admission, failed/stale control rejection and registry/runner drift. Reproduction checks verified 535 historical files, six declared current overlays and 78 frozen generator inputs. The API source builder rejects an unreviewed payment method.

Published the candidate to SWR by immutable schema2 digest and reused the accepted control. Registry configuration, inherited runtime fields, copied and installed sources, imported bytecode and fixed runtime flags were verified for both images; registry credentials were removed. Source maps contain 22 modules and differ only in the payment method. Basic observer metrics survive invalid/incomplete slot comments, while strict completeness still fails; customer/private fields are never retained by the bounded error marker. [Immutable image receipt](../capacity/cce/payment-context-images-2026-10-09.json). No customer dispatch or paid capacity measurement occurred at this checkpoint.

Fresh control adr0151-135b02478459 failed: 25200 scheduled, 20911 dispatched, 20817 customer-confirmed, 94 customer errors and 4289 capacity drops. Slot completeness failed on bounded ring overwrites; retained basic metrics allowed offline diagnosis without repairing the gate. API offered-window CPU was 2.0502 cores, primary background-host CPU 84.1288 percent; 212730 status reads and callback due-to-claim mean 4996.63 ms show backlog and read amplification. Independent ADR0250 recovery verified 20901 unique paid/issued tickets, ten expired unpaid orders, both safety payments, zero duplicates, all queues zero and exact restoration. No candidate or hourly progression. [Executed control evidence](../capacity/cce/payment-context-control-2026-10-09.json).
