# ADR0245: Rebalance API payment connections within a fixed budget

## Status
Accepted for a bounded matched experiment. Production defaults remain unchanged. The fresh control failed; candidate progression is blocked and the split change remains untested.

## Context
ADR0242 control issued 25,200 unique paid tickets at 84 journeys/s for 300 seconds with no customer errors. The redundant-BEGIN candidate issued 25,198 and failed two payment 503s; it is rejected for adoption. ADR0244 independently reconciled all successful payments and restored the failed scope. Both native timeouts captured payment-request and callback leases in commit. The nearest sample showed general 2 available, payment 2 occupied and five payment waiters. PgBouncer also had all 24 server connections active and 18 RDS WALWrite waiters. The sample identifies a constrained payment partition but does not establish the commit stall cause.

## Decision
Reuse the accepted immutable control image for both arms. Change only API_PAYMENT_POOL_MAX from 2 to 3 within DB_POOL_MAX 4, reducing general connections from 2 to 1. Keep four 1-vCPU/1-GiB API pods, pooler 24, shared acquisition 20, wait 500ms, worker images/counts, callback routing, simulator delay, synchronous confirmation, generator and strict customer/financial gates unchanged. Existing pool partition and shared admission code implement the change; do not add a new pool, borrowed checkout, retry or larger timeout.

Bind this configuration experiment explicitly as ADR0245 in the registered short CCE profile and its receipts. Use fresh control and candidate scopes with 84 offered journeys/s for 300 seconds each. A fresh fully passed and restored control from the same runner is required before the candidate. Five minutes matches the prior pair and covers the observed burst; mandatory TTL, relationship, durability, queue-drain and restoration checks remain unchanged. Existing paid-stage authorizations stay consumed; implementation alone does not reserve or start load.

This temporarily amends the equal general/payment partition for this comparison only. It does not supersede the accepted production isolation defaults or adopt ADR0239's transaction change. Hourly binding remains disabled until a short candidate passes all gates.

## Alternatives
Keep 2+2 and qualify an hour immediately: the older hourly payment503 remains unresolved. Add total connections: confounds the fixed budget and may increase WAL contention. Remove payment isolation: status reads can consume payment capacity. Split callback/request lanes or asynchronous confirmation: larger transaction/routing changes, defer until this smaller measured hypothesis fails. Extend timeouts or introduce retries: can hide the failure and alter customer latency.

## Consequences
Payment intake and callbacks gain one existing local slot while general order/hold reads lose one. PgBouncer saturation may simply move waiting downstream. There is no guaranteed capacity improvement; general latency/errors and shared-budget accounting must be compared, alongside paid-ticket throughput. Both arms use the same accepted image and source map, avoiding a second transaction factor.

## Failure and recovery behavior
Reject wrong image, factor, pool totals, resources, source/binding or stale control before dispatch. A failed control stops progression; a failed candidate is not adopted. Preserve original gates and evidence, independently audit successful payments if aggregate targets fail, drain all queues and restore captured normal runtime. Restore the2+2 partition; never reuse a consumed scope. No durability or double-booking guarantee changes.

## Validation evidence
Initial preparation checkpoint: no ADR0245 cloud load or performance improvement had been measured. ADR0242 measured comparison: [evidence](../capacity/cce/transaction-comparison-2026-10-09.json). ADR0244 recovery passed exact financial relationships, both safety payments, all queues and cleanup/runtime checks; affected recovery tests: 44 passed. Offline profile, budget and wrong-factor validation will be recorded after execution.

Executed local qualification: 241 affected CCE profile, native admission, entry, transition and exact recovery tests passed. Both arms generate the same accepted image and fixed pod resources; their decoded environment differs only in the declared payment split. Wrong totals, additional transaction factors, failed/stale controls and wrong partition bindings are rejected. Ruff, canonical names and reproduction checks passed: 535 historical inputs, six explicitly declared current overlays and 78 frozen generator files. No ADR0245 paid stage had started at that local checkpoint; the later failed control is recorded below.

After adding generated pod/environment assertions, the profile and existing pool-waiting/shared-budget suites passed 65 tests. These exercise guard rejection, timeout accounting and preserved bounded admission; no additional live traffic was involved.

Fresh control 4c6657f3e1d1 on revision 28cf3d8 failed: 25,200 scheduled, 23,390 dispatched, 23,295 customer-confirmed, 95 customer errors and 1,810 undispatched journeys. The independent terminal audit verified 23,377 paid/issued tickets, 13 expired unpaid orders, zero duplicates and complete relationships/drain/restoration under ADR0246. Retain the lower customer count and failed original gates. The payment3/general1 candidate was not started. Earlier general-pool headroom cannot be assumed for this round: retained failure frames predominantly show general/global acquisition rejections and order-status holders. Do not shrink general capacity against this failed control. [Actual failed-control evidence](../capacity/cce/payment-partition-control-2026-10-09.json).
