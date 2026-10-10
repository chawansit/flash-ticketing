# ADR0277: Five-minute 50,000-ticket capacity test

Status: Accepted for one bounded experiment; capacity unmeasured.

## Context
The user requests 50,000 issued tickets in five minutes. That requires 166.67 unique paid-and-issued tickets/s. The historical 84/s profile offers only 25,200 journeys; the last placement run issued 20,696 inside its five-minute window and had an omitted recovery discriminator. Those results remain failed and their allowance remains consumed.

## Decision
Register a separate `cce_ticket_target` profile: 168 offered journeys/s, 300 seconds, 50,400 distinct one-seat journeys, 168 isolated shows of 300 seats, two generator shards and 1,000 total active journeys. The two shards each offer 84/s. Reuse the sealed three-attempt recovery workload, deriving only its explicit rate ceiling and coordinator hash for this profile. Preserve historical bytes and receipts. Validate the registered rate, concurrency, recovery and source identities before dispatch.

Keep four 1-vCPU/2-GiB API pods, fourteen 250m/512-MiB background pods, the existing application digest, database connection budgets, Redis hold expiry, gateway delays and callback behavior. No application or persistence architecture change. One paid stage is authorized by the user's direct request; reserve a fresh ledger without resetting earlier consumption. The existing CCE spending/scaling permission applies; no new ECS/RDS/DCS resources. Bound the whole experiment to 60 minutes with mandatory cleanup permitted beyond it.

## Alternatives
A historical 84/s run cannot offer enough tickets. Unbounded generator concurrency hides admission constraints. Adding pods and changing database budgets simultaneously would obscure this first measurement. A new runner duplicates the proven safety and restoration lifecycle.

## Consequences
This is a higher-load capacity test, not a matched performance improvement claim. The generator may still drop arrivals and the application may reject or complete journeys late. Report scheduled, dispatched, first-attempt errors, recovery, final customer failures, latency and late issuance separately. The unchanged customer gates remain in force; additionally require at least 50,000 distinct paid-and-issued tickets in the actual 300-second window. Measure a conservative inner window too to expose clock uncertainty.

## Failure and recovery
Stop further load after this one stage. Preserve failed evidence, exact fixture ownership and source identities. Check all financial relations, zero double-booking, payment durability, post-TTL state and queue drain, independently of the throughput result. Restore the captured ECS runtime, delete owned CCE pods/helpers and remove temporary credentials. Failed throughput is not evidence of payment loss. Unknown integrity or restoration blocks further load.

## Validation evidence
Executed: 142 targeted profile, recovery, transaction and envelope tests passed. Offline reproducibility verified 535 archived inputs and 25 current orchestration overlays. Cloud experiment pending; no capacity or production qualification claimed.


Executed cloud result: `adr0151-ee0b72fef73d`, one consumed paid stage. Offered window 23:05:38–23:10:38 Bangkok on 2026-10-10. Database audit counted 22,101 unique paid tickets inside 300 seconds (73.67/s), 21,900 in the conservative inner window, and 31,666 after drain. The target failed. There were 31,903 orders, including 237 unpaid expirations and no pending orders. The sharded coordinator timed out waiting for its children and did not persist the customer summary; dispatched journeys, generator drops, customer errors and customer latency are unknown. Do not infer them from database counts.

Independent read-only terminal audit passed: all paid ticket/payment/inventory relationships valid, no duplicate booking, no payment loss, both safety tickets durable, all queues empty, 170 owned shows retired, namespace/helpers/private inputs removed, generator idle and original runtime restored. The original failed report remains unchanged. The ledger was closed as `FAILED_RESTORED`; this is not capacity qualification. The audit reuses the established financial checks with only an explicit 168-show expectation bound; its exact program hash is retained in the recovery evidence.

Results: [test summary](../capacity/cce/fifty-thousand-ticket-result-2026-10-10.json), [independent audit](../capacity/cce/fifty-thousand-terminal-recovery-2026-10-10.json). Corrected historical unit-test assumptions: explicitly select the historical authorization workload, isolate the mocked lifecycle from the current live goal and assert the existing worker diagnostic response limit. No application image or cloud budgets were changed after dispatch. Generator terminal-report retention must be corrected before using another run for customer-quality qualification. No further load was started.

Final local validation: 395 tests passed in the combined 396-test suite; the remaining historical authorization fixture inherited the new 168/s rate. It was made explicit and all 19 tests in its targeted suite passed on rerun. Ruff and the 535-file historical reproducibility check passed. These local checks do not change the failed cloud result.
