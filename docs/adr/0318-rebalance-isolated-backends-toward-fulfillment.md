# ADR0318: Rebalance isolated backend slots toward fulfillment

Status: Rejected for adoption; implemented and tested; failed workload independently reconciled and normal runtime restored.

## Context

ADR0317's 16 foreground / 7 background / 1 diagnostic allocation reduced in-window tickets from 30,370 to 14,914 at the same 102 offered journeys/s. Background averaged 6.98 active backends and 9.71 waiting clients, while foreground averaged 9.28 idle backends. Customer ticket-wait p95 reached 14.58 seconds and the generator dropped 15,201 journeys. All actual payments reconciled; the run failed capacity and quality gates. The normal runtime is restored.

Foreground averaged 6.39 active backends in that slow run. API process CPU totaled about 1.97 of four allocated cores. The immediate measured constraint is the restrictive background allocation; more API pods would not address it. RDS read/WAL variation remains a possible additional cause.

## Decision

Test 8 foreground / 15 background / 1 diagnostic backends, maintaining the shared single-user ceiling of 24. This revises ADR0317's rejected allocation for a new experiment; it does not adopt its failed candidate. Keep the rejected 16/7/1 option identifiable for historical reproduction.

Keep the same image, four APIs with two general and two payment client connections each, four writers with batch size four, all other worker counts/resources/settings, admission limit 20, generator concurrency 500, gateway delays and recovery. Offer 102 journeys/s for 300 seconds; require at least 30,000 unique paid-issued tickets in that window. No load increase or infrastructure resize.

Use the existing exact-config signal/reload/rollback procedure and per-alias observation. Bind the selected allocation explicitly in the goal, runtime and observer. Authenticate the independently reconciled ADR0317 failure before scheduling this correction. Changing the alias ceiling must not change PostgreSQL database/user, authentication/TLS, payment commit guarantees, atomic holds, replay, idempotency, expiry or callback semantics.

## Alternatives

Return to one shared pool: preserves borrowing but recreates the original intermittent slot contention. Use 12/11/1: less background headroom than measured worker demand suggests. Increase physical connections or worker/API counts: changes another factor before testing the allocation correction. Introduce weighted borrowing: a more complex scheduling pattern without evidence that it is necessary.

## Consequences

Background receives more backend capacity for payment delivery and ticket issuance; customer traffic retains eight dedicated backends. Foreground's observed mean fits within eight, but peaks may queue. Faster issuance may reduce repeated status reads; this is a hypothesis, not a capacity claim. Neither group can borrow unused reserved slots. If the foreground reserve causes harmful waiting or latency, reject the candidate and restore the original runtime.

## Failure and recovery

Preserve current latency gates, the approved 0.01% final customer failure limit, zero generator drops, zero duplicate charge/double-booking/payment loss, full queue drain and exact restoration. Record initial errors, retries/recovered journeys, final failures and in-window tickets separately. Both generator shards must retain final evidence. Stop increasing load on any failure. Reconcile the exact actual paid cohort after expiry while retaining all failed expectations. Verify native client absence before pooler rollback and original ECS restart. Every experiment remains bounded to 3,600 seconds including normal cleanup; emergency restoration is completed even if that deadline expires.

## Validation evidence

Basis: [ADR0317 comparison](../capacity/cce/backend-partition-comparison-2026-10-14.json), original raw result bb26a572a41bd4dad7d2f119a80202e00eb419b447686e0ff4fd649b4d2e6174 and independently reconciled ledger bounded_cce_ticket_target__b7177356a0c3. Executed local exact PgBouncer1.24.1/TLS verification: 15 background backends and one diagnostic backend occupied; a sixteenth background transaction queued while all eight foreground transactions acquired. Actual diagnostics and pause/reload/resume methods passed; original bytes restored without restarting the local pooler. Owned containers/network removed. Executed 271 focused unit checks, including explicit allocation/contract and budget validation. Full current runner binding for 8/15/1 passed offline. [Local proof](../capacity/cce/backend-rebalance-local-proof-2026-10-14.json). The controlled cloud result failed, as detailed below. The prior hourly qualification remains specific to its original configuration.


## Executed failure and independent closure scope

Run adr0151-2afe2aa585ce / ledger bounded_cce_ticket_target__fb41a8fc8ec6 / raw SHA 0f54c4714069bb725172b73f06847316efcac875d7a5fe2c2b5177d5d1bd9749 failed. Scheduled 30,600, dispatched 25,423, confirmed 20,710, undispatched 5,177; 17,058 initial error journeys, 12,345 recovered, 39,950 retries and 4,713 final customer failures. In-window paid-issued tickets 22,384. Eight foreground backends averaged 7.79 active with 4.76 waiting clients; background averaged 9.50 active and 5.47 idle. Reject 8/15/1. No load increase follows this result.

The exact actual cohort is 25,064 recorded orders: 22,869 paid/issued and 2,195 expired unpaid. 359 dispatched failed holds created no recorded order. 2,159 paid orders have an issued ticket but the customer journey ended with an order-status503; financial reconciliation must not turn those customer failures into success. Extend ADR0313 only for this retained case, preserving customer/drop/error counts and requiring both safety payments, full payment/ticket relationships, queue drain, native absence and original pooler configuration/alias restoration.

Observation also reported a slot-capture window/receipt rejection and missing validated native API CPU summary. Retain those diagnostic gaps. They do not qualify the run; independent customer, issuance and pool measurements support rejection. No cause of an underlying RDS/storage stall is claimed from incomplete failure captures.

Independent ADR0313 audit executed successfully: all 22,869 actual payments had unique issued tickets; 2,195 unpaid orders expired, both safety payments passed, financial relationships were valid, queues drained and normal pooler/runtime restored. All 4,713 customer failures remain failures. Executed 275 focused unit checks and lint passed. [Full comparison](../capacity/cce/backend-rebalance-comparison-2026-10-14.json). The shared pool remains the preferred experimental baseline; neither static allocation is adopted.

Offline replay of the retained offered window identified Missing replica observation as the coverage rejection: 5 offered-window samples lacked complete API metrics. Observed process-start values stayed consistent; the missing samples remain missing and no validated API CPU/completeness claim is made.
