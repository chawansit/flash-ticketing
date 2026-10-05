# ADR0155: Bind inventory freshness to dispatch time

- Status: Accepted; fresh dry and separately approved matched paid comparison passed;further load is not authorized
- Date: 2026-10-05

## Context

The approved matched pair stopped after control. All18000 scheduled journeys were dispatched and customer-confirmed, with zero errors/drops,18000 payments/bookings/tickets, zero duplicates, post-TTL durability and complete queue drain. All31 other gates passed and original services were restored. The bounded_equal_cache_age gate revalidated pre-dispatch inventory using analysis time, after a300s load window, expiry audit and restoration. It rejected the otherwise valid1000ms inventory as stale. Validation at capture passes. A short dry cannot reproduce this timing defect.

## Decision

Keep the300s freshness rule for admission. Immediately after fresh live inventory validation, record a receipt binding its canonical digest, captured/validated timestamps, logical arm, immutable API image and complete source-manifest digest. Record observer-ready and actual launch-request timestamps. Verify the exact receipt and fresh inventory again before reserving/launching a paid stage. At final analysis, validate structural/cache/budget/source invariants using that recorded validation time, then require ordered capture/validation/readiness/launch/offered-window timestamps and launch/offered-start freshness within300s. Never manufacture a receipt from old evidence, renew timestamps, or change a recorded failure to a pass.

Legacy comparisons without the explicit status-refresh contract remain unchanged. Existing runtime/process/counter/observation, customer/latency, financial, post-TTL, zero-double-booking, full queue and restoration gates remain. Preserve the exact22 paid+10 additional gate obligations. This corrects which instant freshness describes; it does not extend the dispatch freshness window.

## Alternatives

Raise/remove the freshness limit: permits stale admission. Validate only at captured_at without a persisted receipt/dispatch chronology: invents qualification. Re-scrape restored services as the measured topology: observes the wrong deployment. Mark the old aggregate result passed or run candidate after control failure: hides the failed protocol or exceeds its stop rule.

## Consequences

Long completed stages can be evaluated without expiring valid admission evidence. Receipt omissions, mutations and delayed launches fail closed. The corrected adapter identity invalidates existing dry bindings and requires fresh qualification. The successful paid control remains useful executed evidence, but the original comparison remains failed and the optimization benefit remains unmeasured.

## Failure and recovery

Missing/malformed/tampered receipt, source/image/factor mismatch, future/inverted chronology or stale launch prevents paid dispatch/final gate success. Persist receipts before customer dispatch and preserve them with stage evidence. Continue mandatory audits/drain/restoration on every failure. Preserve consumed scopes and raw failed results. No replacement, candidate-only run, higher rate or infrastructure change is automatically authorized.

## Persistence, messaging, idempotency, TTL and scaling

Experiment qualification only; no production locking, transaction, messaging, idempotency, TTL, cache age, topology or connection-budget decision changes. Supersedes only ADR0151's analysis-time inventory freshness evaluation. No production ADR is superseded.

## Validation evidence

The [executed comparison report](../capacity/flash-sale-opening/status-refresh-paid-comparison-2026-10-05.json) records the original failed aggregate result and passing31 other gates. A local diagnostic reproduces `Observation is stale or future dated` at final evaluation and passes the recorded inventory at its capture time. Executed 303 focused harness tests passed across status-refresh comparison, two-host paid runner/observers, artifacts, parent retrieval, image staging and scaling preparation; this includes the earlier190-test subset. Ruff and diff checks passed. Regression cases cover delayed final analysis, missing/tampered receipts, stale scheduled/actual launch, malformed/inverted/future chronology and all32 gate obligations. The corrected validator rejects the retained old control record because it lacks the persisted receipt. Default corrected preparation executed with zero cloud calls/customer dispatches. No corrected cloud run is claimed.

The user separately approved one fresh exact-bound off/on safety qualification pair with at most two simulated isolated tickets and zero paid capacity stages. Prior failed scope is archived intact. This authorizes no automatic replacement or measured comparison. Live results will be recorded after execution.

Fresh dry8148deab5b1c executed on corrected runnerf24ad70 and passed both off/on arms.Each100-way cross-host race yielded1 hold/1 paid issued customer-confirmed ticket;hold/payment replay,other-actor authorization,duplicate callbacks,post-TTL durability,zero double-booking,full queue/Kafka drain,observer checks and exact restoration passed.Two safety protocols and zero capacity stages consumed.Stored qualification receipts match canonical inventory/image/source/arm proofs.See the [fresh qualification report](../capacity/flash-sale-opening/status-refresh-dispatch-qualification-2026-10-05.json).No paid-stage dispatch-admission chronology/full-length final gate or performance improvement was measured;those remain local regression coverage and future separately approved comparison.No previous failed result is rewritten.

Following the passing fresh dry,the user separately approved one replacement matched refresh-off/on performance comparison at60 buyer journeys/s300s per arm (18000 scheduled each),max2 measured stages and2 additional safety tickets.Identical2+2 placement,images/cache/machines/budgets;no retries,higher rate,automatic replacement,push or merge.The prior successful dry and failed measured scopes remain archived.Outcomes pending;approval itself is not performance evidence.

The separately approved corrected matched pair001831458f5a executed off/on60 buyer journeys/s300s each and passed all32 gates in both arms.Each18000/18000 paid issued/customer-confirmed,0 errors/drops/duplicates,post-TTL financial durability/full queue drain/exact restoration.Fresh persisted receipts passed before actual launch and at final full-length analysis.Two measured stages and two additional safety tickets consumed scope.See the [matched comparison report](../capacity/flash-sale-opening/status-refresh-paid-replacement-comparison-2026-10-05.json).Cache/API efficiency improved but consumer DB work rose;this validates the timing correction and fixed-load behavior,not increased maximum/hourly capacity.Prior failed control remains unchanged.No further load,push or merge authorized.
