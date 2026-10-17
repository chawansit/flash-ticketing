# ADR0339: Test 180 journeys/s with qualified API pools

## Status

Accepted and explicitly authorized: the user replied Approved to one180/s300s cloud stage including mandatory financial audit, drain and restoration within60minutes. Earlier automatic-review rejection is retained in validation history; no stage had started or been reserved at approval. No second stage, hourly test, machine resize or physical database budget increase is authorized.

## Context

ADR0338 passed 160 offered journeys/s: 48,000 journeys completed without generator drops or customer errors, 47,419 conservative in-window tickets (158.0633/s), journey p95 3.71s, all payment-integrity and restoration gates passed. Four API pods used 3 general and 3 payment clients each while PgBouncer remained capped at 24 physical backends. API CPU averaged 2.40 cores out of four; pool acquisition mean was 11ms. Reservation durability p95 was 3.04s and writer command age mean 1.56s. These measurements justify a modest workload step, not unlimited scaling or another code change.

## Decision

Offer exactly 180 journeys/s for 300 seconds (54,000 journeys) over 180 distributed fixture shows. Require at least 53,400 conservative in-window unique paid-and-issued tickets (178/s), all unchanged quality gates, zero generator drops, and the approved final customer-error ceiling of 0.01%. Keep four generator processes, 800 aggregate active journeys, 500 HTTP connections, sixteen clients, four 1-vCPU/2Gi API pods and fifteen unchanged background pods. Keep API pools 3+3, shared admission 20, acquisition deadline 500ms, simulator 16 delivery jobs/10 clients, physical backends 24 with no reserve. Pin the same ADR0324 application image and all existing financial, polling, recovery, expiry and messaging behavior. Only offered rate and proportional fixture/expected counts change from ADR0338.

## Alternatives

Repeat 160/s first for reproducibility: useful for qualification, but this request prioritizes improving five-minute throughput. Jump directly to 300/s: unsupported. Add compute or database connections immediately: changes multiple factors without proving the next bottleneck. Increase retries, relax latency, or hide undispatched journeys: rejected.

## Consequences

Higher load may saturate reservation writers, API payment pools, PgBouncer or RDS and increase active journey occupancy. Report all scheduled/dispatched counts, first errors, recovery, final failures, latency, conservative issuance and CPU/database waits. A passing sample demonstrates only this short workload; it does not establish hourly, maximum or production capacity.

## Failure and recovery behavior

Preserve atomic seat holds, customer authorization, TTL, payment idempotency, payment/outbox atomic commit and duplicate-safe issuance. Stop escalation on any failed gate. Always reconcile paid tickets, drain queues and restore the exact original runtime within the 60-minute experiment boundary; cleanup may continue if needed for safety. Preserve original failed evidence. No automatic retry stage.

## Validation evidence

Pending focused workload/binding checks, historical profile regression, generated manifest/admission verification and the actual cloud stage. The completed ADR0338 report remains the immutable passing baseline. No higher capacity has yet been measured.

Local validation executed: 153 focused workload/profile tests and 228 isolated historical resource/lifecycle checks passed. Ruff F/I passed. Historical/current reproduction verified 535 files. Full generated four-API/fifteen-worker manifests, exact source/image binding and reservation passed in isolated files using actual existing publication, with no publication mock or live accounting changes. Original broader regression failures came from archived receipts inheriting a consumed live goal; isolated test context fixed collection without relaxing runtime guards. The proposed goal is PREPARED_NOT_AUTHORIZED. Automatic approval review rejected the combined push/launch because the user had not explicitly approved the exact180/s paid stage. No launch, reservation or paid load occurred; the provisional envelope authorization was withdrawn and the prepared goal retained. Explicit180/s approval is required. Cloud load and higher capacity remain pending.

Fresh authorization: the user replied Approved to the exact180/s300s cloud test request. Registered one paid-stage allowance at ledger index152, preserving all quality, financial and restoration gates. Higher capacity remains unmeasured until the stage completes.

### Pre-dispatch correction

Run `adr0151-b8a36980ef01` stopped before paid dispatch: the fixture-identity validator retained a fixed show-count allowlist without 180. The financial-audit validator also lacked 180 and would have blocked the next step. This is a missing workload integration, not an application capacity result. The run restored all services with integrity and cleanup verified; zero paid stages started. Extend only these explicit allowlists, preserve all identity/ownership/freshness/financial checks, and test complete local 180-show fixture preparation with simulated transports before retrying the unused paid allowance. Retain the failed report and bind fresh source hashes; no load gate is weakened.

Correction validation executed: 34 fixture/180-workload checks passed, including complete PaidStage.prepare with 54,000 tokens and simulated remote transfers, retained identity, compiled financial audit and duplicate-ID rejection. Revalidated the actual failed180-show fixture with its creator time and pinned legacy producer. Full generated admission/reservation and535-file reproduction passed again. The failed run used zero paid stages and restored all resources; fresh source contracts retain the same single unused180/s approval. See qualified-pools-180-predispatch-abort-2026-10-17.json.
