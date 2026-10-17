# ADR0337: Test payment delivery headroom with a fixed database budget

## Status

Rejected as a throughput improvement after one explicitly approved bounded cloud comparison. The32-slot configuration is not adopted; ADR0263's sixteen slots and historical/default profiles remain unchanged. ADR0336's authentication candidate is also not adopted. Local correctness checks remain valid but do not establish adequate cloud capacity.

## Context

The ADR0336 160/s experiment recorded mean due-to-claim delay of 4447.23 ms, callback HTTP delivery of 104.72 ms and acknowledgement of 10.29 ms. Sixteen delivery jobs were active. Pending payment attempts peaked at 638, compared with 32 paid-but-unissued orders. The same-budget ADR0335 reference also recorded a 4223.52 ms pickup delay with sixteen slots. These aggregate observations support testing simulator delivery headroom; they do not prove the only cloud bottleneck or that API/database capacity can reach 300/s.

The simulator represents an external gateway but its claim and acknowledgement bookkeeping uses the application's database. Its slot limit can constrain a benchmark independently of gateway network delay. HTTP delivery already occurs outside SQL transactions, with a bounded refill scheduler and token-fenced leases.

## Decision

Qualify 32 independent delivery slots locally with the existing simulator SQL pool limit of ten and the existing hard maximum of 32. Prepare a later comparison of sixteen versus 32 slots on the immutable ADR0324 application image, retaining the synchronous payment intake, all other worker counts/resources, gateway delay distribution, callback URL, 24 physical database backends, API 2+2 pools, admission twenty, recovery and customer polling settings. Do not use the unadopted ADR0336 API image.

Do not change the simulator default, reduce gateway delay, increase database connections or silently apply this setting to historical profiles. A cloud comparison must explicitly select and record the single configuration delta. Local qualification alone does not authorize deployment or a paid stage, and is not a cloud capacity result.

## Alternatives

Increase API replicas first: does not address observed delayed payment pickup. Reduce gateway latency: changes the workload and can manufacture an improvement. Increase the SQL pool: confounds the test and risks database contention. Build a new payment architecture immediately: premature while a configured delivery bound remains a plausible limiting factor. Retain sixteen slots: leaves the hypothesis untested.

## Consequences

More delivery slots can shorten payment pickup queues and reduce repeated status reads. It can also move pressure to callback API pools, PostgreSQL, CPU or Kafka. A five-minute success must measure unique paid-and-issued tickets inside the offered window, not merely final counts after draining. Advancing toward 300/s requires subsequent measurement and possibly separate scaling; 32 slots do not guarantee that target.

## Failure and recovery behavior

Preserve the fifteen-second lease, unique callback/payment identity, token-fenced acknowledgement and replay after lost committed responses. Delivery must not retain a SQL connection. Stop scheduling on shutdown and drain only bounded owned jobs. Do not acknowledge a failed delivery or perform an inline transport retry. Preserve seat expiry, authorization, atomic holds, payment/outbox atomicity and duplicate-safe issuance.

Stop escalation on generator drops, latency failure, final customer failures above 0.01%, financial violations or incomplete drain. Preserve the failed evidence and restore the exact original runtime. More callback work may worsen database contention; that is a failed candidate, not a reason to relax gates.

## Validation evidence

Pending local real-PostgreSQL tests of 32 parked deliveries with SQL pools of two and ten, lost responses, stale-token replay and duplicate-safe issuance. Existing refill shutdown/backoff and transport tests will also run. No cloud deployment, new paid allowance or capacity improvement is claimed.


Executed 44 tests inside the unchanged immutable ADR0324 image with isolated PostgreSQL17.6; zero skipped, 27.62seconds of test execution. All fifteen real-SQL scenario/slot combinations passed, including32 delivery jobs with SQL pools of two and ten, lost committed responses, stale-token fencing and duplicate-safe ticket issuance. Refill stop/drain now exercises both twelve and32 slots; bounded transport/backoff checks passed. Owned local containers/network were removed. The isolated layout emitted an unregistered integration-marker warning; it did not skip tests.

[Local qualification and measured payment queue evidence](../capacity/cce/payment-delivery-headroom-local-2026-10-17.json). No application source, default or cloud configuration changed; no new image is needed. Explicit bounded-runner configuration integration and a fresh cloud allowance remain prerequisites. This local result is not a cloud throughput measurement.


Implemented explicit ADR0337 selection in the existing bounded runner. Only the native simulator receives32 delivery slots; ECS bootstrap and historical defaults remain sixteen, other worker environments remain unchanged, and APIs/workers use the immutable ADR0324 image. Source-bound admission rejects undeclared budget changes, a conflicting ADR0336 API overlay, partial scope and unrelated runtime drift. The failed/reconciled ADR0335 result is labeled as a failed reference, never a passing control.

Executed155 focused runner regressions; Ruff F/I and535-file historical/current reproduction passed. Full generated four-API/fifteen-worker manifests, configuration binding and reservation admission passed using the actual existing publication receipt, without a publication mock. Approval/reservation accounting used isolated local copies only; live envelope/journal/state hashes were unchanged. Verified generated simulator settings32/10. Prepared an unauthorized source-bound goal for160/s300seconds,800 active journeys,500 HTTP connections, sixteen HTTP clients, one paid stage and3600seconds including mandatory safety/audits/drain/restoration. The prior allowance is consumed; a new bounded comparison needs approval.

[Runner readiness](../capacity/cce/payment-delivery-headroom-runner-2026-10-17.json) supersedes the local receipt's pending-integration scope. No cloud capacity improvement has been measured.


### Fresh single-run authorization

The user explicitly approved one bounded comparison after reviewing runner readiness. Registered one fresh allowance at journal index150:160 offered journeys/s for300seconds; only simulator delivery slots change16-to32, retaining the immutableADR0324 image, four APIs/fifteen workers,800 active journeys,500 HTTP connections, sixteen HTTP clients,24 physical DB backends and unchanged gateway delays. Safety, financial audit, full drain and exact restoration remain mandatory within3600seconds. The prior allowance is consumed; this approval permits no second paid stage or rate increase. Authorization is not a performance result.


### Executed cloud outcome: failed, original runtime restored

Ran160 offered journeys/s for300seconds on17October2026,10:33:51–10:38:51 Bangkok, with all APIs/workers on the immutableADR0324 image. All four API/fifteen worker receipts passed, including32 simulator delivery slots. The changed simulator environment was recorded under the source-bound reservation; all SQL, gateway-delay and resource budgets remained fixed.

48,000 journeys were scheduled;38,639 dispatched;38,191 customers confirmed their ticket. There were9361 generator drops,6505 initially failing journeys,6057 recovered journeys and448 final failures (1.15945% of dispatched, above0.01%). Final failures were69 ticket-status503,126 payment503,158 reservation-command503 and95 hold503. Total journey worst-shard p95 was9393.45ms. The database recorded37516 unique paid-issued tickets within the offered window and37254 conservatively:124.18/s. Both the same-image sixteen-slot failed reference127.15/s and best clean qualified138.49/s remain higher. No new capacity qualified.

Pickup mean improved4223.52to3311.82ms against the sixteen-slot same-image reference, while callback delivery worsened107.39to211.52ms. Successful API connection acquisition mean increased31.25to70.08ms; average commit remained2.39ms and query means approximately2–3ms. Full observer-lifetime counters recorded9621 admission rejections and two pool timeouts. These include safety/drain traffic and are not customer journey counts. Slot failure history remains incomplete; complete wait sampling and before/after query snapshots do not prove the exact causal holder. More callback concurrency worsened downstream admission pressure and is not an acceptable correction.

Independent read-only post-TTL audit verified38,544 actual orders:38,260 succeeded payments with38,260 unique issued tickets, plus284 expired unpaid orders. All seven relationships and both safety cohorts passed, with no duplicate booking, pending payment or lost succeeded payment.69 paid customers still failed retrieval within their journey; they remain final customer failures. Original48,000-completeness audit remains failed. Queues drained; original runtime stable/restored; namespace and helpers removed; generator idle; credentials cleared; no retired-secondary access. Only operational recovery cleared toFAILED_RESTORED; original result hash and paid-stage consumption preserved. Total experiment plus independent reconciliation1459.52seconds.

Do not increase callback slots or offered load from this result. Retain sixteen slots; next correction should address measured payment database acquisition/admission contention and competing status traffic. Additional compute or connection changes require a separate measured comparison, not an inferred benefit from this run. No further paid stage is authorized.

[Failed cloud result](../capacity/cce/payment-delivery-headroom-result-2026-10-17.json) and [independent reconciliation](../capacity/cce/payment-delivery-headroom-recovery-2026-10-17.json).
