# ADR0337: Test payment delivery headroom with a fixed database budget

## Status

Accepted for local qualification and preparation of a configuration-only comparison. Cloud execution is not yet authorized. Supersedes ADR0263's sixteen simulator delivery slots only in an explicitly selected future experiment; existing defaults and historical profiles remain unchanged. ADR0336's authentication candidate is not adopted.

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
