# ADR0355: Qualify five-minute rates through200/s

## Status

Accepted for the user-preapproved300s progression through clean200/s after ADR0354 pair closed. Both160/s arms PASSED_RESTORED; candidate selected from observed51.21% lower worst-shard journey p95 and zero failures/drops. Preserve both consumed arms and their exact evidence.

## Context

The user wants rough five-minute capacity evidence before any hour-long workload. Corrected160/s control passed/restored. Candidate160/s completed:48000 scheduled/dispatched/fulfilled, zero drops/errors,47482 conservative tickets=158.273/s, journey p951696.18ms versus control3476.27ms. This is one matched pair, not maximum throughput proof. Existing orchestration pins160/s and supports historical180/s;200/s requires explicit source-bound workload integration, not an unchecked rate flag.

## Decision

After the160/s comparison closes, select a fully passed/restored immutable image from measured evidence, then qualify it at180/s and200/s in separate300s rounds. Each higher step requires a fully passed/restored immediately lower-rate result on the replacement RDS. These are workload qualifications, not new matched image comparisons; supersede ADR0347 same-rate control/candidate pairing only for this explicitly selected qualification mode. Keep historical comparison semantics intact. Never claim a candidate improvement from a pre-dispatch stop.

Bind exact lower-result report/ledger/hash, selected image/source/settings, rate/fixture/expected journey counts and all reviewed runner sources. Preserve the unchanged generator leaf algorithm,800 active journeys/four processes/500 HTTP connections/16 clients, fixed24 backends, application resources and recovery gates. Higher-rate workload must reach the same minimum issuance rule (expected journeys minus600) and zero drops; retain all existing failure, latency and financial correctness requirements. No hour-long workload or automatic resource resize.

## Alternatives

Jump straight to200/s: rejects stepwise limiting evidence. Run an image comparison at each step: unnecessary for qualification once an image is selected. Change resources while increasing rate: loses attribution. Loosen drops/latency/financial checks: rejected. Reuse or reset consumed arms: rejected.

## Persistence, locking, messaging, idempotency, TTL and scaling

Only offered workload and experiment evidence contract change. No application persistence, lock, message, idempotency, TTL, pool or resource change. Each attempt has fresh identity/journal boundary and one paid window; preserve every failed attempt. No main merge.

## Failure and recovery

Failed quality stops upward progression. Existing financial/post-TTL reconciliation, queue drain, generator idle and full restoration must complete. User preapproval permits necessary reviewed fixes and fresh attempts toward clean200/s, never resetting a consumed attempt or relaxing correctness. Revalidate changed preparation before cloud deployment. Unknown recovery ownership blocks further load until recovered.

## Validation evidence

Final focused147 regressions passed, plus two executed sealed-coordinator guard cases for180/s and200/s. Both retain the calibrated leaf and exact800-active/four-process/500-connection/16-client resources; wrong rate rejects before traffic. Ruff passed. Actual180/s plans/manifests/binding and real reservation in isolated copied bookkeeping passed against the genuine PASSED_RESTORED160/s candidate; no synthetic prerequisite admitted live work. Live bookkeeping remained unchanged.200/s live preparation/admission remains conditional on a real clean180/s result. [180/s readiness](../capacity/cce/rate-ladder-180-readiness-2026-10-19.json). No new-rate traffic had started at this readiness point.
