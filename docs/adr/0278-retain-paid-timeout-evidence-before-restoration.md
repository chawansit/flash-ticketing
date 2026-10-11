# ADR0278 Retain paid timeout evidence before restoration

## Status

Accepted for implementation and one bounded five-minute 84 offered journeys/s baseline following the user request to continue the stated plan. Implemented and locally validated. With fresh SWR authentication, one cloud baseline completed on 11 October 2026 and failed quality/diagnostic gates. Independent terminal audit passed; runtime restored. No capacity improvement or hourly qualification is claimed. No capacity improvement is claimed.

## Context

ADR0277 independently verified 22,101 unique paid tickets inside 300 seconds, but the sharded coordinator timed out before writing customer.json. The exception also skipped ordinary CPU/database observer collection. Unknown customer errors and generator drops prevent quality qualification. Historical sealed workloads must remain reproducible.

## Decision

Create an explicit opt-in sealed workload overlay derived from the existing three-attempt recovery bundle. Each child atomically checkpoints non-secret accounting every ten seconds, including at the end of scheduling and after completion. On the coordinator's existing timeout, stop and reap only owned children, retain available final reports or checkpoints, and always produce a failed combined summary. Missing or partial customer counts and latencies remain unknown; observed checkpoint counts are labeled lower bounds. Neither a checkpoint nor a nonzero child exit may pass qualification. Preserve scheduled arrivals, rate, concurrency, gateway delay, retries and the completion deadline.

Keep the historical recovery bundle, coordinator and manifests unchanged. Bind the new overlay digest in the native runner and require its explicit ADR0278 selector before transferring it. Reject unexpected selectors and mismatched source hashes.

If paid dispatch fails, collect available CPU/database/Kafka diagnostics independently before resource restoration. Stop already-started jobs first in this failure path to avoid waiting a full observation window after an early error. Preserve incomplete coverage and collection errors; a successful diagnostic fetch does not turn a failed run into a pass. Mandatory financial audits, queue drain and restoration remain independent.

Run one fresh 84/s, 300-second distributed baseline with two shards and 500 active journeys, four 1-vCPU/2-GiB API pods, fourteen 250m/512-MiB workers, unchanged application digest, 24 physical database connections and all existing quality gates. Collect a read-only count of unique paid-and-issued tickets inside the same 300-second offered window, independently of customer confirmation and eventual drain totals. This diagnostic does not replace customer quality gates. This is a baseline with corrected evidence retention, not a matched application-performance improvement. Do not increase load if the baseline fails. Hourly qualification follows only a passing short test and an appropriate fresh scope.

## Alternatives

Extending deadlines hides delayed journeys and changes the workload. Inferring customer dispatch or errors from database orders is invalid. Rewriting the orchestration platform adds unrelated work. Mutating historical sealed inputs destroys comparability. Silently treating missing shard results as zeros hides unknown outcomes.

## Consequences

Checkpoint writes add small bounded generator-side overhead; record that difference. Partial counters cannot certify full customer outcomes, and post-timeout financial audit remains necessary. No application image rebuild or database schema change is needed. This does not fix an unconfirmed application bottleneck.

## Failure and recovery

Timeout and malformed/missing reports are failed evidence. Never replay the paid stage automatically or reset consumed ledger entries. Do not expose raw worker logs, manifests or credentials. Retain the original failure, attempt diagnostic collection and execute owned cleanup plus payment/booking/ticket reconciliation. Unknown restoration blocks another paid stage.

## Supersession

Extends ADR0254 and ADR0277 evidence retention for a newly selected workload only. Historical results, financial semantics, expiry, atomicity, recovery policy, image and resource contracts remain unchanged.

## Validation evidence

Executed 70 focused tests and 186 wider runner/recovery tests successfully. The focused tests exercised real hung child processes with final, partial, missing and malformed reports, unknown counters, overlay drift, failure diagnostics and mandatory cleanup. Ruff passed for changed Python sources. Offline reproduction verified 535 historical sources and 25 explicit overlays without cloud calls. Local tests establish reporting behavior; the executed cloud result below failed qualification.

The initial cloud preflight reached both ECS hosts, verified the original primary runtime, idle generator, normal queue state and empty secondary runtime, then failed SWR authentication. No fresh ledger or paid-stage allowance was consumed. See [preflight evidence](../capacity/cce/timeout-retention-preflight-2026-10-11.json). The prior ADR0277 capacity result is unchanged.

The executed baseline exposed a reporting distinction: a child normally exits1 when quality gates fail, even when its final measurement is complete. Preserve final metrics for that case; measurement completeness and quality acceptance are separate. Timeout, missing/partial reports and abnormal exits still cannot certify complete metrics. This clarification changes future reporting only and does not rewrite the retained cloud result or waive any gate.

Cloud result: [fixed-budget baseline](../capacity/cce/timeout-baseline-result-2026-10-11.json), [independent terminal audit](../capacity/cce/timeout-baseline-terminal-recovery-2026-10-11.json). 20,744 customer-confirmed tickets, 4,456 undispatched journeys, 556 initially errored journeys recovered, zero final failures; 20,279 unique paid-and-issued tickets inside 300 seconds. Maximum shard journey p95 was 24.159 seconds. APIs used 1.886 of four cores; the Python database bridge used 0.924 of one core; physical database slots peaked at 24 active with 12 clients waiting. Partial slot snapshots covered 594 of 625 failures; the complete-history gate remains failed. The original failed report is preserved. The reporting clarification passed 71 focused tests including a real normal-exit1 child; no cloud replay was performed. Next performance work should target the measured database forwarding path while retaining the physical connection budget.
