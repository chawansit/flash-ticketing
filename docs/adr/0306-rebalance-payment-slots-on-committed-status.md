# ADR0306: Rebalance payment slots on the committed-status runtime

## Status

Accepted for a fresh controlled short comparison; not adopted as the baseline. This reopens the fixed-budget partition experiment from ADR0245 on the newer committed-status runtime, without replacing its historical result or accepted baseline.

## Context

ADR0304 candidate captures show payment-pool waits reaching seven per API while general-pool waits stayed at zero; each API had two general and two payment connections. Payment request/callback holders occupied commit and query phases. The API CPU mean was about 1.20 cores across four pods, so adding CPU alone is not the evidenced first correction. Global WAL/data-file waits are correlation, not a proven per-request cause.

ADR0285 payment write pipelining produced mixed occupancy/throughput results and remains disabled. The newer committed status publication avoids database reads which were present in the older partition experiment.

## Decision

Run fresh control with two general plus two payment connections per API, then candidate with one general plus three payment connections. Retain four API pods, 14 background pods, the exact ADR0297 shared image, committed-status publication enabled, the default ADR0283 observer query, 24 physical database connections and admission budget 20. Offer 84 journeys/s for 300 seconds with the same generator, gateway delay, retry policy and ADR0305 customer outcome target.

Only API payment partition changes between arms. No app image rebuild, schema/index/WAL change, additional compute, longer waits, or observer-query correction is bundled. Require a fresh restored passing control and exact same bindings before candidate. Investigate whether scarce local payment slots, despite spare general slots, explain recoverable rejections; do not claim this proves the underlying RDS stall cause.

## Alternatives

Increase connections or CPU: changes budgets and may move pressure into RDS. Increase checkout timeout: hides occupancy rather than reducing it. Reapply pipelining: previous evidence did not establish throughput gain. Move reads to replicas: consistency and topology changes outside this isolated experiment.

## Consequences

Payment checkout capacity increases without increasing total API or physical database connections. General reads may queue more; customer latency, drops, recovery and final errors must remain visible. A candidate with lower payment errors but poorer ticket throughput or total latency is not automatically adopted.

## Failure and recovery behavior

Preserve atomic holds, expiry, idempotent payment operation lookup, durable callbacks and ticket issuance. Failed control stops candidate. Restore the original two/two partition after either run; post-TTL audits, queues, original topology and owned-resource cleanup remain mandatory.

## Validation evidence

Evidence: [ADR0304 candidate](../capacity/cce/runtime-cohort-candidate-result-2026-10-13.json) and [failure analysis](../capacity/cce/runtime-cohort-candidate-failure-analysis-2026-10-13.json). Implemented the fixed-budget partition selector and exact control/candidate binding guards. The same 78 focused checks and legacy stage/lifecycle checks above passed; Ruff and source compilation passed. [Local validation](../capacity/cce/payment-slot-outcome-local-validation-2026-10-13.json). Fresh cloud comparison pending; no capacity improvement claimed.
