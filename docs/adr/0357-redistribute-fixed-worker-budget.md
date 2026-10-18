# ADR0357: Redistribute the fixed worker budget

## Status

Accepted for preparation and user-preapproved qualification through clean200/s. Extends ADR0355; supersedes ADR0315/0345 uniform per-worker quota and four-writer/six-consumer placement only for the explicitly selected ADR0357 mode. No node, VM, database, backend or aggregate resource resize. Historical contracts/results remain unchanged.

## Context

The exact combined image passed160/s with zero drops/errors and journey p951696.18ms. First180/s run dispatched/fulfilled53998 of54000, dropped2, recovered15 initial failures, p955696.82ms, conservative53181=177.27/s. It failed quality and independently reconciled/restored under ADR0356. Writer PostgreSQL batches averaged76.77ms and reservation command age2413ms in observer-lifetime summaries; these are clues rather than isolated causal proof. Offered-window API CPU was2.88 of4 cores with balanced traffic. Simulator CPU approached0.235 of0.25 core, while maintenance/reconciler were about0.031/0.044. Existing write pipelining is already enabled; ADR0320 outer-transaction pipelining was locally rejected.

## Decision

Keep the exact combined application image, four APIs,15 worker pods, total worker requests/limits3750m CPU and7680Mi memory,24 physical backends/no reserve and shared admission20. Reassign one consumer to a fifth reservation writer: five consumers, five writers, and one each publisher, projection consumer, maintenance, reconciler and simulator. Keep250m CPU per writer/consumer/publisher/projection worker. Simulator becomes500m; maintenance/reconciler125m each. Every pod retains512Mi memory. All other settings/pools/TTL/workload/recovery gates remain unchanged.

Introduce a distinct ADR0357 worker identity with exact per-role counts/resources in both local admission and hash-sealed transferred observation. Reject missing/extra workers, old uniform quotas, CPU/memory growth, source/manifest/process/env/UID/IP drift and incomplete diagnostics. Bind the allocation inside the roundtrip qualification contract; allow only these explicit placement/resource differences from the clean lower-rate image. Do not treat this as a pure rate-only comparison. The same redistribution must be retained from a passed180/s allocation round into200/s.

Use fresh identities for each attempt under existing preapproval; never reset the consumed failed180. Keep all financial/latency/zero-drop and conservative in-window issuance gates. Extend the existing bounded financial auditor's explicit show-count allowlist to200 without changing its seven predicates, RR READ ONLY snapshot, timeouts or count relationships; compile/test the actual200 builder before deployment.

## Alternatives

Repeat the unchanged failed180: no new limiting evidence. Enable an already-enabled write pipeline: no effect. Reapply rejected outer pipelining: contradicted by local evidence. Rewrite financial SQL: larger correctness change than reallocating existing idle worker budget. Add nodes/VMs/backends: outside this fixed-aggregate experiment. Relax quality/audit gates: rejected.

## Consequences

Writer service capacity and simulator CPU headroom increase while consumer/maintenance/reconciliation headroom decreases. Monitor consumer lag, expiry/refund queues, CPU and full drain; no performance gain is assumed. This evaluates one bounded allocation configuration, not isolated attribution to each changed role. No additional cloud capacity or hour-long workload is introduced.

## Persistence, locking, messaging, idempotency, TTL and scaling

No transaction, locks, messages, idempotency, SQL or expiry semantics change. Kafka ownership rebalances across five consumers; six partitions remain. Writer identity/Redis consumer ownership stays per-pod unique and replay-safe. Only role counts and CPU quota distribution change inside the same15-pod3750m budget. Commit-before-Redis ACK, duplicate handling and durable financial audits remain mandatory.

## Failure and recovery

Fail closed on any identity/resource/diagnostic mismatch or lower-rate prerequisite failure. Failed quality stops upward progression. Reconcile actual dispatched cohort with existing bounded database audit if offered counts differ; preserve original failure. Drain all queues and restore original new-RDS runtime. Disable the allocation to restore historical configuration; no reset or automatic resize.

## Validation evidence

Focused169 regressions passed (two optional actual-image cases skipped). Exact candidate-image helper rehearsal then passed separately with networking disabled:22 captured helper files and all22 runtime module hashes verified, strict transferred allocation identity, actual CLI/hash rejection and five-writer/five-consumer installation. Retained inventory timestamp is explicitly synthetic structural replay, not fresh cloud topology. Final focused run41 passed/one new test failed on missing imports/one optional skipped; corrected imports and reexecuted that actual inherited startup test successfully. Both200 same-allocation prerequisite rejection and actual200 audit builder with all seven predicates passed. Ruff passed. Actual four-API/15-worker preparation proved3750m/7680Mi and exact quotas/images; real reservation passed in isolated bookkeeping against genuine clean160/s evidence, leaving live bookkeeping untouched. Source contract and prepared goal hashes are in [readiness](../capacity/cce/fixed-worker-allocation-180-readiness-2026-10-19.json). No new paid attempt started at readiness.
