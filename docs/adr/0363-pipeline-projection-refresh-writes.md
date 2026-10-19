# ADR0363: Pipeline projection refresh writes

## Status
Accepted for fresh180 qualification after executed local validation. No cloud capacity pass claimed.

## Context
ADR0362's180/s run143a58c24860 completed54000 journeys with0drops,3recovered payment503s and0final failures. Financial, post-TTL, drain and full restoration passed; projection observation failed with1996events remaining (peak13830). The previous clean180 had peak512 and finished0. Current projection executed2679batches/293.879busy seconds, versus8633/276.830 for the clean reference; the current observer counted95522INSERT calls. Its refresh transaction reads the inbox result then sends each sorted show upsert synchronously. This can amplify batch service time as backlog grows. These are observed associations, not proof of causation.

## Decision
Validate one source change over the previously clean combined source19da6fb554d1e592e3eba59a52427b10510c213ab62c2a80181b8d5d27123925: enable the existing PostgreSQL transaction pipeline in consume_refresh_batch. Preserve every SQL statement, envelope validation, inbox uniqueness, sorted per-show lock order, refresh generation/seat union/full-refresh semantics, TTL, retry and commit-before-Kafka-offset behavior. Fetching the inbox result remains a synchronization point; subsequent write-only upserts share transport flushing. No new schema, persistence store, locks, messaging contract, idempotency key, TTL or resource scaling. ADR0362 remains a failed experiment and is not included in this image.

This explicitly supersedes ADR0355's same-image/rate-minus20 rule only for a new-image180 qualification using the actual clean parent180 on identical ADR0359 resources.200 requires an actual clean180 with this exact new image and allocation. All correctness, zero-drop, initial-error-free prerequisite, financial, drain and restoration gates remain.

## Alternatives
Blind repeat: no corrective evidence. More projection CPU/replicas: changes resources and partition ownership. Rewrite bulk upserts: larger SQL and lock-order proof. Increase admission limits: does not address observed projection work. Retain ADR0362: its small writer improvement did not produce a clean qualification. Pipeline the reservation outer transaction: rejected by ADR0320 and not part of this choice.

## Consequences
Fewer transport waits are expected for multiple refresh writes; actual benefit must be measured. Server execution, locks and statement deadlines remain. Pipeline errors may surface at synchronization or context exit, so rollback/replay and no-offset-on-error tests are required. This does not establish that payment503s are solved.

## Failure and recovery
Any write failure rolls back inbox and all refresh intents; Kafka offsets remain uncommitted and existing bounded retry/poison handling applies. Commit acknowledgement loss replays inbox uniqueness without incrementing generation again. Rollback uses the immutable parent image. Failed cloud attempts remain immutable, restored, closed and separately classified; no consumed sequence reset, main merge or automatic infrastructure resize.

## Validation evidence
Actual single-module image built from the immutable clean parent; copied and installed22modules verified.38 PostgreSQL cases passed, including multi-show rollback after a late locked write, concurrent opposite input order, full-refresh/union/completed-generation and lost-ack replay. One real Kafka integration case skipped because the isolated suite has no broker;20 consumer/offset cases passed separately in the actual image with networking disabled. Two AST/source scope checks passed: only pipeline=True changes, with every other node and SQL identical.

80 alternating pairs per arm with controlled1ms one-way delay:32-show batch median129.489 to19.969ms (84.58% lower),p95159.067 to26.349ms. SELECT1median3.640ms.5760inbox rows and32refresh rows with all180generations verified. All owned containers/network removed. This is local latency evidence, not RDS throughput.

Published/pulled manifest a8531f98d23c4d5f3197cd5273d75c0508dbffb54e846a4f8963dd25d7787c7b, configuration dd577b4adfdcea0bd295d47ce2c62d19b70fc84a343e22b3f23c3aa87f48e505, source72fabf5016df66fd73cfdb0faf05e4c1f80fa41868afa044c266609ffed665f6.152 focused runner cases passed,4optional skipped. Separate actual-image22-helper/22-runtime rehearsal passed; inventory timestamp is explicitly synthetic structural evidence. Actual manifests/reservation passed in isolated bookkeeping with the genuine clean parent180; live allowance unchanged. Final source-seal metadata verification is retained in the readiness receipt. [Local evidence](../capacity/cce/projection-refresh-pipeline-local-2026-10-19.json), [publication](../capacity/cce/projection-refresh-pipeline-registry-2026-10-19.json).
