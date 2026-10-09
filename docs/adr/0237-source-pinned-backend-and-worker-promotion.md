# ADR0237: Promote source-pinned backend and worker behavior

Status: Accepted for main-branch review and local validation; no cloud deployment or capacity qualification.

## Context

Schema prerequisite PR #5 is merged. The hourly evidence used separate frozen API, consumer and writer images; current experimental checkout contains later changes that are not part of those images. All 21 runtime files in the retained API image have been exported locally and matched to the recorded SHA-256 contract. The retained consumer and writer source exports separately match all 21 recorded hashes. Consumer differs only in workers.py; writer differs in config.py, workers.py and infrastructure/reservations.py.

## Decision

Use the verified API source as the base. Adopt the writer's config.py and infrastructure/reservations.py, retaining default-disabled, writer-only pipeline activation. Assemble workers.py from the consumer version with only main() replaced by the writer version: every other top-level syntax node must remain identical to the verified consumer source, and main() must remain identical to the verified writer source. Record original per-role file hashes and the resulting combined source hashes. Sort imports in postgres.py and reservations.py only to satisfy repository lint; compare all non-import syntax and import sets against the retained sources to prove no algorithm change. This integrated runtime is a new source identity; its tests must execute against this checkout. It is not byte-identical to any single measured image, and has no measured capacity of its own.

Retain existing Compose topology, budgets, dependencies and migration behavior. No CCE adapter, paid generator, cloud credentials, infrastructure or new deployment profile is promoted in this PR. Add focused regressions from the verified source exports, plus consumer-coalescing and writer-pipeline regressions corresponding to their recorded corrections. Resolve necessary test imports without importing unrelated cloud orchestration.

Adopt the following existing experimental behavior for main:

- PostgreSQL remains the durable seat/booking authority. Redis-first intake is provisional until the bounded writer transaction commits; only then publish a durable marker and acknowledge the stream. Retain sorted locks, per-command savepoints, idempotency, replay and exactly-one booking/ticket constraints. Extend writer cursor advancement so visited streams cannot starve other shows; bounded batches remain 1 by default, allowed 1â€“8.
- Retain the fixed hold TTL default of 120 seconds. Do not extend a hold on replay. Optional command-age rejection leaves at least 30 seconds before expiry; production Redis-first mode requires replica acknowledgement.
- Keep synchronous financial callback confirmation as the default. When PAYMENT_CONFIRMATION_ASYNC=1 is explicitly configured, acknowledge only after verified durable receipt admission; a separately started confirmation worker applies the financial transaction with bounded retries, lease fencing, visible REVIEW failures and idempotency. Receipt acceptance does not mean payment/ticket completion. This partially supersedes ADR0004 only at webhook acknowledgement when enabled; its financial/late-payment semantics remain.
- Retain outbox/inbox at-least-once delivery and commit-before-offset acknowledgement. Coalesce SeatsChanged intents across a bounded partition batch while preserving the relative order and separate transactions of business events; persist deferred intents before offset commit. Deterministic show lock order avoids opposite-batch lock cycles. This adopts the ADR0222 correction to the historical consecutive-only coalescing restriction; business transaction batching is not introduced.
- Optional committed status projection/cache has bounded snapshot age and customer authorization. Cache, event refresh, deduplication and poll guidance remain disabled by default. Consumer projection is advisory; financial completion remains in PostgreSQL.
- Optional API payment pool partition and shared acquisition/reclamation keep the configured total connection budget fixed. They remain disabled by default. External gateway calls and callback waits must not hold database connections.
- Writer pipeline only changes transport synchronization of already-validated statements inside the existing savepoint/outer transaction. Pipeline errors propagate before outcome/commit/Redis acknowledgement; do not retry an ambiguous commit as a new order.

No outbox/Kafka/idempotency/TTL guarantee is weakened and no new horizontal-scaling decision is made. Prior accepted main decisions continue except the explicit optional callback boundary above. Exact default/feature-flag and source provenance tables accompany the PR.

## Alternatives

- Copy current experimental src: rejected; it does not match the measured image contracts.
- Promote only the API image's workers.py: rejected; it omits the measured consumer correction and optional writer pipeline.
- Ship three independent source trees in one repository: rejected because duplicated domain/persistence code could diverge silently.
- Merge unrelated cloud runners and deployment changes now: deferred to a separate dependency-complete PR.

## Consequences

A single repository can build the reviewed behavior of all worker roles. Optional features require explicit configuration; the measured CCE settings are not the local defaults. New combined source needs its own local/full-stack validation and a separately authorized cloud comparison before performance claims. One repository integration cannot close the recorded hourly payment 503 or observation gaps. The 18,000-seat contention/projection issue remains separate and unresolved.

## Failure and recovery behavior

Preserve failed tests and repair only demonstrated integration defects. Source mismatches or unexpected feature activation block publication. Failed domain commands do not abort successful siblings; failed SQL/commit rolls back uncommitted work, and committed-but-unacknowledged commands replay from durable identity. Kafka redelivery uses inbox/ticket uniqueness. An unavailable gateway retains leased/retryable payment work; exhausted durable receipts remain visible for review, never silently discarded. Cache or projection failure cannot authorize a booking or erase financial truth.

Runtime rollback can retain migrations 007â€“010. If asynchronous receipts were enabled, do not remove the worker or drop receipt data while outstanding financial work exists: stop admission, drain/reconcile and verify before rollback. No cloud rollback or feature activation occurs in this repository-only task. Existing main-merge permission still requires user approval for this PR.

## Validation evidence

Before implementation: API image and consumer/writer exports matched all 21 hashes per role. Next execute native PostgreSQL/Redis regressions covering 100 contenders/one durable owner, starvation, commit-response loss/replay, payment duplicate/expiry/fencing, ticket issuance/inbox/outbox and complete owned queue drain. Verify default flags and configured budgets. Run full-stack HTTP/Kafka CI, lint, naming and private-artifact checks before merge recommendation. Append actual outcomes without relabelling failed experiments. No cloud load or capacity improvement is measured here.

Historical source/decisions are pinned to [experimental revision 750ad8a](https://github.com/chawansit/flash-ticketing/tree/750ad8a4bf680a8440c001f2300b7fa8d56d8e77). Relevant exact decisions: [writer continuation](https://github.com/chawansit/flash-ticketing/blob/750ad8a4bf680a8440c001f2300b7fa8d56d8e77/docs/adr/0122-reservation-writer-visited-stream-continuation.md), [durable confirmation](https://github.com/chawansit/flash-ticketing/blob/750ad8a4bf680a8440c001f2300b7fa8d56d8e77/docs/adr/0160-durable-asynchronous-payment-confirmation.md), [atomic dispatch claim](https://github.com/chawansit/flash-ticketing/blob/750ad8a4bf680a8440c001f2300b7fa8d56d8e77/docs/adr/0189-atomic-payment-dispatch-claim.md), [interleaved refresh](https://github.com/chawansit/flash-ticketing/blob/750ad8a4bf680a8440c001f2300b7fa8d56d8e77/docs/adr/0222-coalesce-interleaved-seat-refresh-intents.md), [writer pipeline](https://github.com/chawansit/flash-ticketing/blob/750ad8a4bf680a8440c001f2300b7fa8d56d8e77/docs/adr/0225-pipeline-reservation-command-writes.md).

Executed local evidence: 580 native Linux unit/integration tests passed in 121.66 seconds, with two full-stack HTTP/Kafka tests skipped locally. Four additional combined sequential/pipelined and synchronous/asynchronous financial journeys passed in 1.90 seconds on real PostgreSQL 17.6 and Redis 7.4.5 with simulated gateway/broker transports. They verified duplicate delivery, committed ticket uniqueness, controlled consumed-hold expiry and zero owned pending stream/payment/receipt/outbox/refresh work. Source imports and final file hashes were verified. Initial collection/test-contract failures are retained in the sanitized evidence; they required no new backend algorithm change. Owned containers/networks were removed. Full-stack PR CI remains pending. [Validation and source provenance](../capacity/backend-workers-promotion-2026-10-09.json), [default flags and operational limits](../backend-runtime-promotion.md).
