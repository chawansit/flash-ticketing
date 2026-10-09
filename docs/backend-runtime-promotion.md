# Reviewed backend and worker integration

This PR follows schema PR #5. It promotes verified runtime source and correctness regressions; it does not deploy services, change machine sizes or qualify production throughput.

## Source identity

The retained API image supplies the base runtime. Consumer and writer source exports each matched their separate 21-file hash contracts. The consumer correction coalesces interleaved refresh intents and orders show locks deterministically. The writer correction adds an optional transport pipeline.

A combined build cannot be byte-identical to all three role images. The integrated workers module uses the consumer source with main() from the writer source; syntax comparison verifies every top-level node's origin. Two standard-library import blocks are sorted for lint without changing non-import syntax or imported names. Original and integrated SHA-256 maps are retained in the [promotion evidence](capacity/backend-workers-promotion-2026-10-09.json). Local tests and full-stack CI must validate this new integrated identity. Later source changes require their own evidence; historical hashes must not be represented as hashes of those changes.

## Defaults and feature flags

These are application defaults with a clean environment. Existing Compose overrides still apply. The measured hourly profile explicitly configured different values; it is not activated by merging this PR.

| Setting | Default | Behavior and activation requirement |
| --- | --- | --- |
| RESERVATION_MODE | postgres | Redis-first must be selected explicitly; initial acceptance is provisional until DURABLE. |
| HOLD_SECONDS | 120 | Replay never renews expiry. |
| RESERVATION_WRITER_BATCH_SIZE | 1 | Bounded 1–8 commands; hourly profile used 4. |
| RESERVATION_WRITE_PIPELINE | 0 | Writer-only optimization; financial validation, transaction and commit-before-ACK remain unchanged. |
| PAYMENT_CONFIRMATION_ASYNC | 0 | Callback performs financial confirmation before acknowledgement. Setting 1 instead acknowledges durable receipt intake. |
| CONFIRMATION_MAX_PENDING | 10000 | Durable outstanding-receipt admission ceiling per configured provider. |
| CONFIRMATION_CONCURRENCY | 2 | When enabled it must fit the worker connection budget. A confirmation worker must be started explicitly. |
| CONFIRMATION_LEASE_SECONDS | 30 | Bounded 5–300; stale-token financial updates roll back. |
| CONFIRMATION_MAX_ATTEMPTS | 8 | Bounded 1–20; exhausted work remains visible in REVIEW. |
| CONFIRMATION_RETRY_MS | 100 | Bounded 50–5000 base delay; transient failures use capped jittered backoff. |
| ORDER_STATUS_CACHE_MS | 0 | Advisory cache disabled; when enabled snapshot age is bounded to at most 3000 ms. |
| ORDER_STATUS_EVENT_REFRESH | 0 | Requires enabled status cache; projects committed business events. |
| ORDER_STATUS_EVENT_REFRESH_DEDUP | 0 | Requires event refresh; skips eligible already-fresh fulfilled snapshots. |
| ORDER_STATUS_POLL_MS | 0 | No new polling hint by default; explicit 100–1500 ms hint includes 20% jitter guidance. |
| DB_POOL_MAX | 12 | Total API connection ceiling; enabling a payment partition divides this budget. |
| DB_POOL_MAX_WAITING | unset | Uses configured total connections as the default waiter budget; explicit range 1–64. |
| DB_POOL_WAIT_MS | 150 | Explicit range 50–1000; hourly CCE profile used 500. |
| API_PAYMENT_POOL_MAX | 0 | Partition disabled; when enabled must be smaller than total pool. |
| API_POOL_SHARED_WAITING | 0 | Requires a partition and sufficient shared acquisition budget. |
| API_PARTIAL_TIMEOUT_RECLAIM | 0 | Requires shared waiting; optional reclamation does not add connections. |
| SIMULATOR_DISPATCH_MODE | batch | Refill is explicit; external callbacks run without retained DB connections. |
| SIMULATOR_CONCURRENCY | 4 | Existing bounded simulator concurrency validation still applies. |

Redis-first production requires at least one replica acknowledgement. Optional command-age rejection must leave at least 30 seconds before expiry. Cache authorization is checked per customer; an advisory cache cannot create seat ownership or establish financial truth.

Enabling asynchronous confirmation requires both verified durable receipt admission and an explicitly running confirmation worker, with the shared database connection ceiling reviewed. Merging code or setting an environment variable only on the host does not start that service or guarantee that Compose forwards that variable. Deployment wiring remains a separate PR.

## Payment and ticket flow

1. Default PostgreSQL holds return after commit. Redis-first holds return provisional acceptance and require command-status confirmation.
2. Payment initiation commits a durable simulated payment attempt. Payment gateway HTTP occurs outside the database transaction.
3. Default callbacks verify their signature and commit financial changes/outbox before success acknowledgement. Optional asynchronous callbacks commit a receipt and return RECEIVED, not paid or fulfilled.
4. Receipt worker, if enabled, applies the same financial transaction once using leases and idempotency; late success refunds rather than reclaiming a reassigned seat.
5. Outbox publisher sends with at-least-once semantics. Consumer inbox and unique booking/ticket constraints make redelivery safe. Business handlers retain separate transactions and offset commit waits for deferred refresh persistence.
6. Customer ticket availability follows committed fulfillment. Status cache is bounded and advisory.

The frozen API uses the existing explicit simulator delay_seconds input, whose default is 1 second. The hourly client sent 0; the later local bank-like randomized simulator is not included in this promotion. This source integration does not change that historical test or claim its gateway latency represents a real bank.

## Verification and operational limits

Required regressions cover atomic multi-seat holds, 100 same-seat contenders, writer progress across busy shows, commit-before-marker replay, pipeline SQL/commit failures, signed callbacks, duplicate payment/event delivery, lease fencing, authorization, late-payment refunds, unique issued tickets and owned queue drain. Native PostgreSQL/Redis results and full HTTP/Kafka CI must be reported separately.

The earlier hourly result and its payment 503/diagnostic failures remain unchanged. The 18,000-seat projection issue remains unresolved. This PR makes runtime behavior reviewable on main; it does not establish a capacity improvement.

Rollback can retain additive schema. After async receipts are enabled, stop intake and drain/reconcile outstanding receipts before removing confirmation workers. Never erase receipt or payment history as automatic cleanup.
