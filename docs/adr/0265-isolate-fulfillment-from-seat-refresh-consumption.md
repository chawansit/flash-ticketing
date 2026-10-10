# ADR0265: Isolate fulfillment from seat-refresh consumption

## Status

Accepted for implementation and local correctness validation; disabled by default. Cloud comparison and capacity qualification are pending. Supersedes ADR0261's shared consumer progress only when enabled. Its mixed default, stable refresh locking, batching and inbox replay guarantees remain supported.

## Context

ADR0263 reduced payment pickup waiting to 105.6 ms mean, but payment-to-ticket p95 remained 6.14 seconds. Paid-but-unfulfilled orders peaked at 348, and SeatsChanged handling recorded 24 SQLSTATE 55P03 failures. Fulfillment and refresh advance the same Kafka group: refresh retries at the end of a partition batch delay its next business events. This dependency is real; its contribution to total latency remains unmeasured.

The goal is 100 unique paid-and-issued tickets/second, providing 20% throughput headroom over 300,000 tickets/hour. Offered journeys and HTTP requests are not successful ticket throughput.

## Decision

Introduce opt-in EVENT_CONSUMER_SEPARATION=1. The existing consumer role and ticketing-fulfillment-v1 group handle OrderPaid, TicketsIssued and RefundRequested, skipping valid SeatsChanged records without SQL or cache work. A projection-consumer role uses ticketing-seat-projection-v1 and handles only SeatsChanged, preserving bounded batching, coalescing and stable show lock order. Each group advances independently over ticketing.events.

Use independent groups on the same topic for the first isolated correction rather than changing publisher routing/topics simultaneously. The durable outbox, envelope, order partition key and publication acknowledgement remain unchanged. Both lanes validate event schema/type before skipping; unsupported records follow existing bounded retry/dead-letter behavior. Retry fallback applies the same lane filter. Offsets commit only after the selected lane's durable handling succeeds. Unselected valid records create no inbox row.

Retain the existing fulfillment inbox identity for handled events in both lanes. Event types are disjoint, preserving deduplication across mixed/split rollout and rollback without a schema migration. Payment, booking and ticket transactions, seat TTL, customer authorization and committed status-cache refresh stay unchanged. Do not add cache optimization or atomic payment/ticket finalization in this experiment.

Projection workers have a separately bounded local pool. A future cloud profile must keep the physical PgBouncer server cap at 24 and explicitly account for all worker pools, consumer counts, CPU and both groups' lag. No cloud rollout, image repointing or load is performed by enabling a local Compose profile.

## Alternatives

Separate topics: stronger traffic isolation, but introduces publisher routing and existing-record migration as extra factors. More fulfillment replicas in the same group: cannot exceed partition parallelism or remove refresh processing in each assigned partition. Skip refresh without another durable consumer: loses event-driven freshness. Change payment finalization now: changes a separate transaction boundary before this dependency is evaluated.

## Consequences

Refresh retry/lock time no longer delays fulfillment group progress. Both groups read the stream, increasing Kafka reads and envelope decoding; database work is performed only in the selected lane. Shared CPU, PostgreSQL and Redis remain common resources, so independent offsets do not guarantee resource isolation or 100 tickets/second. Six partitions still bound each group's parallelism. Projection lag becomes a separately monitored freshness risk; reconciliation remains a recovery mechanism, not permission to ignore it.

## Failure and recovery behavior

The projection role refuses startup unless separation is enabled. Start and verify it before switching fulfillment to split mode. For a fresh isolated fixture, earliest replay is safe; historical refresh envelopes deduplicate against existing inbox rows. For a production rollout, retain Kafka history covering the handoff, pause intake, drain the mixed group and refresh work, record every partition offset, initialize the new group at those verified offsets while inactive, then resume both lanes. Never seed offsets from current end positions while writes remain active.

If projection fails, fulfillment may continue, but freshness/lag gates must fail and stop escalation. Restart replays uncommitted envelopes; refresh requests and inbox rows commit together. Lost Kafka acknowledgement replays into the same inbox and must not generate extra tickets/refresh work. Poison records remain visible in dead_letters and block qualification. Check lag for both groups plus outbox, refresh, financial and reservation queues before declaring drain.

Rollback: stop intake, drain both groups and refresh requests, stop projection, then restore mixed consumers with separation disabled. If either lane cannot drain, recover its recorded offsets and durable work before rollback; never assume fulfillment's skipped refresh offsets prove projection completion. Preserve the immutable control image and failed evidence.

## Validation evidence

Implementation and local validation completed. Covered checks: refresh lock failure while fulfillment progresses and commits independently; separate group identities; replay after restart/lost offset acknowledgement; mixed/split rollback deduplication; poison/fallback routing; unchanged duplicate callbacks, holds, late-payment refund, payment durability and ticket uniqueness. A fresh immutable candidate and controlled 84/s comparison are required before testing 100/s or claiming improvement.

Baseline: [ADR0263 result](../capacity/cce/payment-dispatch-16-result-2026-10-10.json). No new cloud load or measured improvement is claimed.

Executed validation: 34 focused unit tests passed. Inside the actual candidate, 138 integration tests passed against isolated PostgreSQL 17.6, Redis 7.4.5 and Kafka 3.9.1; no tests skipped. The real Kafka test held a refresh row lock, verified the fulfillment group committed offset 2 while projection had no committed offset, then released the lock, restarted projection and redelivered envelopes without duplicate tickets or refresh generations. Existing hold, callback, lost-commit-response, expiry/refund and committed-status-cache tests also passed. Ruff passed for all changed Python files. The opt-in Compose overlay resolved both roles to the same candidate, with local fulfillment/projection pools 10/2.

Candidate derived from the actual ADR0261 image. Only config.py and workers.py differ; existing business transactions and consume_events/consume_refresh_batch ASTs are unchanged. The only pre-existing config difference was declaration order of the writer fields; alignment to the measured parent changes no named runtime settings. All 22 copied, installed and imported modules and frozen dependency inputs verified. Published and pulled immutable SWR manifest sha256:cad773daf44496d91da263edd9fdf4121200ea64116afe218af2f8e0a9cee947; the pulled runtime was reverified. [Candidate receipt](../capacity/cce/event-lane-image-2026-10-10.json).

No cloud deployment, paid load, 100/s result or production qualification performed. The current cloud runner audits one event group; before using this candidate it must verify projection startup/identity, retain lag for both groups and require both to drain. An old runner's single-group zero cannot qualify this feature. For the initial cloud correction, predeclare worker counts and rebalance aggregate local pools while keeping physical PostgreSQL connections capped at 24. Keep API count, gateway delay, workload and customer gates fixed. Start with one fresh 84/s five-minute correction; failed gates stop escalation. A 100/s ten-minute test is conditional on every short-stage gate passing. Hourly qualification counts distinct tickets issued within the measured hour, excluding the completion tail.
