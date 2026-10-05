# ADR0151: Identical-cache-age committed status refresh comparison

- Status: Proposed; local manifest prepared, runner/image qualification and execution approval pending
- Date: 2026-10-05

## Context

ADR0149 proved local event-refresh correctness on an isolated frozen-base candidate, but no cloud performance improvement. ADR0148 found status-read admission failures alongside payment/callback failures. Its cache-disabled84-buyers/s probe failed. Enabling refresh requires a positive bounded cache age, so comparing cache-disabled control against cache-enabled refresh would change two factors. The passing ADR0147 topology pair is a placement comparison and cannot execute this experiment unchanged.

## Decision proposed

Compare refresh off/on at 1,000 ms advisory cache age on both arms, using the same isolated per-role images, two APIs on each of the same hosts, frozen generator, connection budgets, polling interval, admission and callback settings. Only consumer ORDER_STATUS_EVENT_REFRESH changes 0 to 1. API and consumer ORDER_STATUS_CACHE_MS is 1000 in both arms; other background services keep refresh disabled. No coalescing/callback-reserve/seat-projection candidate is included.

Use 60 distinct buyers/s for 300s, 18000 unique isolated seat journeys per arm, 500 active journeys across two identical shards, 1s status polls, callback target 1, no customer retries and 420s completion deadline. These are proposed limits, not a paid-run authorization. Run the control first and stop on any required failure. A matching enabled-cache control is necessary even though prior cache-disabled placement results passed.

The prepared [manifest](../capacity/flash-sale-opening/order-status-event-refresh-comparison-plan-2026-10-05.json) records 13 locally verified runtime hashes and fixed budgets. Extend the runner with a separate consumed-on-dispatch approval ledger and identical placement for both arms before seeking concrete execution approval. Existing ADR0147/0148 qualification cannot be reused after the adapter/source/config changes. Build and verify both /app and installed import sources; preserve per-role image provenance and verify all API/background containers before fixtures/customer dispatch.

## Alternatives

Compare against cache-disabled historical results: confounds cache age, placement and runtime conditions. Test 84 buyers/s immediately: does not first establish an interpretable control. Enable coalescing or callback reservation together: hides attribution. Raise generator limits: changes the measured workload. Treat local correctness tests as capacity evidence: unsupported.

## Consequences

This first comparison measures event refresh, not the 300000-ticket/hour target. It may reduce cache-miss reads and polling latency, or increase consumer reads and CPU/Kafka lag. Record customer confirmations/errors/drops, paid issued counts, status reads per journey/cache metrics, callback backlog, consumer query/connection utilization, both API-host CPUs and per-replica distribution. Attribute differences only between these identical-cache arms; do not attribute all gains over an older cache-disabled run to event refresh. A short comparison cannot certify hourly production capacity.

## Persistence, messaging, idempotency, TTL and scaling

No hold/payment transaction, locking, outbox/Kafka delivery, idempotency, hold TTL or connection-budget changes. ADR0149's advisory freshness policy remains unchanged. Only the candidate comparison's existing cache-disabled assertion must be replaced with an explicit identical 1000ms bounded-cache assertion; retain all latency, customer, financial, zero-double-booking, full queue/Kafka, observation and restoration gates. ADR0147's historical cache-disabled results remain valid historical evidence and are not rewritten. No financial decision is superseded.

## Failure and recovery

Source/import/image/start drift, ownership mismatch, missing required observations or failed safety gates prevent paid dispatch. Failed control prevents candidate dispatch. A dispatched stage consumes its allowance even if its response is lost. Audit payment durability and overlapping bookings after hold deadlines, drain queues/Kafka and restore the exact prior runtime for every arm, including failure. Use ADR0150 bounded owned-stop transport recovery; never retry customer journeys or deployment RPCs automatically. No replacement, higher rate, hour-long run, infrastructure resize or publication follows automatically.

## Validation evidence

The local manifest's13 runtime sources match the ADR0149 isolated source directory after LF normalization. That candidate previously passed 155 focused and 663 full native tests, as recorded in ADR0149. ADR0150 local harness checks are separately recorded; no new candidate images, cloud preflight/safety tickets, paid comparison or performance improvement is claimed here. Candidate-aware runner/image preparation and fresh dry qualification remain future work.
