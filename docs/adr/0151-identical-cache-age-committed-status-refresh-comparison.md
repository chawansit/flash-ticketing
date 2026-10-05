# ADR0151: Identical-cache-age committed status refresh comparison

- Status: Accepted; images staged and corrected dry pair passed/restored; paid comparison requires separate approval
- Date: 2026-10-05

## Context

ADR0149 proved local event-refresh correctness on an isolated frozen-base candidate, but no cloud performance improvement. ADR0148 found status-read admission failures alongside payment/callback failures. Its cache-disabled84-buyers/s probe failed. Enabling refresh requires a positive bounded cache age, so comparing cache-disabled control against cache-enabled refresh would change two factors. The passing ADR0147 topology pair is a placement comparison and cannot execute this experiment unchanged.

## Decision

Compare refresh off/on at 1,000 ms advisory cache age on both arms, using the same isolated per-role images, two APIs on each of the same hosts, frozen generator, connection budgets, polling interval, admission and callback settings. Only consumer ORDER_STATUS_EVENT_REFRESH changes 0 to 1. API and consumer ORDER_STATUS_CACHE_MS is 1000 in both arms; other background services keep refresh disabled. No coalescing/callback-reserve/seat-projection candidate is included.

Use 60 distinct buyers/s for 300s, 18000 unique isolated seat journeys per arm, 500 active journeys across two identical shards, 1s status polls, callback target 1, no customer retries and 420s completion deadline. These are proposed limits, not a paid-run authorization. Run the control first and stop on any required failure. A matching enabled-cache control is necessary even though prior cache-disabled placement results passed.

The prepared [manifest](../capacity/flash-sale-opening/order-status-event-refresh-comparison-plan-2026-10-05.json) records the complete 19-module isolated runtime source map and fixed budgets. The local runner uses a separate consumed-on-dispatch approval ledger and identical placement for both arms. Existing ADR0147/0148 qualification cannot be reused after the adapter/source/config changes. Build and verify both /app and installed import sources; preserve per-role image provenance and verify all API/background containers before fixtures/customer dispatch.

## Alternatives

Compare against cache-disabled historical results: confounds cache age, placement and runtime conditions. Test 84 buyers/s immediately: does not first establish an interpretable control. Enable coalescing or callback reservation together: hides attribution. Raise generator limits: changes the measured workload. Treat local correctness tests as capacity evidence: unsupported.

## Consequences

This first comparison measures event refresh, not the 300000-ticket/hour target. It may reduce cache-miss reads and polling latency, or increase consumer reads and CPU/Kafka lag. Record customer confirmations/errors/drops, paid issued counts, status reads per journey/cache metrics, callback backlog, consumer query/connection utilization, both API-host CPUs and per-replica distribution. Attribute differences only between these identical-cache arms; do not attribute all gains over an older cache-disabled run to event refresh. A short comparison cannot certify hourly production capacity.

## Persistence, messaging, idempotency, TTL and scaling

No hold/payment transaction, locking, outbox/Kafka delivery, idempotency, hold TTL or connection-budget changes. ADR0149's advisory freshness policy remains unchanged. Only the candidate comparison's existing cache-disabled assertion must be replaced with an explicit identical 1000ms bounded-cache assertion; retain all latency, customer, financial, zero-double-booking, full queue/Kafka, observation and restoration gates. ADR0147's historical cache-disabled results remain valid historical evidence and are not rewritten. No financial decision is superseded.

## Failure and recovery

Source/import/image/start drift, ownership mismatch, missing required observations or failed safety gates prevent paid dispatch. Failed control prevents candidate dispatch. A dispatched stage consumes its allowance even if its response is lost. Audit payment durability and overlapping bookings after hold deadlines, drain queues/Kafka and restore the exact prior runtime for every arm, including failure. Use ADR0150 bounded owned-stop transport recovery; never retry customer journeys or deployment RPCs automatically. No replacement, higher rate, hour-long run, infrastructure resize or publication follows automatically.

## Validation evidence

The original manifest's 13 selected runtime sources match the ADR0149 isolated source directory after LF normalization. That candidate previously passed 155 focused and 663 full native tests, as recorded in ADR0149. ADR0150 local harness checks are separately recorded; no new candidate images, cloud preflight/safety tickets, paid comparison or performance improvement is claimed here. Implement a default-local CLI with explicit qualify/execute modes. Use two separate existing bounded topology protocols, one per logical arm; skip their original four-primary paid control hooks, then dispatch the logical arm only after both hosts have two APIs and a fresh cross-host safety/payment/replay/post-TTL audit. Restore the original primary runtime, remove secondary test resources, prove global drain and generator idle after each arm before proceeding. Each protocol gets one safety ticket; an approved dry pair and paid pair therefore need at most four safety tickets total. Read-only source verification precedes the safety fixture itself.

Parameterize existing observation only through an explicit candidate contract: identical1000ms API cache, refresh disabled on APIs and other workers, consumer cache1000ms with arm-specific flag, full isolated runtime-source map and matching immutable per-role images. Retain existing frozen behavior when no contract is supplied. For this experiment rename only the historical cache_disabled gate to bounded_equal_cache_age; keep every other original/cross-host gate and require exact gate sets.

Validate artifact parent layers and the recorded source-manifest label before mutation. Image construction/export/transfer remains separate work; the runner cannot build/pull images. Default preparation performs no SSH, state approval or paid dispatch. Explicit cloud modes require a new exact configuration/artifact/adapter-bound allowance and fresh matching dry qualification for paid execution. An exclusive owned local run lock blocks simultaneous or ambiguous replays. Count protocol and safety allowance before starting its SSH workflow, and record each paid launch before the generator call. Missing/ambiguous evidence remains failed; never clear the active marker merely because an exception occurred. No automatic qualification followed by paid execution and no automatic replacement are allowed.

Local unit/synthetic protocol tests exercise fixed placement/factor checks, source/image/cache/budget drift, exact gate retention, control-failure stopping, per-arm restoration, missing-evidence rejection and allowance exhaustion before cloud access. Executed results are recorded in the local runner validation report below. No cloud test is authorized by this decision.

The remote observer receives the canonical SHA256 of the exact inventory already validated on the host, together with an explicit ADR0151 marker. It checks that digest and actual1000ms cache/immutable image/2+2 placement before reusing the unchanged legacy structural/budget validator on a copy with only its historical placement/cache assertions normalized. Original evidence remains untouched; this is an observer-installation adapter, not a relaxed paid gate. No host-only candidate module or credentials are transferred to import on the observer. The same existing scrape extracts bounded order-cache outcome counters in both arms; no extra scrape or generator request is added. Report counter-reset/coverage gaps explicitly rather than inventing a hit rate or RPS from partial observations.

Dry-only authorization may reserve one qualification pair and two safety tickets with zero paid allowance; paid mode requires explicit two-paid/four-safety scope. The dry receipt expires after3600s and must bind exact source, artifact, config and adapter identity. Scope changes never launch a paid pair automatically. Preparation currently needs the previously qualified isolated source directory; reproducible source export and image construction are deliberately separate pending work, documented in the [runner guide](../capacity/flash-sale-opening/order-status-event-refresh-runner.md).

The [local runner validation report](../capacity/flash-sale-opening/order-status-event-refresh-runner-local-validation-2026-10-05.json) records 350 executed focused harness tests and passing lint/diff checks, including both successful and unavailable cache-measurement reporting. These are local unit/synthetic protocol checks, not a cloud safety, durability or capacity result. Default preparation was executed with zero cloud calls and zero customer dispatches. Candidate image construction, reproducible isolated-source export and fresh live qualification remain pending.

ADR0152 supersedes the ignored temporary-source prerequisite with a pinned eight-file patch, reproducible Git export and offline per-role image derivation. The runner now points to that reproduced source tree. The original local test report remains historical evidence; the changed experiment-plan binding requires a fresh preparation receipt. No execution allowance or live gate is superseded.

The approved dry pair6d045bfad023 exercised one control safety ticket: loaded-source, payment replay, post-TTL durability and full queue gates passed. Both CPU observers rejected the inherited four-primary control mapping; candidate was stopped and original runtime/queues restored. ADR0154 adds explicit observer placement. Original failed evidence and consumed allowance remain; no corrected replacement or paid comparison has run.

The separately approved corrected drycb2e26816335 passed both fixed2+2 off/on arms with two simulated safety tickets, complete required qualification/restoration and zero capacity stages. See the [corrected qualification report](../capacity/flash-sale-opening/status-refresh-corrected-dry-qualification-2026-10-05.json). Prior failed control remains failed historical evidence. No new capacity result is claimed.
