# ADR0144: Compare enabled-cache control with isolated miss coalescing

- Status: Accepted bounded comparison; enabled-cache control failed, conditional candidate skipped
- Date: 2026-10-04

## Context

ADR0143 locally passed 121 focused and 739 full tests, but its branch also contains the cloud-unqualified seat-map projection. ADR0142's latest pinned cloud control used cache0 and failed customer/ticket gates despite subsequent complete simulated refund accounting. Comparing that disabled cache against enabled coalescing would combine two factors. User Continue authorizes the proposed enabled-cache control followed by one coalescing candidate only if every control gate passes.

## Decision

Create an isolated Git revision from deb330e by importing only ADR0143's application cache port/status-read method, Redis order cache adapter, bounded read coordinator and corresponding tests/docs. Verify the complete source diff, unchanged seat projection and financial mutation methods before deployment. Pin all fixture/generator/observer/orchestration scripts to deb330e and verify actual upload bytes. Validate the isolated candidate on owned native-Linux PostgreSQL/Redis services with source/dependency identity and meaningful regression tests before cloud use.

Execute at most one control with deb330e and ORDER_STATUS_CACHE_MS=1000, then at most one isolated candidate at the same1000ms age only if all control customer/financial/uniqueness/queue/Kafka/source/observer/CPU/restoration/cleanup gates pass. Do not proceed from a failed or missing gate. Use the existing one-stage ADR0040/0090/0142 lifecycle per experiment; no new unattended schedule or generalized paid-stage progression capability. Preserve failed control evidence and do not dispatch a replacement stage.

Both stages use60buyers/s300s,60shows/300seats,18000buyers,two shards/eight clients,500concurrency,1s polling and zero customer retries. Keep API4/pool4/shared acquisition12/payment2, consumer6/pool8, writer3/batch4, simulator8/refill, PgBouncer24/reserve0/client160. Change only cache0 to1000 for the enabling control; the conditional candidate changes only read coalescing with the same1000ms age. A cache-hit benefit cannot be attributed to coalescing. Retain the22 gates with cache_age_matched and isolated_runtime_deployed replacing the disabled-cache/historical-runtime identity names, without loosening criteria.

## Alternatives and consequences

Deploying current HEAD bundles the rejected projection factor. Comparing enabled coalescing against cache0 confounds activation. Higher load, longer TTLs, extra callback slots/connections or retries conceal the controlled question. Local coalescing reduces duplicate same-order reads but may add Redis recheck/thread waiting costs; distinct-order journeys may show no benefit. Fresh fixtures/cloud scheduling/data growth remain variation; no sustained300000/hour or production capacity is established.

## Persistence, locking, messaging, idempotency, TTL and scaling

PostgreSQL financial authority/seat uniqueness/locking, Redis atomic holds, writer replay, outbox/Kafka and payment/callback idempotency remain unchanged. The enabling control selects the already implemented1000ms advisory cache age; hold/command/payment TTLs do not change. No host, API/thread/pool/acquisition/simulator budget or schema choice changes. This extends ADR0143 only to a bounded isolated qualification; no production promotion or supersession of correctness decisions.

## Failure and recovery

Use previously authorized fresh restricted45minute temporary root keys on verified known hosts; compensate partial installation and remove exact owned entries/local key files at each experiment end. Passwords remain protected memory-only. Require clean cloud checkouts, source/image identity, normal readiness and idle generator before dispatch. Audit accepted payments after all hold deadlines, zero double booking, all global Redis/database queues and Kafka. Record later ticket/refund recovery separately; simulated refunds cannot pass failed ticket/customer gates. Always restore cloud deb330e source/images and normal cache0/service budgets, verify readiness/idle/private scratch, and remove keys. No candidate promotion, extra load, GitHub push or main merge follows automatically.

## Validation evidence

Recorded before new orchestration/source preparation. Require full isolated diff/source/dependency verification, local coalescing/atomic hold/payment replay regressions and report guard cases for missing tickets/duplicates/undrained queues/wrong source/harness/cache/layout. Confirm the conditional candidate is forbidden after any missing/failed control or cleanup gate. Preserve compact stage results and raw private artifacts separately. Executed results will be appended; planned steps are not implemented capacity.

## Executed qualification and recovery

One enabled-cache control ran on deb330ec91e553640d1d0ba10e92aa8f29cd86dc, run checkout-20261004T162012Z-ec4461. It passed19of22 gates and failed customer_load, post_ttl_financial and the inherited compound hold_deadlines_elapsed/cohort check. Actual hold deadlines elapsed.18000 buyers were scheduled,17968 dispatched,32 dropped;3799 confirmed a ticket by the client deadline and14169 failed journeys (78.8569% of dispatched journeys, not HTTP error percentage). No customer retries were used. Worst-shard hold-to-ticket p95 was74.420s and payment p95 was287.335ms. Cache activation alone did not qualify.

PostTTL audit found16981 successful simulated payments and16964 tickets, zero duplicate seats/multi-booking, completed callback deliveries and empty global queues/Kafka. Read-only post-restoration snapshot at2026-10-04T16:32:41Z accounted for16981 payments as16964 fulfilled plus17 simulated refunded, zero refund-pending;987 unpaid orders expired. Later refund accounting does not turn customer or exact paid-ticket gates into passes and does not confirm any real-provider refund.

Generator-anchored sampled CPU was97.580% and API1.950 cores, API acquisition mean64.660ms and event-loop lag mean16.726ms. Callback delivery mean173.619ms; callback backlog peak5909 and due-to-claim p95 histogram upper bound120s. Full-run physical status reads108207 (6.022 per dispatched journey). Historical passing cache0 control had CPU81.358% and acquisition2.881ms; current environment previously also failed the pinned cache0 reproduction, so the initiating regression cause cannot be attributed solely to cache activation or coalescing. No WAL/storage cause or production capacity is established.

Isolated coalescing revision dfb11bc8b6907bcaf2b9cde3d7e2f7ddf1724a19 passed111 focused and663 full native Linux tests, zero skipped. This is the isolated baseline tree's test set, not the later branch's739-test set. Exact archived bytes and locked dependencies were verified;13 normalized LF runtime hashes matched immutable Git source, while raw Windows archive Python files retained CRLF. Projection and financial mutation methods remained unchanged. Eight synthetic report guards and ten conditional-dispatch guard cases passed. Preparation failures were retained without cloud load: historical upload line endings, binary archive hash handling, and a newer test path absent from the baseline were corrected before their relevant execution.

The conditional candidate was not deployed or dispatched (candidate runs0). Cloud baseline source/images and normal service/cache0 settings were restored and verified; generator idle and private scratch cleanup passed. Exact temporary root keys were removed from both ECS hosts and local key files deleted. No higher load, retry experiment, GitHub push or main merge followed. Callback-priority admission within the existing budget is a possible separate decision; it is not implemented by this ADR.

Evidence: [control](../capacity/flash-sale-opening/enabled-cache-control-2026-10-04.json), [isolated local validation](../capacity/flash-sale-opening/isolated-coalescing-local-validation-2026-10-04.json), [conditional comparison plan](../capacity/flash-sale-opening/enabled-cache-coalescing-comparison-plan-2026-10-04.json). Raw evidence remains in ignored tmp files.
