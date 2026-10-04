# ADR 0141: Fixed-load cloud qualification of atomic seat projection

- Status: Control executed; cloud candidate rejected by failed gates; no capacity promotion
- Date: 2026-10-04

## Context

ADR0139/0140 passed native Linux69 focused/721 full tests and five18k worker-shape snapshot acknowledgements at unchanged100ms; original baseline timed out five times. Windows retains one513-seat replay timeout. Native candidate mean Python construction94.936ms and Redis45.991ms expose a CPU-placement tradeoff; local microbenchmarks do not establish cloud benefit. Both ECS hosts are now password-accessible with verified known host keys, clean deb330e checkouts, matching baseline source/images, restored normal API budgets/readiness and no active generator.

## Decision

Run one separate cache-factor control against passing ADR0133:60 buyers/s300s,60 shows300seats,18000 distinct buyers/tickets, two generator processes/eight HTTP clients each,500 total in-flight journeys and1s polling. Keep four APIs/pool4/shared12/financial2, six consumers/pool8, three writers/batch4,24 PgBouncer server slots, cache0 and refill simulator8. Change only cache.py from source-verified candidate ddea4c2 (runtime a97d362). Backend helper already verifies cache hashes in APIs/workers before dispatch; additionally verify all11 runtime module hashes after restoration. No single-concert concentration factor is included.

Reuse existing ADR0040 paid smoke lifecycle, ADR0090 bounded generator controls and established read-only observers/audits/CPU collection. Preserve all20 ADR0133 gates and add deployed projection identity and exact fixed workload. Each missing/failed gate is a failure; one completed control cannot authorize another. This composes existing paid tooling for this control; it does not implement general unattended paid-stage progression.

## Persistence, locking, messaging, idempotency, TTL and scaling

Financial transactions, PostgreSQL authority/locks, atomic holds, writer fairness, payment/callback/inbox/outbox idempotency, Kafka delivery, TTLs and connection budgets remain unchanged. Source/encoding choice is governed by ADR0139. No infrastructure scaling or database parameter change. Restore normal service budgets after the control. Retain candidate cloud source/images only if all required gates and access cleanup pass; otherwise restore verified deb330e source/images. Keep local qualified candidate and tests even if cloud gates fail.

## Alternatives and consequences

Higher RPS now conflates code and overload. A one-show fixture changes concentration and is deferred until this factor passes. Skipping postTTL or global queue/Kafka audits, loosening deadlines or adding customer retries hides failures. Python encoding may reduce Redis blocking while adding worker CPU; report observed client/latency/CPU/queue differences and sampled-window limits. This five-minute distributed development-payment control does not prove300000paid tickets/hour or hot-concert capacity. Linux local qualification does not waive the retained Windows failure.

## Failure and recovery

Use only the previously authorized temporary root key, freshly generated with restrict and45minute expiry on verified OpenSSH7.7+, then remove exactly its entry from both hosts and local key files after restoration/audits. Password remains memory-only. Known host verification stays strict. A partial key/bootstrap/source-transfer failure requires cleanup before any load. Failures never overwrite local runtime/tests or discard prior evidence. Source bundle transfer uses exact hash/revision and cleanup. Retain post-failure financial/queue evidence even when customer latency fails; restore service settings/readiness/generator idle/private scratch, then remove access. No further run or main merge follows from a failure.

## Validation evidence

Recorded before key installation, source sync or load. Read-only access/baseline preflight passed without changes. [Exact profile, source hashes,22gates and scope](../capacity/flash-sale-opening/atomic-projection-cloud-control-plan-2026-10-04.json). Require local lifecycle/source/CLI guards before installation, then record actual cloud result, exact financial counts/postTTL/zero-double-booking/full-keyspace queues/Kafka, CPU window and source/restoration/temporary-access cleanup. Missing evidence stays a failed gate.

Offline preparation executed before key installation: syntax checked16 private lifecycle helpers; verified11 exact Git/local runtime hashes, cache.py as the only runtime difference, financial-method AST equality and fixed CLI budgets. Seven synthetic report guard cases passed: valid fixture, duplicate seat, missing ticket, undrained queue, wrong source revision, concentration change and acquisition-budget overflow. Synthetic cases are report validation only, not backend/cloud capacity evidence. Native local qualification remains69 focused/721 full passed; the separate Windows513-seat replay timeout remains a recorded failure. Actual cloud run and cleanup evidence will be appended after completion.

## Executed cloud result and recovery

Run checkout-20261004T145727Z-abde20 tested ddea4c2 at the exact60buyers/s300s distributed profile.17of22 inherited/additional gates passed; customer, exact-ticket financial, queue drain, Kafka drain and combined hold/cohort gates failed.18000scheduled/17880dispatched/120drops;1057customer-confirmed tickets by deadline,16823failed journeys (94.09% of dispatched), zero customer retries. This journey percentage is not an HTTP error percentage.

Worst-shard p95: hold148.100ms versus16.632ms baseline; payment367.693ms versus68.409ms; payment-to-ticket12985.120ms versus1113.159ms. Comparable sampled offered-window host CPU99.397% versus81.358%, aggregate API1.969cores versus1.295. API pool acquisition mean81.083ms versus2.881ms, event-loop lag mean25.160ms; offered-window callback backlog7644. Post-TTL due-to-callback p95143044.670ms. These observations establish saturation/backlog and late completion; they do not prove the initiating cause is encoding, WAL or storage.

Fresh post-TTL audit recorded zero duplicate booked seats/multi-booking orders,17880orders,16309successful simulator payments,7182tickets and pending_refresh2; last control-observer Kafka lag9. The inherited hold gate also requires cohort equality: hold deadlines did elapse, but equality failed.

Both cloud checkouts and all service images were restored to verified deb330e; normal service counts/pools/payment flags/cache/readiness, generator idle and private scratch cleanup passed. Exactly one owned key entry was removed from each ECS and local key files deleted. No second/higher-load run, push or main merge. Local qualified source/tests remain intact.

A separate read-only post-restoration snapshot at15:12:05UTC accounted for all16309successful simulated payments as7182fulfilled+9127refunded, with0refund_pending. All full-keyspace queues and Kafka lag were0 then. These durable simulated refunds prevent treating the ticket mismatch as unexplained loss, but do not pass the original successful-ticket gate. Real payment gateway refund dispatch is future scope. Preserve both failed-gate and later recovery evidence.

[Cloud result, comparison and recovery](../capacity/flash-sale-opening/atomic-projection-cloud-control-2026-10-04.json). Before another capacity experiment, distinguish candidate regression from changed environmental conditions with baseline reproduction and targeted API/callback profiling; keep source, workload and connection budgets controlled. Further cloud load requires a new explicit resume.
