# ADR0161: Isolated asynchronous confirmation qualification and comparison

- Status: Accepted; safety qualification passed; measured control failed, restored, candidate skipped and scope closed
- Date: 2026-10-05

## Context

The user authorized ADR0160 steps1-6, including local recovery tests and a controlled comparison. The previous measured control failed and its scope is closed. Workspace API/config also include an earlier unqualified callback-admission-reserve change; deploying the whole branch would confound the comparison. Existing fixed two-host runners pin source/images/budgets and stop after failed control but do not inventory or audit a new confirmation worker/receipt queue.

## Decision

Export a separate immutable source tree from the verified ADR0156 isolated candidate. Overlay only ADR0160 changed modules,receipt migration,tests and polling helper; exclude the prior callback-reserve API/config delta. Prove the financial apply_callback body AST equals the prior callback transaction body and preserve all other parent runtime modules. Both compared arms use identical new images/source and migrations,refresh-on/cache1000ms,dedup-off,polling guidance500ms and2+2API placement. Only API PAYMENT_CONFIRMATION_ASYNC changes0->1. The paid client honors identical bounded guidance/jitter in both arms; retain original errors and bounded generator concurrency.

Allocate a confirmation worker with2connections/concurrency2 in both arms,using simulator parent image and verified new code; shrink simulator pool12->10 in both arms,keeping its dispatch concurrency8/refill mode. Confirmation worker is enabled in both arms, but synchronous control creates no receipts. Keep PgBouncer server budget24 and all API,consumer,writer and other service ceilings unchanged. Explicitly verify the total pool/waiter ceilings before dispatch. Do not silently compare historical original control against this new common topology.

Extend the existing script-driven runner through a separate bound profile and additive contract hooks; no duplicate orchestration engine,global monkeypatching,automatic replacements or weakened gates. Inventory/source/CPU/metrics must include the confirmation worker. Pending receipts,PROCESSING/RETRY/REVIEW and capacity counters must be audited globally,including restoration and failures; a REVIEW receipt fails completion/drain. Source-safe default-off migration may remain after restoration, but all accepted receipts must drain before removing their worker. Missing queue or observer evidence fails closed. Existing financial expected-ticket,duplicate,TTL,global Redis/outbox/Kafka and customer-completion gates remain mandatory.

Authorized next cloud experiment is at most one fresh safety-only off/on pair(two simulated paid tickets) followed,only if qualified,by one matched60journeys/s300s off/on comparison(18000scheduled/arm plus one isolated safety ticket/arm). Maximum2measured stages and4simulated safety tickets across qualification/comparison. Stop after a failed control or mandatory gate;no higher-rate/hourly run,push or merge. Bind a new independent ledger to exact prepared source/config/harness hashes and preserve all previous failed scopes/counters. If qualification/build/preflight fails before cloud dispatch, do not infer a performance result.

## Alternatives

Deploy the whole workspace: imports unrelated admission behavior. Reuse old dedup ledger: confuses consumed scope and tested factor. Add an uncounted confirmation pool only to candidate: confounds budget. Publish directly to Kafka on callback: changes ADR0160 durable receipt choice. Bypass receipt queues in existing global audit: can declare drain while financial work remains; rejected.

## Consequences

The new common topology/polling changes require a fresh passing control. Native/image/source and harness qualification take time but prevent image drift and incomplete financial audits. Two confirmation connections may limit throughput; measure rather than assume that queueing increases sustained capacity. No300000tickets/hour or RPS improvement is established by preparation. Synthetic harness tests and mocked Kafka acknowledgements do not substitute for live durability/drain evidence.

## Failure and recovery

Owned source/containers/artifacts must be verified and cleaned up without broad deletion. Driver reservations persist before ambiguous dispatch. Original primary4API/runtime settings,secondary resources,credentials and generator idle state must restore. Retain ownership/lock if unprocessed receipts or ambiguous remote cleanup remain; restore worker capacity to finish accepted work before declaring completion. Migration is additive and default-off;never drop acknowledged receipts for rollback. Source drift,missing confirmation metrics,queue mismatch or budget growth blocks paid dispatch. No retry of a consumed failed experiment without a new explicit bounded scope.

## Persistence,locking,messaging,idempotency,TTL and scaling

ADR0160 receipt persistence/lease/capacity rules apply. Financial transaction body,booking uniqueness,outbox/Kafka delivery,payment idempotency and holdTTL remain unchanged. This extends ADR0151/0157 profile machinery solely for the new factor;does not supersede their historical evidence or accepted application decisions. Dedicated compute scaling remains future measured scope.

## Validation evidence

Recorded before profile/export implementation. Workspace focused checks executed76unit and84real PostgreSQL17.6/Redis tests before subsequent new corruption/precommit/invariant tests;broader native Linux and profile tests pending. No cloud mutation or new load yet. Append exact executed source/native/harness/image and live results before declaring qualification.

Restoration implementation refinement: first verify drain while confirmation capacity remains, restore all primary APIs to synchronous/default-off intake after removing secondary APIs, then verify drain again and remove the receipt worker. Preserve the worker and ownership on either ambiguous/failed check. This prevents removing receipt processing while asynchronous intake can still acknowledge callbacks.

Executed local evidence:739 native Linux unit/integration tests passed (prior738-pass/1-test read-only setup failure retained);2 native PostgreSQL audit tests passed;421 focused host harness tests passed;Ruff and diff checks passed. Reproducible213-file/21-module source and6 immutable offline images verified;financial transaction AST preserved and unrelated callback reserve excluded. [Local validation](../capacity/flash-sale-opening/async-payment-confirmation-local-validation-2026-10-05.json). Cloud results are recorded below; no asynchronous capacity improvement has been measured.

## Executed cloud checkpoint: 2026-10-05

Immutable image staging passed without service changes. The fresh off/on safety pair passed cross-host100-request exactly-one-seat-owner, replay, authorization, post-TTL payment/ticket durability and complete global drain checks. Candidate safety produced one completed durable receipt and one paid-issued ticket. Original deployment restored after both arms; additive migration009 retained with checksum recorded. [Safety evidence](../capacity/flash-sale-opening/async-payment-confirmation-safety-qualification-2026-10-05.json).

The subsequent synchronous control offered60journeys/s for300seconds:18000scheduled/dispatched,17996customer-confirmed paid-issued tickets,4payment-initiationHTTP503s (0.022222% of dispatched journeys),0generator drops and no customer retries. No duplicate booked seats or multi-booking orders were observed. The expected18000paid cohort failed, so customer-load, post-TTL-financial and aggregate zero-double-booking gates failed closed; no duplicate booking is implied. All tracked global queues drained, original primary4API/runtime and normal consumer restored, secondary test resources removed and generator idle. The runner skipped the asynchronous measured arm and closed the scope after the failed control. [Measured control evidence](../capacity/flash-sale-opening/async-payment-confirmation-measured-control-2026-10-05.json).

Five API database-admission failures (three PoolTimeout and two TooManyRequests) occurred in two adjacent one-second samples on one replica: four payment-initiation503s and one callback503 recovered by simulator redelivery. Coarse snapshots cannot prove occupancy at the instant of rejection. Exact trigger and asynchronous throughput benefit remain unproven;300000tickets/hour is unqualified. A replacement experiment requires a new explicitly bounded scope; unused allowance does not authorize automatic retry.
