# ADR0322: Bound redundant ticket-notification reads

## Status

Implemented, published and measured in one five-minute cloud comparison. Quality gate failed; candidate remains default-off and is not capacity-qualified. Original runtime was independently restored and reconciled. This opt-in experiment supersedes ADR0297/ADR0298 TicketsIssued advisory-refresh fallback only while a bounded, successfully published fulfillment hint remains available. Their financial, authorization and customer-cache freshness guarantees remain unchanged.

## Context

ADR0321 completed 30,600 journeys with no customer errors or drops but missed the conservative 100-ticket/s gate by nineteen tickets. Fulfillment partition 1 accumulated 661 records with stable assignment and fewer incoming records than peers. The exact initiating cause remains unconfirmed.

Retained offered-window counters identify consumer-1 performing 12,145 BEGIN operations versus 10,113-10,271 on peers. It published 2,712 additional advisory fills beyond committed captures, versus 6-14 on peers; its fresh-notification dedup counter advanced only 1,937 versus 5,039-5,125. The current projector requires both an unexpired local hint and a matching fresh Redis entry to skip TicketsIssued reads. A delayed notification opens another database transaction when the customer cache expires, adding work to an already delayed consumer. Aggregate phase observations do not bind partition identity to a pod: this proves avoidable work, not the initial straggler cause.

## Decision

Add default-off ORDER_STATUS_NOTIFICATION_DEDUP, requiring committed capture and existing event-refresh deduplication. After a valid FULFILLED snapshot has committed and successfully published, retain only its order UUID and a fixed thirty-second suppression deadline in a process-local table capped at 1,024 entries. Within that deadline, TicketsIssued may skip its advisory database refresh even when the customer cache expires. Never serve this hint as customer data, republish from it, or extend Redis snapshot TTL. Redis misses retain the authorized database path.

Keep the durable TicketsIssued inbox transaction and notification processing. Failed commit, unavailable capture, failed publication, restart, eviction or expired suppression hint retains existing refresh fallback. A new OrderPaid capture or RefundRequested invalidates the hint. Repeated TicketsIssued events do not extend its deadline.

Compare one candidate against the exact ADR0321 image at unchanged offered load, resources, recovery and connection budgets. Preserve per-endpoint consumer phase/failure counters from the already fetched metric payload to associate a lagging partition with its pod. This adds no database requests or metric scrapes. Retain bounded exception classes before cleanup without request bodies, credentials or unbounded labels.

## Alternatives

Increase customer cache TTL: weakens freshness. Increase consumer CPU/connections: confounds attribution before saturation is confirmed. Pipeline fulfillment writes: does not remove the observed redundant reads. Permanently disable TicketsIssued repair: removes useful fallback after restart/publication failure. Retain whole snapshots longer: unnecessary memory/freshness risk. Deferred.

## Consequences

Reduces measured redundant consumer database work during delayed notifications. Hints are bounded by time and count. Expired customer entries remain expired and demand reads repair them; notification-driven repair is deferred only for successfully published orders within thirty seconds. This may increase API cache misses relative to refreshing every notification; measure that trade-off. Initial partition delay, burst throttling and network/database variation remain unconfirmed.

## Failure and recovery behavior

PostgreSQL remains authoritative. Preserve paid-order locks, actual ticket identities, uniqueness, atomic ticket/order/outbox/inbox commit and Kafka offsets advanced only after durable processing. Replay must not duplicate payments or tickets. Refunds invalidate suppression; no old snapshot becomes fresh. Rollback disables the feature/restores the parent image. Reconcile actual cohorts, drain queues and restore original runtime after every experiment.

## Validation evidence

Decision recorded before implementation. Test delayed notifications after Redis expiry, hint expiry/eviction, duplicates, restart, failed publication, commit failure, refund invalidation and customer authorization/freshness. Execute PostgreSQL/Redis replay and payment integrity checks. Verify immutable image modules and explicit role flags before cloud admission. Short comparison and hourly qualification remain pending.


## Executed local validation

The default-off correction and essential bounded exception counters were built on the exact ADR0321 image. Four modules change: projector, configuration, worker wiring and error metrics. The other eighteen modules and dependency inputs remain unchanged. All twenty-two copied/installed/imported modules and eight role defaults were verified.

Executed 48 PostgreSQL17.6/Redis integration checks passed, with one existing Kafka-dependent case skipped. Delayed notification tests preserve expired/missing/corrupt cache entries, verify authorized durable ticket retrieval and confirm replay/restart cannot create duplicate tickets. An initial UUID representation assertion was corrected; a local connection acquisition timeout passed on rerun without changing its gate. Initial failures remain retained privately.

Executed 124 application/observer unit checks passed. After correcting a constructor-order integration regression before cloud deployment, 74 existing runner admission checks passed. Seven new offline candidate checks passed, including consumer-only activation, rejection of unpublished images and resource/image/baseline drift. Counts overlap and must not be summed. Full candidate binding was exercised offline with image admission mocked only in the test; actual admission still rejects the unpublished receipt. No remote publication or deployment is claimed by that mock.

At the local-validation checkpoint, SWR rejected the supplied login and existing Docker authentication. The immutable candidate was locally prepared, and the original ECS runtime/generator were verified healthy and idle. No new namespace or paid stage had started at that checkpoint. Publication and the cloud comparison are recorded below. [Local evidence](../capacity/cce/ticket-notification-dedup-local-2026-10-14.json), [immutable image receipt](../capacity/cce/ticket-notification-dedup-image-2026-10-14.json).


## Cloud comparison and terminal recovery, 14 October 2026

Fresh registry authentication allowed immutable SWR publication, pull and remote verification of all twenty-two module hashes and eight role defaults. The published image is `sha256:c64ef9e7df3f4864f1019cbe496ce4526654b71abd906cfd93a30e363f4ff554`. The exact source/image/configuration receipt is retained; only native fulfillment consumers enabled the feature. A pre-dispatch attempt exposed two missing ADR0322 worker-transition registrations. That attempt restored without paid traffic; both registrations were corrected and an exact capture-path regression executed before the new paid attempt.

The paid comparison ran at unchanged102 offered journeys/s for300seconds, four API pods, fifteen background pods, four writers, shared24 physical database backends, admission20 and generator concurrency500. Bangkok offered window:14:22:37.798-14:27:37.798. It scheduled30,600 journeys, dispatched/fulfilled30,581 and dropped19 before dispatch. Twenty initial errors (thirteen payment503, seven order-status503) all recovered, with zero final customer failures. Numerical issuance reached30,291 unique in-window tickets and30,047 conservative inner-window tickets (100.16/s), but the zero-drop/customer gate failed. No hourly stage followed and no sustainable capacity gain is claimed.

Against ADR0321, worst-shard total-journey p95 improved from7,433.79ms to4,518.84ms (39.2%); ticket-wait p95 improved from6,330.01ms to2,753.83ms. Hold HTTP p95 worsened from19.06ms to103.74ms, durable reservation p95 from1,104.10ms to2,031.37ms, and payment HTTP p95 from161.53ms to261.67ms. API process CPU increased from1.689 to1.711cores; background process CPU from1.452 to1.503cores. These are process measurements, not CCE node CPU or throttling.

Fulfillment maximum partition lag fell from661 to77 and drained to zero. Per-consumer counters now connect partition1 to consumer-3; its offered-window event means were lower than peers, so this run does not reproduce the former partition1 straggler. All six consumers recorded one OperationalError within approximately2-3seconds of traffic opening, without a specific SQLSTATE. The exception message/cause was not retained. Twenty-one slot-failure captures show simultaneous payment-request/callback commit stalls and general BEGIN waits. Database sampling observed WALWrite contention, but WAL I/O timing was unavailable; neither the exact commit-stall cause nor storage saturation is established. Frontend/network/generator variation also remains unresolved.

Original fixed-expectation financial gates remain failed because they expected30,600 dispatched payments. Independent fresh audits against the retained30581-customer fixture reconciled30,581 orders, successful payments, unique tickets and callbacks, with zero pending work, duplicate seats, relationship errors or dead letters. Both independent safety payments reconciled. This closes financial recovery without changing the original load result or qualifying the19 missing journeys.

Automatic candidate restoration failed before restarting ECS services. The retained cleanup records identify error classes only, so its initiating ValueError is unresolved. Independent repair verified the exact owned saved snapshot/restore model, absence of native pods and generator activity, restored the original four-API runtime, retired only owned fixtures, removed run-added workers/helpers and private configuration snapshots, and verified stable runtime plus zero queues. The ledger is FAILED_RESTORED and the original result digest is preserved. Add bounded private cleanup trace retention so a future failure retains its initiating cause without public secrets or skipped cleanup.

Executed45 candidate-admission/recovery checks and47 lifecycle/cleanup checks passed. These overlap prior tests. A missing local test import path and an initial cleanup-capture indentation regression were corrected before the final executed passes; failed output was retained. These follow-up runner changes have not had a new cloud test.

Keep the correction experimental. Next investigate the shared commit/pool stalls and early OperationalErrors before another unchanged-load comparison. Do not increase load or start hourly qualification while the zero-drop gate fails. [Cloud result and independent recovery](../capacity/cce/ticket-notification-dedup-comparison-2026-10-14.json).
