# ADR0322: Bound redundant ticket-notification reads

## Status

Implemented and locally verified; registry authentication blocks publication and cloud comparison. No new cloud result or capacity claim. This opt-in experiment supersedes ADR0297/ADR0298 TicketsIssued advisory-refresh fallback only while a bounded, successfully published fulfillment hint remains available. Their financial, authorization and customer-cache freshness guarantees remain unchanged.

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

SWR rejected the supplied login and existing Docker authentication. The immutable candidate remains locally prepared, and the original ECS runtime/generator were verified healthy and idle. No new namespace, paid stage or hourly qualification started. Renew registry credentials, complete actual publication/source verification, then run the unchanged short comparison. [Local evidence](../capacity/cce/ticket-notification-dedup-local-2026-10-14.json), [prepared image checkpoint](../capacity/cce/ticket-notification-dedup-image-2026-10-14.json).
