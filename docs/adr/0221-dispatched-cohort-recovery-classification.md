# ADR0221: Dispatched cohort recovery classification

Status: Accepted for the exact ADR0219 failed scope

## Context

ADR0219 scheduled 25,200 journeys, dispatched 24,331 and dropped 869 before dispatch. All dispatched journeys reported a unique paid and issued ticket, but planned-volume and diagnostic gates failed. Runtime restoration succeeded. The envelope conservatively blocks new experiments until independent recovery verification. Existing safety-only recovery decisions do not cover paid traffic.

## Decision

For this exact consumed ADR0219 scope, append a hash-bound recovery receipt after independently auditing all 84 owned shows against the 24,331 dispatched orders and paid tickets. Require post-TTL relationship integrity, zero global double-booking, full queue and Kafka drain, original runtime semantics, generator idle, owned cleanup, and no new dispatch. Bind retained artifacts and a fresh read-only verification. Preserve the original failed journal entry, original planned-volume expectations and all failed gates. A receipt permits a fresh experiment identity; it does not pass the failed experiment or establish production capacity.

## Alternatives

- Rewrite expected volume to dispatched volume: rejected because it would conceal 869 drops.
- Ignore recovery: rejected because payment and restoration uncertainty must block load.
- Repeat the consumed experiment: rejected because it would blur ownership and evidence.

## Consequences

Recovery and performance qualification become separate outcomes. The exception applies only to the exact known run; other paid failures remain blocked. No application persistence, locking, payment or messaging semantics change.

## Failure and recovery behavior

Missing artifacts, changed hashes, mismatched counts, stale fresh verification or any correctness/drain/restoration failure prevent receipt creation. Preserve failed evidence and account for read-only verification time. No load, deployment, cleanup mutation or automatic replay is authorized by this classifier.

## Validation evidence

Independent live read-only verification passed: 24,331 unique issued tickets, no expired unpaid orders, no relationship errors or global duplicates, post-TTL complete, full queues/Kafka drained, original runtime verified and unchanged. Verification took 120.578 seconds. The original failed journal entry is unchanged; a separately hash-bound receipt permits a fresh experiment. See [recovery receipt](../capacity/flash-sale-opening/shared-callback-rate-probe-recovery-2026-10-08.json). Local tests cover stale/future evidence, mismatched tickets, relationships, TTL, queues, restoration, binding, new dispatch, original failure preservation and post-publication tampering. The classifier is included in future harness source identities. Recovery time is tracked separately and included in total elapsed accounting. ADR0219 remains failed.

Final recovery/envelope/profile checks passed 78 tests in 83.42 seconds. The failed experiment remains consumed and cannot be reopened.

## Exact ADR0223 timing repeat extension

The ADR0222 timing repeat under ADR0223 (`adr0151-cac068bf517e`, scope `bounded_interleaved_refresh_probe__380556ea4230`, arm `adr0151-arm-1189895a697f`) scheduled 25,200 journeys, dispatched and fulfilled 25,168, and dropped 32 before dispatch. Financial counts match dispatched volume and show zero duplicate bookings, but the original planned-volume and diagnostic gates remain failed. Restoration and full drain completed. This exact case may use the same independent read-only recovery pattern.

Before extending the classifier, record two immutable allowlisted cases with exact run, scope, arm, decision, profile and expected dispatched/drop counts. Preserve the original case, receipt and failed journal entries. Unknown cases remain rejected; no inference from an arbitrary lower count is allowed. Apply the same financial relationships, post-TTL, global duplicates, queues, source/runtime and cleanup proof requirements. Collect a bounded read-only EXPLAIN ANALYZE of the existing paid-cohort query and catalog index definitions during this verification, with sanitized plan output and a five-second statement limit. Plan collection failure does not weaken mandatory recovery checks. No paid traffic, database DDL, deployment or new connection budget is authorized by the recovery extension. Record verification time and append a separate hash-bound receipt.

The immutable two-case extension passed 76 recovery/envelope tests in 6.50 seconds and lint. Independent live verification of all 25,168 tickets and relationships passed in 119.593 seconds, with no new paid traffic; an interrupted read-only attempt is separately accounted at 115.079 seconds. Both original failed results and journal entries remain unchanged. See [timing-repeat recovery receipt](../capacity/flash-sale-opening/interleaved-refresh-timing-recovery-2026-10-08.json). The hourly test remains blocked by failed qualification.


ADR0224 exact-case extension: allow independent recovery verification for consumed ledger bounded_orders_event_index_probe__c9a9ead76625, paid run adr0151-bb1154096e30, arm adr0151-arm-a977076bad22. Exactly 24,423 dispatched journeys paid and received unique tickets; 777 were never dispatched. Preserve the failed 25,200 expectation and all failed gates. Require the same financial relationships, expired hold deadlines, global duplicate/queue checks and original runtime verification. Also preserve and independently verify the intentional valid orders event index. Unknown scopes remain rejected; no new load or replay is authorized by recovery.

ADR0224 extension validation: 85 focused recovery/envelope tests passed in 6.32 s; final affected regression suite also passed. Live independent read-only verification took 125.563 s and reconciled all 24,423 paid-and-issued tickets, relationships, elapsed TTL, empty global queues/Kafka, restored runtime and exact persistent index. [Append-only recovery receipt](../capacity/flash-sale-opening/orders-event-index-recovery-2026-10-08.json). Failed throughput/count gates remain failed; no old scope reopened.

Publication preserves each new recovery receipt as exact bytes with explicit Git -text attributes. Verification hashes are computed from the created artifact bytes, so platform line-ending conversion must not invalidate a published receipt. Existing failed reports and journal entries are not rewritten.

## ADR0225 exact dispatched-cohort extension

Allow only consumed scope bounded_writer_write_pipeline_probe__eb7ff590f16c, paid report adr0151-4988723f04a7 and arm adr0151-arm-3000e8c3419a. Its unchanged 84/s, 300-second test scheduled 25,200 journeys, dispatched and fulfilled 25,196, and dropped four without retry or dispatched customer failure. The exact-count gates remain failed. Require the original immutable binding, all retained count/source/ownership proofs, a fresh read-only financial and relationship audit for precisely 25,196 paid tickets, global zero duplicates, complete queues/Kafka drain, generator idle, original runtime unchanged, and the identical valid orders index. No arbitrary count substitution, old-scope replay, latency relaxation or hourly qualification is authorized.

ADR0225 extension validation: the pre-audit recovery/envelope suite passed 91 tests in 8.44 s; direct writer-case positive and negative regressions subsequently passed as part of 24 recovery tests in 0.93 s. The independent read-only audit reconciled all 25,196 dispatched paid tickets after TTL, unchanged valid index, zero duplicates, complete queue drain and exact restoration in 125.891 s. [Recovery receipt](../capacity/flash-sale-opening/writer-write-pipeline-recovery-2026-10-08.json). The four undispatched journeys remain failed target evidence.
