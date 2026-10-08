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
