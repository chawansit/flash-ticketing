# ADR0186: Journaled worker handover qualification

## Status
Accepted for local implementation and fault-injection qualification under ADR0172 and ADR0184. No live adapter, registered worker-split profile, cloud mutation or load is authorized by this component. Existing recovery classifications and historical profiles are unchanged; no accepted decision is superseded.

## Context
Moving fourteen workers to the existing secondary ECS requires a separate lifecycle. The historical deployment helper assumes API-only secondary placement and excludes Kafka from its changed-service snapshot. A lost SSH response does not prove that a worker start or broker restart failed. Repeating an ambiguous action could overlap workers or discard restoration ownership. Inherited image environment defaults also need explicit binding to prevent code/configuration drift.

## Decision
Bind image metadata to immutable per-role images before preparing the pair. Resolve inherited environment, command and entrypoint values explicitly. Snapshot the entire original running topology and exact Kafka named-volume identity from runtime observations, rejecting stopped, unrelated or unsupported resources. Preserve original per-role settings, health checks, restart policies and mounts for restoration; compare observed runtime semantics rather than mutable tags.

Qualify an ordered handover engine locally using an explicitly offline adapter. The engine has no SSH implementation, load dispatch action or command-line execution mode. Before every action, write an exclusive, flushed journal intent; after validating its receipt, write an acknowledgement. Receipts carry only the exact action and input digest plus success, never arbitrary error text or credentials. Any failure, interruption or invalid acknowledgement stops forward progress and is not replayed. Failed journal persistence also prevents the next forward action.

Stop dispatch and drain queues before stopping original workers. Prove all workers absent on both hosts before changing the broker or starting the selected arm. Verify dependency readiness and host-aware inventory before the engine reports deployment preparation complete. Restoration always attempts stopping dispatch and mandatory post-TTL payment/ticket, double-booking and complete queue audits independently. Stop arm workers and prove absence before restoring the saved broker and runtime. An unknown absence result blocks broker/runtime restoration. Cleanup remains allowed after the normal action deadline or human pause, but requires exact owned resources. Failed audits remain failed even when runtime restoration succeeds.

## Alternatives
Reuse the API-only secondary driver: its inventory, image staging and restoration assumptions are incompatible. Retry uncertain deployment actions: risks overlapping workers. Save only Compose file contents: omits actual runtime overrides and Kafka volume ownership. Register a profile before fault qualification and recovery verification: would permit premature experiments.

## Consequences
The local engine can expose ordering, crash ambiguity and rollback mistakes before cloud deployment. It does not establish live connectivity, broker readiness, successful payment throughput or production capacity. Live image transfer, protected-file sealing, guarded adapter integration and complete preflight/profile qualification remain separate required work. The local journal contains ownership digests and exception types only. Source snapshots and image environments are private in-memory inputs and must not be published.

## Failure and recovery behavior
A pending intent with no acknowledgement means unknown outcome, not permission to retry. Preserve its journal and block reuse of that output directory. Continue mandatory owned cleanup where identity permits it. If stop/absence checks or restoration cannot be verified, report RECOVERY_REQUIRED. Do not delete Kafka volumes, infer historical fixture ownership, reopen consumed scopes or weaken any customer/correctness gate.

## Validation evidence
Pending execution. Local simulated adapters and synthetic runtime/image records will exercise success, interruption, invalid receipts, persistence failure, ownership drift and recovery ordering. No cloud action is part of this qualification.

Executed local validation: 221 affected tests passed in 11.87 seconds, including legacy observer compatibility. Four synthetic Compose models validated in 0.687 seconds without starting containers. Ruff and repository naming checks passed. Initial fault injection exposed an omitted stop-worker acknowledgement gate; it was corrected and retested. See [lifecycle evidence](../capacity/flash-sale-opening/background-service-separation-lifecycle-2026-10-06.json). Live adapter, image transfer, profile registration and cloud qualification remain pending; historical recovery is unchanged.
