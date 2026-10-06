# ADR0190: Historical recovery exception and test isolation

## Status
Proposed; requires explicit human approval of the unresolved historical recovery exception. Not implemented or cloud-qualified. No clearance, new deployment or load is authorized by this document.

## Context
The paid comparison adr0151-647e2505c995 preserved a failed candidate result and restoration records, but lost the exact paid fixture show identifiers when disposable containers and private manifests were removed. A fresh read-only inspection checked 16 exact host/container artifact locations and found none available. Primary runtime identities remained unchanged, the generator was idle and every checked global queue was zero. These facts cannot prove per-entity financial reconciliation for the historical run. The user confirmed that no snapshot or fixture backup is available.

ADR0172 requires escalation of unresolved ownership and blocks further cloud mutation/load. ADR0182 only handles its narrowly verified pre-dispatch abort; it must not classify this paid failure. ADR0185 prevents future loss of fixture receipts but cannot reconstruct the old receipt. Inferring ownership from show titles, timestamps or matching counts is rejected.

## Proposed decision
Request explicit acceptance that the historical paid run remains failed and unreconciled, while preparing a separate, verifiably isolated test environment on existing resources. Preserve all historical business records, evidence, scope counters and the original RECOVERY_REQUIRED ledger status. Do not delete, reset, relabel or claim successful reconciliation of the old run.

Before any new experiment, establish all of the following:

- A dedicated PostgreSQL test database on the existing RDS instance, using separately scoped credentials and audited connection targets. Preserve the original database. Bind PgBouncer and every API/background process to the new target; keep the total connection budget and machine sizes unchanged.
- A dedicated Redis logical database only if the existing DCS instance supports it and all clients, registry discovery and audits are verified to use it. If logical database isolation is unavailable, stop and design a fully qualified namespace alternative before implementation; event UUIDs alone do not isolate the global reservation registry.
- Dedicated Kafka topics and consumer groups on the existing broker. Publication, consumption and lag audits must share the exact binding. Current topic and group names are hardcoded, so configuration and rejection tests are required before adoption.
- An environment identity receipt covering database, Redis database/namespace, Kafka topics/groups, image sources and every participating process. Prove no process or callback route can consume or write across the old/new boundaries. Replace rather than overlap workloads on the existing machines; verify owned restoration.
- Existing exclusive pre-dispatch fixture retention with a canonical hash, durable local receipt and a separately retained non-secret copy before any customer dispatch. Include run-to-show ownership and expected financial outcomes in the audit binding.
- A narrowly scoped, append-only human-approved quarantine exception, recognized by the runner only for the isolated environment. It must not unlock the original target, reopen old scopes, ignore other recovery failures or classify the old result as passed. Register and locally qualify the new environment/profile before any load.

A new baseline and candidate must use the same isolated environment, dataset size/history, machines, budgets, workload and duration. Previous throughput results remain historical references, not an automatically comparable baseline. All future customer, authorization, payment durability, zero-double-booking, post-TTL, queue-drain and restoration gates remain unchanged.

## Alternatives
Recover exact identities from an original fixture/manifest or trusted pre-removal snapshot: preferred if such evidence becomes available, but the user reports no backup. Treat equal aggregate counts or empty queues as full recovery: rejected. Reset the old database or delete shows selected by title/time: rejected. Continue indefinitely with only local tests: safe, but cannot measure cloud capacity. Provision new infrastructure: outside the zero-new-spend boundary and unnecessary for this proposal.

## Consequences
Historical uncertainty remains visible and is explicitly accepted rather than repaired. Test isolation requires separately verified configuration and runner changes and a fresh baseline; it is not already implemented. New logical namespaces consume capacity on existing services, with no new machines, paid services or resizing authorized. Existing service charges continue. This exception does not establish production readiness or 300,000 tickets/hour.

If approval is withheld, no new cloud mutations or load proceed. Local application work remains allowed under ADR0172.

## Failure and recovery behavior
Missing approval, unverified namespace support, incomplete client binding, overlapping workers, cross-environment events, stale evidence or ambiguous restoration keeps forward progression blocked. Do not fall back silently to the original database, Redis namespace or Kafka topic. Preserve failed evidence and exact owned resources; stop increasing load and perform mandatory scoped financial and queue verification. Cleanup must never delete the historical database or touch records whose ownership is uncertain.

Once approved and implemented, retain original restoration snapshots before switching any runtime. Restoring old service configuration does not clear historical financial uncertainty. Every new fixture remains independently auditable after containers are removed.

## Validation evidence
Read-only cloud inspection executed in 38.703 seconds: 16 exact artifact locations checked, zero artifacts available; primary runtime unchanged; generator idle; checked global queues all zero. See [inspection evidence](../capacity/flash-sale-opening/paid-fixture-recovery-inspection-2026-10-06.json).

Implementation, namespace isolation tests, cloud deployment, recovery exception registration, load comparison and hourly qualification have not been executed. Working-tree naming checks passed for 1,531 documents with zero errors; git diff whitespace checks passed. No executable code was changed, so no application tests were run for this documentation checkpoint. The immutable historical ledger and failed gates remain unchanged.
