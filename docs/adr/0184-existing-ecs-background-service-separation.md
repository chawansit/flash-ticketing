# ADR0184: Existing ECS background service separation

## Status
Accepted for offline preparation under ADR0172. Live deployment, recovery classification, profile registration and capacity qualification are pending. This experimental profile does not supersede the accepted historical placement profiles.

## Context
The ADR0181 control passed at 60 paid journeys/second. The 1+3 candidate failed customer and financial expectation gates. Restored simulator configuration targets the primary Docker API service; a reduction in primary APIs also reduces reachable callback pool capacity. Background processing competes with APIs for CPU on the primary ECS. The user requested separation onto the existing secondary ECS, with no new infrastructure spending.

## Decision
Prepare a separate, explicitly bound worker-placement experiment. Keep four APIs, Nginx, one PgBouncer and the existing Kafka broker on the primary in both arms. Control runs the existing background replicas on the primary; candidate runs exactly those replicas on the secondary. Preserve every per-role image, source identity, pool size, admission setting and workload. Route callbacks through the same four-API Nginx ingress in both arms. All workers in both arms use the same private PgBouncer and Kafka endpoints. DCS and verified-TLS RDS remain unchanged; the generator remains dedicated.

Add a private Kafka listener alongside the existing internal listener, retaining the broker image, controller configuration and exact existing data volume. Bind it only to the primary private address. No broker data migration, public listener, extra broker, extra pooler or replica is authorized. Pin actual broker/runtime configuration before generating executable models. Read-only worker metric ports bind to the worker host private address in deterministic role ranges; runtime inventory must observe every replica on its actual host.

The passing 2+2 result remains a historical reference, not the matched control. First qualify the common four-API ingress and private dependencies without paid load. A fresh four-API control must pass before the off-host candidate runs. A control failure stops progression. The measured factor is background placement, including its network cost; do not attribute a combined API redistribution and worker move to CPU isolation alone.

Pure preparation rejects mutable images, changed budgets, unknown roles, dependency shortcuts and unsupported worker mounts. Only verified owner-only read-only configuration mounts may be transferred with exact hashes; never copy arbitrary directories or persistent data. Both arms have the same logical DB connection total and one 24-connection PgBouncer server pool. Preparation is not a deployment or registered load profile.

The observer implementation uses an explicit ADR0184 worker-placement identity and a canonical prepared-pair digest. Collect all containers on each host, including stopped or unrelated resources, and reject unexpected resources or overlapping workers. Verify running state, image, environment, private bindings, command and Kafka volume identity before host-specific import/source proofs. Retain process-start identity for each metrics endpoint; missing replicas, restarts, counter resets and incomplete samples fail the observation. CPU specifications include the same inventory digest and exact per-host role counts. Extend shared CPU observation only under this explicit decision marker; preserve historical profile validation. These adapters do not authorize deployment or load and do not substitute for queue, customer or financial audits.

## Alternatives
Use 2+2 versus four-primary APIs plus remote workers: changes two placements at once. Add an ECS or resize: outside the zero-new-spending envelope. Move Kafka as well: adds persistent data ownership and recovery risk. Leave callback routing on primary Docker DNS: changes reachable payment capacity with API placement. Add a read replica: does not resolve callback capacity or worker CPU contention.

## Consequences
The primary may still be constrained by four APIs, PgBouncer or Kafka; improvement is unmeasured. Off-host workers depend on private network connectivity. Private Kafka remains plaintext on the existing trusted network; public access must remain blocked. Host placement adds latency and requires host-aware inventory, CPU and worker metrics collection. Image staging must use the shared ownership generator and immutable per-role artifacts. Historical profiles must continue rejecting unexpected secondary workers.

## Failure and recovery behavior
No new load while the previous paid failure remains RECOVERY_REQUIRED. Retain original failed gates and consumed reservations. Verify exact paid fixture ownership and per-entity payment/ticket relationships before recording any separately defined recovery decision; never infer ownership from titles or timestamps.

Before moving workers, stop dispatch, drain all reservation, Kafka, outbox, callback and confirmation queues, and stop source replicas before starting destination replicas. Inventory must prove the source replicas absent and destination counts exact. Cross-host dependencies and broker metadata must pass readiness checks before any customer dispatch. A Kafka restart is allowed only in a qualified owned lifecycle with drained queues and exact broker/volume restoration.

On failure or interruption, stop destination workers, perform mandatory post-TTL financial and full queue checks, then restore the exact original broker configuration, API/worker semantics and private files. Do not use Compose volume deletion. Ambiguous ownership, overlapping replicas, inaccessible queues or uncertain restoration block new load. New firewall rules or other human infrastructure actions are escalated with the concrete required configuration.

## Validation evidence
Pending at ADR creation. Local model tests and Compose validation will be recorded after execution. No cloud deployment, load, capacity gain or production qualification is claimed.

Executed local validation: 337 affected tests passed in 41.52 seconds; 20 evidence tests passed after the test-style correction. Ruff passed. Three synthetic Compose models validated without starting containers. Read-only live checks found restored runtime unchanged, secondary empty, generator idle and every queue zero. Exact prior primary test containers were absent; scoped paid recovery remains unresolved. See [preparation evidence](../capacity/flash-sale-opening/background-service-separation-preparation-2026-10-06.json). No deployment, load, recovery reclassification or capacity improvement.

Host-aware observer implementation passed 298 affected tests in 146.14 seconds (helper 146.688 seconds), followed by 81 affected metrics tests in 0.62 seconds after adding invalid-confirmation-gauge failure retention. Ruff passed. The bounded scan of 22 exact retained files (73,541,853 bytes) found no fixture ownership fields. Existing failed scopes remain unchanged; no deployment or load. See [observer validation](../capacity/flash-sale-opening/background-service-separation-observers-2026-10-06.json). Image staging, lifecycle integration, live qualification and registered load profile are still pending.
