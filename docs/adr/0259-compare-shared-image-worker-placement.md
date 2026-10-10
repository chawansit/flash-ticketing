# ADR0259: Compare shared-image worker placement

## Status

Accepted for a bounded test implementation, 10 October 2026. Runner integration is implemented; the fresh cloud control failed, and candidate migration remains blocked. This extends ADR0258's inactive deployment preparation and supersedes ADR0228's API-only placement restriction only for this comparison. Other profiles and historical evidence remain unchanged.

## Context

The user approved continuing the proposed control and worker migration comparison. The latest hourly recovery run numerically issued 302,295 tickets inside its hour, but incomplete diagnostics prevented qualification. Primary ECS background CPU was high; moving background processes is a hypothesis requiring a matched measurement. ADR0258 published and locally validated one shared application image.

Executed read-only preflight confirmed primary and generator SSH access, secondary access through the pinned private SSH hop, no owned CCE namespaces, an idle generator, and 6.13 GiB free on the primary. Kafka advertises `kafka:9092` inside Docker and exposes no host port. This address is not directly usable by CCE workers.

## Decision

Reuse the proven paid generator and financial audits. Run a fresh five-minute control at 84 offered journeys/s with four CCE API pods and the thirteen benchmark workers on ECS. Only after the control passes every current gate, repeat with those thirteen workers on CCE. Use the selected ADR0258 image digest for every application role in both arms. Replacing role images is common preparation, not the measured factor.

Keep APIs at four replicas, 1 vCPU/2 GiB each in both arms. The CCE candidate has six consumers, three reservation writers, and one publisher, maintenance worker, reconciler and simulator: thirteen worker pods at 250m CPU/512 MiB each. Confirmation remains disabled. Maximum candidate allocation is seventeen pods, 7.25 vCPU/14.5 GiB. Preserve physical PgBouncer server budget 24, API pool 4/payment pool 2, acquisition budget 20, simulator HTTP concurrency 12 and simulator database pool 10. Preserve complete per-role environment, gateway delays, recovery bundle, polling settings and ten-second API keep-alive from the accepted benchmark. Do not use the five-second development preview setting.

Scope is this two-arm comparison, one paid stage per arm, each experiment bounded to 3,600 seconds plus mandatory cleanup. The standing no-cumulative-cap policy and explicitly uncapped CCE comparison/scaling permission apply to this fixed allocation; this is not permission for indefinite deployments or an hourly/higher-rate test. Publish reviewed implementation and redacted evidence to a codex branch; main merge still requires approval.

For CCE broker connectivity, prepare an owned TCP forwarder bound only to the primary private address, forwarding port 9092 to the existing Docker broker. Resolve `kafka` to that private address in candidate worker pods. This retains the broker's advertised hostname, topic partitions, consumer group and delivery semantics without recreating Kafka. Verify metadata and consumer progress, not merely TCP connection success. The existing private PgBouncer bridge remains the database route. Record proxy resource/CPU cost separately. If the bridge cannot be qualified, stop before customer traffic.

## Alternatives

- Leave workers on ECS: valid control, but does not test the measured CPU placement hypothesis.
- Reconfigure/restart Kafka with another advertised listener: viable later; unnecessarily interrupts the broker for this bounded test.
- Move Kafka and PgBouncer at the same time: changes multiple factors and their durability/topology risks.
- Use the inactive preview verbatim: rejected because preview keep-alive, role settings and networking are not a sealed benchmark configuration.
- Start a new load harness: rejected; retain the proven generator, recovery and financial checks.

## Consequences

Worker placement and its necessary network route are the measured change. The result measures this concrete deployment, not a universal benefit from CCE. CCE process CPU must be observed independently of ECS CPU; metrics availability alone does not prove worker readiness. Client SQL pools do not represent additional physical RDS connections. Provider-admitted CPU/memory must match the requested comparison.

## Failure and recovery behavior

Pause dispatch before transferring worker ownership. Stop each old ECS role before its corresponding CCE replicas become active. Preserve graceful termination and replay/idempotency rules. Do not overlap old and new replica counts. Validate image, environment, admitted resources, dependencies and actual worker progress before paid traffic. Keep old container definitions and exact image/configuration identities for restoration.

On failed control, do not launch the candidate. On deployment, ownership or customer gate failure, stop progression, retain evidence and perform payment/post-TTL reconciliation and complete queue drain. Remove only owned pods, namespace, credentials and bridges, and restore captured ECS services and routing. Cleanup may exceed the experiment deadline to preserve financial correctness.

## Validation evidence

- ADR0258: twelve unit tests and twenty-seven integration tests executed for the shared image; these are packaging/correctness checks, not cloud capacity.
- Live readiness returned `ready`. CCE server-side dry runs admitted both 1-vCPU/2-GiB API and 250m/512-MiB worker resources unchanged, with no persisted pods. The selected SWR digest was pulled successfully again.
- Read-only live preflight: `docs/capacity/cce/shared-worker-placement-preflight-2026-10-10.json`.
- Executed: `pytest tests/unit/test_cce_shared_worker_comparison.py tests/unit/test_shared_application_image.py -q -p no:cacheprovider`: 35 passed. These validate the fixed image/budgets, environment drift rejection, private Kafka routing, inactive replicas and candidate rejection after a failed or unrestored control.
- Implemented: registered shared-image control/candidate profile, owned private Kafka forwarding, stop-before-start worker transfer, native worker identity/CPU/progress observation and ownership-safe restoration.
- Executed integration checks: 325 tests passed, one skipped; Ruff passed; archived-source reproduction verified 535 historical inputs and 18 declared overlays.
- Cloud control `adr0151-ba78fda091fd`: 20,040 dispatched and eventually fulfilled of 25,200 scheduled journeys; 5,160 drops; 594 recovered initial-error journeys; zero final failures. Journey p95 11.24 seconds. Control failed customer and slot-history completeness gates. All paid outcomes and relationships independently reconciled after TTL, with no duplicate booking, empty queues and verified restoration. Candidate was not started; no worker-placement capacity improvement is measured. See [control evidence](../capacity/cce/shared-worker-control-2026-10-10.json) and ADR0260.

### Runner integration boundaries

The measured API and background roles use the ADR0258 shared image in both arms. The existing runner may retain its frozen ECS API and audit-helper images during unmeasured bootstrap and restoration; these containers are stopped before paid dispatch and are not presented as measured shared-image roles. This preserves the qualified fixture, financial-audit and restoration implementation.

Per-role resolved environments are sealed before dispatch. Only the database transport address, Kafka DNS route and callback transport address are normalized when comparing placements; credentials, database options, feature flags and connection budgets must match. Actual CCE worker UID, admitted resources, startup source/environment proof, process identity and progress remain separate from the captured ECS inventory. Neither a pod readiness flag nor an old ECS inventory is evidence of native worker progress.

The historical benchmark also starts one confirmation poller on ECS even when API asynchronous confirmation is disabled. Retain that passive poller on ECS in both arms, with the shared image and its unchanged two-connection client pool; no confirmation pod is added to CCE. Include its CPU and source proof in ECS observations. The thirteen migrated workers exclude this unchanged poller. The maximum application client-pool allocation is therefore 148, still sharing 24 physical PgBouncer connections. Kafka forwarding is candidate-only transport overhead, separately recorded as a 0.25-vCPU/64-MiB owned helper.
