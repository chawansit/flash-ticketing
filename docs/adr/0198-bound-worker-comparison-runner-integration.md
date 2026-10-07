# ADR0198: Bound worker comparison runner integration

## Status
Accepted for implementation under ADR0172 and ADR0184–ADR0197. The worker profile remains unregistered until complete local lifecycle qualification. Observer, diagnostic setup and paid-stage adapter components are locally qualified. Complete restoration/artifact cleanup assembly and full lifecycle qualification remain pending. No production topology decision or customer gate is superseded.

## Context
The historical paid runner assumes a two-host API placement. Worker separation retains all four APIs on the primary machine and moves only the same background workers. Shared source assumptions and historical inventory markers cannot certify this topology.

## Decision
Integrate the existing guarded configuration, runtime, readiness and concrete financial/queue audits with a worker-specific paid-stage adapter. Preserve the frozen customer generator, workload, strict customer gates and matched offered windows. Use exact per-role source expectations for all API and worker replicas. Keep historical runners and frozen financial/metric parsers unchanged; load isolated observer namespaces and adapt only qualified endpoint/inventory routing.

Bind all observer sources, inventory, retained fixture, original runtime, scope and stage expectations before dispatch. Observer artifacts use a separately generated, scope-bound staging owner and never enter the sealed Compose configuration directory, so retained diagnostic files cannot invalidate restoration. Require source/process identity and full metric coverage for four APIs and fourteen workers, including confirmation, plus both host CPU windows and the existing full-visibility read-only database diagnostics. No missing or reset counters become successful samples. Preserve the existing diagnostic account and verified TLS trust; no new grants or infrastructure.

The assembled lifecycle remains single-use: stop dispatch, run independent post-TTL/payment/booking/queue audits, stop only owned workers, restore the exact original runtime, independently verify restored queues and generator idleness, then remove only owned private artifacts after every mandatory gate passes. Failed control prevents candidate dispatch. Ambiguous stage launch, unknown ownership or failed restoration blocks further load. Registration and live preflight remain separate gates; integration does not authorize bypassing the standing runner.

## Alternatives
Reusing the old two-host API runner unchanged certifies the wrong topology. Changing imported historical globals couples comparisons and permits harness drift. New diagnostic connections or weakened startup/error gates would change the comparison. A separate unguarded cloud command would bypass the work envelope.

## Consequences
Explicit adapters and failure tests add local work but preserve comparison meaning. Observer cost must remain identical across arms and be measured with customer outcomes. The worker comparison does not by itself qualify hourly ticket throughput.

## Failure and recovery behavior
Reject changed role sources, inventory, scope, trust, budgets, fixture or metric identities before dispatch. Failed observers remain failed. Preserve diagnostic and fixture evidence on audit/journal failures. Cleanup is still allowed after pause or deadline expiry using the original ownership binding; no ambiguous launch is replayed.

## Validation evidence
The affected suite passed 577 tests with zero failures or skips in 121.99 seconds. It includes the existing 23 PostgreSQL financial cases and a real isolated Linux observer import/protected-file check. After a test-only cached-image restriction, the native case passed again in 2.72 seconds. Temporary local test containers were removed. After the common-source compatibility cleanup and sealed-configuration namespace correction, the final observer/diagnostic subset passed 124 tests with zero failures or skips in 20.13 seconds, including the native Linux check. Namespace collision and owner changes are covered before transfer. Ruff and working-tree naming checks passed.

The diagnostic transport and runtime fault tests use synthetic observations. Live RDS TLS, diagnostic credentials and dependency readiness were not exercised. The worker profile remains unregistered, and complete paid-stage/lifecycle qualification is pending. No cloud readiness, deployment, load or capacity improvement is claimed.

See [the compact integration checkpoint](../capacity/flash-sale-opening/background-service-separation-runner-integration-2026-10-07.json).

The paid-stage adapter now preserves the frozen 60 journeys/s, 300-second workload, retains and binds the exact fixture before token creation, verifies transferred sources and the private token manifest, and consumes the paid allowance before a potentially ambiguous launch. The launcher uses the existing command template, records exact process identity, flushes its ownership receipt, and independently stops owned jobs after pause or interruption. Stage completion cannot certify restoration or authorize the candidate; candidate dispatch requires a restored passing control. The primary host verifies already-staged observer helpers instead of uploading them twice.

The affected suite passed 618 tests with zero failures or skips in 175.70 seconds. After final adapter-source fingerprint and strict boolean stop-evidence corrections, 43 focused tests passed with zero failures or skips in 44.92 seconds, including an isolated Linux launcher/process-group cleanup case. Stage ordering and fault scenarios use synthetic transports and observer outcomes; they do not prove live database trust, customer throughput or production restoration. The worker profile remains unregistered. See [the paid-stage integration checkpoint](../capacity/flash-sale-opening/background-service-separation-paid-stage-integration-2026-10-07.json).
