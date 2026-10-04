# ADR 0140: Native Linux local projection qualification

- Status: Accepted for bounded local diagnosis and qualification only
- Date: 2026-10-04

## Context

ADR0139's final local candidate received five of five 18k full-worker-shape acknowledgements on Windows against Docker Redis. Its focused run passed68/failed1: both large snapshot/financial replay gates passed, but a513-seat replay later timed out. A Windows socket failure does not prove slow Lua execution. Preserve this failed run. ADR0137 previously separated native Linux script placement from Windows filesystem overhead for the generator; it did not qualify this Redis candidate.

## Decision

Run the exact candidate source/tests on a copied native Linux filesystem using an existing locally resolved immutable test image, owned PostgreSQL17.6/Redis7.4.5 containers and a private owned Docker network. No Windows bind mounts, cloud endpoints, new dependencies or image build. Verify source hashes and installed locked application/test dependency versions before execution. Preserve Windows failures, record both environments and do not infer transport causality from a Linux pass alone. A native pass qualifies only the specified local Linux environment.

Use one bounded local command for source/dependency preflight, focused tests, full unit/integration tests only on focused pass, same-Redis original/candidate timing, evidence retrieval and finally-owned cleanup. Reuse ADR0040's bounded orchestration and ADR0137's native-filesystem identity rule. No load generator or cloud stage runs. Benchmark five alternating samples per300/18000 inventory on the same Redis, preserving the production100ms adapter deadline and actual worker seat fields. Server timing excludes Python construction; acknowledgement and whole client wall time remain separate.

## Alternatives and consequences

Repeated Windows tests until green hide retained failures and do not isolate transport. Changing socket timeouts or adding retries alters the gate. A cloud diagnostic is outside current authorization. A native local environment better represents Linux execution but not ECS capacity or end-to-end cloud latency. Container resource contention and dependency/platform differences must be reported.

## Persistence, locking, messaging, idempotency, TTL and scaling

No production architecture, schema, persistence authority, atomic holds, idempotency, messaging/Kafka, TTL, budgets, scaling or endpoint changes. Test databases and keys are isolated and synthetic; tests retain original deadlines and financial/concurrency assertions. This extends only the local qualification environment specification and supersedes no production decision.

## Failure and recovery

Source/dependency mismatch stops before tests. Focused failure prevents the full suite. Bound subprocesses and always remove only owned containers/network and native private copies. Retain raw logs and compact source/dependency/timing/cleanup evidence. Native test failures and failed benchmark acknowledgements remain failures; no automatic retry. Do not waive Windows failures, post-TTL durability, zero-double-booking or full financial/queue/Kafka gates needed for any future cloud control.

## Validation evidence

Recorded before execution. Required evidence: source identity, pinned image/dependencies, focused/full results, same-host benchmark acknowledgements/CPU tradeoff, owned cleanup. Cloud access/load/push/merge remain outside this experiment. ADR0139 and its failed attempts remain independently reported.

### Executed result

Source and every requirements.lock dependency verified. Native focused69passed/11.32s; full unit/integration721passed/89.64s, zero failures/skips and two dependency deprecation warnings each. Same-Redis candidate acknowledged all10samples (five each300/18000); original18k0/5 remained errors. Original/candidate18k server means155.398/45.991ms; candidate Python construction94.936ms/client total145.88ms. Owned PostgreSQL/Redis/runner containers, private network and copied native context removed. Retained Windows gate remains failed; no cloud access/load/push/merge. [Evidence](../capacity/flash-sale-opening/framed-seat-projection-local-validation-2026-10-04.json).
