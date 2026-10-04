# ADR 0137: Native-filesystem local Linux paid-generator qualification

- Status: Accepted for bounded local diagnostic and qualification; not cloud capacity
- Date: 2026-10-04

## Context

ADR0135's single-concert and unchanged distributed Linux synthetic controls both eventually fulfilled3,600 journeys uniquely with zero errors/drops/retries, but failed arrival and completion gates. They run scripts on the Windows repository bind mount. Short established cProfile diagnostics show89,025 posix.stat calls consuming11.203s in one300-journey child, with14.270s cumulative in httpcore.current_async_library. The installed implementation repeatedly attempts an optional sniffio import; import resolution probes script directories on the bind mount. Profiling perturbs timings, so it diagnoses a candidate cause and cannot qualify throughput.

ADR0136's bulk Redis publication candidate failed both18,000-seat gates and was rejected/restored. The100ms warmup failure is independent and remains mandatory.

## Decision

For one local synthetic control, copy unchanged Python tooling into an owned temporary directory on the Linux container's native filesystem before launching the same two child processes. Run the helper and children from that directory, without Windows-mounted directories on their import path. Verify normalized SHA256 identity against the retained ADR0135 candidate for every generator/journey/responder dependency and retain the mapping in the result.

Use the same image/dependencies,4CPU quota, network-none/loopback responder,60/s60s, concurrency500, two processes,8HTTP clients and250connections per child, poll1s, callback1, responder30ms/command0.5s/ticket5s, and90s completion deadline. Preserve100ms p95/500ms max dispatch-lag and every responder, unique-seat, retry/drop, connection-close and private-manifest gate. No dependency installation, missing-import monkeypatch, added client budget or deadline relaxation.

The factor under investigation is local script-filesystem placement. Retain the bind-mounted failures; a native pass cannot retroactively qualify them. Reuse ADR0090 responder/diagnostic and ADR0040 bounded orchestration/cleanup. This supplements their local environment specification and supersedes no production decision.

## Persistence, locking, messaging, idempotency, TTL and scaling

No application or generator source changes. Local files contain synthetic credentials only, stay private under ignored tmp, and are removed with the owned container. PostgreSQL financial authority, inventory locks, writer fairness, idempotency, hold/signature/lease/cache TTLs and Kafka delivery stay unchanged. No API/worker/connection/load scaling.

## Alternatives and consequences

Installing sniffio changes dependencies and does not isolate filesystem placement. Monkeypatching optional imports changes library behavior and is rejected. IncreasingCPU or relaxing timing gates changes the control and is rejected. Treating the failed loopback stage as backend capacity misattributes a synthetic client limitation and is rejected.

Native filesystem qualification better matches cloud native disk metadata behavior, but local Docker still differs from ECS. Source and environment identity must accompany any result. Synthetic gates do not audit financial correctness, durability or backend queue/Kafka drain.

## Failure and recovery

Reject source-hash mismatch before the measured stage. Bound owned child waits; terminate/reap on failure and remove private manifests. Container --rm removes native copies. Failed gates remain failures and prohibit cloud progression, higher load and main merge. Keep the unresolved18k warmup gate even if local generator timing passes.

## Validation evidence

At decision time native-filesystem qualification has not run. Retained raw profile and caller evidence: tmp/adr0135-capacity-investigation/client-profile-diagnostic.json and client-profile-import-callers.json. [Failed original controls](../capacity/flash-sale-opening/single-concert-tooling-local-validation-2026-10-04.json). Append actual result, source identity, limits and owned cleanup before claiming a local pass.
