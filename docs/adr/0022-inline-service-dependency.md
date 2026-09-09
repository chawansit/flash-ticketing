# ADR 0022: Inline in-memory service dependency

Status: Experiment completed — candidate not adopted; synchronous dependency restored.

## Context
ADR 0021 measured service lookup execution p95 0.005ms but worker submission/resumption delays near 80ms under hot-seat bursts. The dependency only reads app.state.reservations and performs no I/O.

## Decision
Declare the service dependency async so FastAPI executes the in-memory lookup on the event loop. Preserve its value, signature annotations and all consuming routes. Authentication and synchronous reservation/Redis/PostgreSQL handlers retain their existing execution model. Keep admission eight and all ownership/idempotency/TTL decisions unchanged. Record this decision before code changes.

## Alternatives
Increasing thread tokens is unsupported by sampled occupancy (7/40). Moving blocking handlers onto the event loop would harm responsiveness. Bypassing authentication or introducing early seat rejection is unrelated and risks semantics. Retaining the synchronous lookup is the rollback control.

## Consequences
One worker transition is removed for every route using the service dependency. Future changes must keep this dependency free of blocking work. A local regression verifies FastAPI dependency execution through a real request, not merely the async keyword.

## Experiment and acceptance
Same backend/generator, diagnostic probes enabled in both controls, same pool/TTL/admission. Two fresh-seat 1,000-contender waves per version, order baseline/candidate/candidate/baseline; one five-minute 400 RPS 95/5 uniform read/hold run per version. Verify exactly one live owner per contested seat and acknowledgement persistence after expiry. Candidate must pass full existing cloud suite, uniform read p95 <=150ms, hold p95 <=300ms, no unexpected responses or generator drops. Report burst failed-response <=200ms separately even if still unmet; do not claim maximum capacity. Adopt only if correctness holds and measurements support reduced scheduling overhead without material regression.

## Failure and recovery
Revert dependency to sync if gates fail. Both compared APIs use identical source except this declaration and identical optional diagnostics. Stop benchmark services and remove manifests/firewall after capture; preserve synthetic volumes and failed evidence. The proposed replacement applies only if adopted; following the failed gate, the synchronous choice remains in force. No ownership, messaging, locking or TTL ADR is superseded.

## Validation evidence
Executed: candidate real-route/admission checks passed (2 tests, 1.25s), complete local unit suite passed (44 tests, 9.45s), existing cloud suite passed (84 tests, 13.35s). Four hot waves retained one live owner each and all 12,004 acknowledged holds across 24 worker IDs persisted and expired correctly. Candidate service dispatch was absent while authentication/handler probe coverage remained complete.

Both uniform runs recorded one read ReadError, so both failed the zero-error gate. Candidate read/hold p95 were lower (13.47/51.83ms vs 14.93/58.44ms), and hot failed-total p95 was 582–603ms vs 619–646ms, but admission rejections increased from 1,905 to 1,918. The failed-response 200ms goal remains unmet. Restore sync as declared; retain candidate patch and regression in the experiment artifacts. No execution-model change is adopted and no previous architectural decision is superseded. Diagnose read transport errors before reconsidering adoption. See [full evidence](../capacity/service-dependency/README.md). Services and temporary credentials/firewall were cleaned up.
