# ADR 0026: Direct ASGI request instrumentation and admission

Status: Accepted after correctness and controlled cloud comparison.

## Context

Fresh 100/1,000-contender warm-connection baseline: one HTTP winner per wave; failed-total p95 85.8/556.3ms. Probe coverage spans 64 admitted requests. Authentication execution p95 0.133ms, service lookup 0.005ms, handler 12.231ms, database body 8.773ms; dispatcher submission/resumption intervals are tens of milliseconds and aggregate pre-handler dispatch p95 368.3ms. Percentiles are separate distributions, not additive. Scheduling overhead is a supported target, but middleware causality is still a hypothesis.

## Decision

Replace only the BaseHTTPMiddleware-based request instrumentation/admission adapter with direct ASGI middleware. Preserve process-local no-await check/increment, admission limit, reject-before-auth behavior, status/error bodies, request IDs, response timing headers, fixed-label metrics and structured logs. Release admission once response headers are ready, or in finally on failure/cancellation; never decrement twice. Keep context until the downstream ASGI call finishes and restore it in finally. Pass non-HTTP scopes unchanged. Keep authentication and service dependencies synchronous and all DB/Redis handlers unchanged. This removes middleware task/memory-stream hops without moving blocking work to the event loop.

## Alternatives

Async service alone removed one lookup hop but previously failed the overall gate (ADR 0022). Changing auth execution or adding cache-based early seat rejection introduces other semantics and is deferred. More worker tokens are unsupported by prior sampled occupancy. Keep the existing adapter as rollback control.

## Consequences

Pure ASGI middleware has explicit response-start, streaming, exception and cancellation responsibilities. Header timing remains time to response headers, not full response-body duration. ASGI send/backpressure and client queues remain outside that timing. State remains per process; PostgreSQL remains global ownership authority. No JWT bypass, unbounded retries or global-lock claim.

## Failure and recovery

Test full-admission rejection, successful/error responses, cancellation, streaming release and context isolation; execute the full applicable cloud suite in isolated test schemas. Compare fresh warm 100/1,000-contender waves, verify one live durable owner and later expiry, then uniform/mixed load with all failures retained. Revert if correctness/security gates fail or benefit is unsupported. A latency target miss is reported explicitly even if directional improvement exists. No persistence, TTL, idempotency or Kafka ADR is superseded; the instrumentation/admission implementation is replaced.

## Validation evidence

Full candidate cloud suite: 114 tests passed (2 dependency warnings). Latest local unit suite including the measurement harness: 67 passed (2 dependency warnings). All six valid contention waves had one live durable owner; corrected post-expiry verification also passed. Direct ASGI 1,000-contender failed-total p95 was 464.843/495.394ms versus baseline 556.334/575.859ms; the 200ms target remains unmet and candidate admission rejections increased slightly. Uniform 400 RPS for 300s passed both variants with 6,000 successful holds each and no dropped requests: worst-worker read p95 12.973 -> 11.954ms and hold p95 60.770 -> 48.846ms. This is a directional paired result, not a statistical confidence interval. A short read-only diagnostic ran during the candidate uniform window; observer coverage also differs. Preserve baseline source for rollback. Default client expiry stays 5s, server timeout 5s, admission 8, DB pool 12, hold TTL 120s. Both contention variants retain identical optional protocol/dispatcher probes.
