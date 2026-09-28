# ADR 0073: Encode seat-map delta responses in the synchronous read worker

Status: Proposed

## Context

The corrected five-minute 1,000 RPS stage at revision `04bd56f` removed the Nginx descriptor failure but did not meet the read or hold latency gates. It completed 275,909 of 300,000 scheduled requests, dropped 24,091 at the generator in-flight bound, and recorded worst-worker read p95 838.572 ms and hold p95 1,194.575 ms. There were no transport errors, Nginx `EMFILE` or upstream-timeout errors, admission rejections, duplicate holds, durability mismatches, or residual queues.

The saved pressure evidence isolates the next bottleneck. Across all four API replicas, event-loop lag p95 reached the 100 ms histogram bucket, p99 reached 150–250 ms and instantaneous peaks reached 145–240 ms. Generator scheduling p95 was 1.1 ms, PgBouncer had no waiting clients, and the maximum sampled RDS observer query was 9.224 ms. The synchronous delta route currently returns a Python dictionary. FastAPI then performs response-model validation and JSON serialization after the worker call returns, which adds CPU work on the event loop at the dominant read volume.

## Decision

Keep the seat-delta endpoint as a synchronous route and construct a `JSONResponse` inside that route from the cache result. FastAPI treats an explicit response object as final, so JSON encoding occurs while the synchronous handler is running in its worker thread and response-model validation is skipped at runtime. Retain `response_model=AvailabilityDelta` so OpenAPI continues to document the response contract.

The Redis delta continuity, reset behavior, HTTP schema and cache semantics remain unchanged. Integration tests remain responsible for checking the returned schema and behavior.

## Alternatives considered

1. **Add more API replicas on the same four-vCPU ECS.** Deferred because the evidence shows CPU/event-loop pressure; adding processes without adding CPU can increase scheduling contention and would not isolate the serialization cost.
2. **Scale to another ECS immediately.** Deferred until this bounded software correction is measured, because it adds infrastructure and changes capacity topology before removing known per-request CPU overhead.
3. **Add ORJSON as a new dependency.** Deferred because returning an explicit response removes validation and moves encoding off the event loop without adding a library or changing JSON compatibility.
4. **Encode and coalesce the complete HTTP response in Redis Lua.** Rejected for this step because it moves presentation concerns and more CPU work into shared Redis, increasing blast radius for all API replicas.
5. **Add a local delta microcache.** Deferred because it introduces bounded staleness and per-replica cache coherence behavior that require a separate decision and validation.

## Consequences

- The dominant delta-read path avoids runtime Pydantic response validation and event-loop JSON encoding.
- OpenAPI retains the typed `AvailabilityDelta` contract.
- Malformed cache output is no longer caught by FastAPI response validation, so cache unit tests and endpoint integration tests must continue to assert the exact schema.
- This change does not reduce Redis Lua work, network payload size or reservation-writer load. If event-loop pressure remains high, those components need separate measurement and decisions.

## Failure/recovery behavior

Cache failures continue through the existing structured `Failure` handling before response construction. If JSON encoding fails, the request returns the existing unhandled-server-error behavior and API error telemetry records it. Reverting the explicit `JSONResponse` restores FastAPI runtime validation and event-loop serialization without changing Redis or persisted state.

## Validation evidence

Pre-change cloud evidence from run `20260928T113121Z-5b02de2f` at revision `04bd56f`:

- 275,909 physical attempts and 24,091 in-flight drops over five minutes at 1,000 offered RPS;
- zero transport and Nginx errors;
- read p95 838.572 ms and hold p95 1,194.575 ms;
- generator scheduling p95 1.1 ms;
- API event-loop lag p95 in the 100 ms bucket on every replica, with 145–240 ms peaks;
- PgBouncer waiting and max wait both zero; maximum sampled RDS query 9.224 ms;
- 16,539 acknowledged holds were durable, zero overlapping intervals, and all queues drained.

The explicit-response implementation passed Ruff, 15 focused API/browse tests and the complete current-source unit/integration suite (256 tests; two dependency deprecation warnings) on 2026-09-28. A corrected short cloud comparison remains pending. Acceptance requires unchanged endpoint/schema tests, reduced API event-loop lag and improved read/hold latency under the same topology and workload. A later sustained certification is still required before claiming 1,000 RPS capacity.
