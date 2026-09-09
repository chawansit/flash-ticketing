# ADR 0020: Protocol ingress timing and bounded admission experiment

Status: Accepted — experiment completed; admission 16 not adopted. Default admission 8 retained.

## Context
ADR 0019 separated generator lane waits and connection establishment but not server scheduling before application middleware. The existing server uses pinned Uvicorn 0.52.4 / httptools 0.8.0. Hot-seat waves retain one durable winner but mostly return admission rejection at limit eight.

## Decision
Add an opt-in benchmark server adapter using the same httptools protocol. Timestamp its headers-complete callback on each request, then measure entry into the ASGI application and response-header emission with the same monotonic clock. Preserve existing Server-Timing app duration and append headers-to-ASGI and ASGI-to-headers durations; add fixed-label Prometheus histograms and correlated structured logs. This measures protocol/application scheduling, including pipelined request waiting, but excludes network/kernel waiting before the callback and response transmission after headers. No credentials or payloads enter logs.

Compare admission eight and sixteen using the same adapter, machine, pool maximum twelve, Redis shield and authoritative PostgreSQL transaction. Do not reorder authentication, idempotency or seat checks and do not treat Redis state as an ownership grant. Use two warm four-process 1,000-contender waves per limit with equal total lanes, fresh seats and no retries; collect direct protocol timing. Run five minutes of uniform 400 HTTP RPS (95/5 reads/unique-seat holds) at each limit to test distinct-seat pressure. Instrumented eight is the matched baseline. Keep the default eight unless sixteen reduces admission failures without unexpected errors, correctness loss, scheduling drops or read/hold latency gate failure. Findings may justify further work rather than adoption.

## Alternatives
A proxy adds an extra hop; packet timing cannot cheaply isolate application scheduling with persistent connections. Switching HTTP parsers would confound results. Raising admission without distinct-seat validation can overwhelm the database pool. An early Redis seat-rejection cache risks breaking idempotent replays and stale-availability semantics; it is not selected.

## Consequences
The adapter depends on the pinned Uvicorn protocol callback and remains opt-in, protected by real-socket tests for fragmented headers and keep-alive. Both compared settings incur identical instrumentation overhead. Increasing admission to sixteen is an experiment, not a production sizing claim. No locking, messaging, idempotency, TTL or seat-ownership decision is superseded. Default admission/scaling policy remains unchanged until executed evidence supports an explicit adoption decision.

## Failure and recovery
Missing protocol timing is reported, never replaced with fabricated zero. Preserve all admission and transport failures. Stop escalation on correctness failure; revert admission eight after a failed candidate. Verify one live durable owner per contention wave and all acknowledged records after expiry. Stop benchmark services and delete temporary credentials/firewall rules after capture. Retain synthetic data and all failed evidence.

## Validation evidence
Executed: real HTTP protocol and harness tests passed locally; the existing cloud suite passed 84 tests in 12.97s. Matched cloud experiments completed on 9 September 2026. Gates: one durable winner per contested seat, exact acknowledgement persistence and expiry, zero unexpected responses/drops for uniform load, reads p95 <=150ms and holds p95 <=300ms. Failed contender response target remains <=200ms including generator waiting; do not hide it when reporting admission improvements.


## Executed decision
Admission 16 reduced ADMISSION_FULL from 1,921 to 1,768 across two 1,000-contender waves, but failed-contender total p95 increased from 560–611ms to 649–681ms. Both settings passed 400 HTTP RPS for five minutes, with zero unexpected responses or generator drops. All four contested seats had exactly one live durable owner. All 12,004 acknowledged holds across 24 worker IDs matched persisted records and expired correctly. Keep default eight: neither configuration meets the failed-contender 200ms target, and fewer overload responses alone do not justify adoption. No prior ownership or scaling decision is superseded. The optional instrumentation is retained for further diagnosis.

See [full report and raw evidence](../capacity/ingress-admission/README.md). Services were stopped, temporary credentials and firewall rule removed, and ECSs/data preserved.
