# ADR 0032: Server keep-alive margin experiment

Status: Accepted for the measured benchmark topology; general production default unchanged

## Context

ADR 0024 reproduced 22 read transport failures near the five-second server
idle boundary. ADR 0025 compared five-second and two-second client pool expiry:
both bounded sustained variants happened to complete without an error, while the
two-second client setting created 2.22 times as many TCP connections. The client
default therefore remained five seconds.

The matched ADR 0031 cloud run has now reproduced one reset in 684,000 reads.
The failed request reused an existing connection, sent its request, and received
`ConnectionResetError` errno 104 while waiting for response headers. The API
container did not restart or run out of memory and recorded no ASGI-layer send
error or client disconnect. The benchmark currently gives the client pool and
Uvicorn the same five-second idle expiry, leaving a boundary race.

## Decision

Add an explicitly bounded `SERVER_KEEPALIVE_SECONDS` setting to the benchmark
ingress server, retaining five seconds as the source default during the
experiment. Compare the failed five/five control with a ten-second server and
unchanged five-second client at 400 RPS for 30 minutes. Keep the same application
commit, topology, fixture, API worker, DB pool, admission, workload mix and
no-retry transport diagnostics.

The candidate creates a strict ordering: an idle benchmark client retires a
connection before the server's idle timeout. Measure TCP connection attempts,
latency, CPU, connection-close telemetry, durability and all queues. Do not
escalate to 500 RPS unless this 400 RPS gate has zero unexpected responses,
transport errors and drops.

This experiment does not add automatic retry and does not change hold,
PostgreSQL, Redis, Kafka, idempotency or TTL semantics.

## Alternatives considered

- A two-second client expiry already increased connection churn by 2.22 times
  without demonstrating error reduction in ADR 0025.
- Retrying GET after a reset could be safe for availability reads, but it would
  hide the transport failure from this capacity gate and would not fix the
  server/client boundary.
- Leaving equal timeouts preserves a known race window reproduced by ADR 0024
  and again observed in the ADR 0031 soak.
- A much longer server timeout retains more idle sockets without evidence that
  the extra margin is needed. Ten seconds is a bounded first comparison.

## Consequences

The server may retain an idle connection for up to five seconds longer. Active
connections and request behavior are unchanged. File descriptor and memory use
may rise when clients disappear without closing sockets, so the experiment must
retain container resource evidence.

A passing run establishes one bounded configuration, not universal elimination
of network resets. Production deployments behind a load balancer must align
client, load-balancer and server idle timeouts separately.

## Failure and recovery behavior

Invalid values fail server startup rather than silently applying an unsafe
timeout. If the candidate fails health checks, restore the five-second default.
If any measured request fails, retain all artifacts, do not retry or reclassify
the result, restore the default and stop load escalation.

A lost SSH control session does not affect background collectors or generators.
After every run, wait for hold expiry and verify durable ownership and queues
before changing configuration.

## Validation evidence

The five/five control at commit `152414d` attempted 720,000 requests with one
reused-connection reset and zero drops. All 36,000 holds remained durable with
zero overlap and all queues drained. PostgreSQL indexing evidence is recorded in
ADR 0031.

The ten/five candidate at commit `1a68518` passed 400 RPS for 30 minutes:
720,000 requests, zero unexpected responses, transport errors or drops, read
p95 12.817 ms and hold p95 38.715 ms. The subsequent 500 RPS admission-eight
control and admission-twelve candidate completed another 1.8 million requests
without a transport error. All accepted holds passed post-expiry durability and
overlap checks.

Accept ten-second server keep-alive for this measured benchmark topology while
the client pool expiry remains five seconds. Keep the script fallback at five
seconds and the override explicit because an external load balancer requires a
separate timeout decision. This evidence clears the capacity-test transport
gate but does not prove universal elimination of resets. Full evidence is in
[the Huawei report](../capacity/huawei-keepalive-admission/README.md).
