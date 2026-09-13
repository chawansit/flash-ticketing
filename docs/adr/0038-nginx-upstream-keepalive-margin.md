# ADR 0038: Nginx upstream keep-alive margin

Status: Accepted for the measured four-API benchmark topology

## Context

ADR 0032 establishes a ten-second Uvicorn keep-alive with five-second benchmark
client expiry. The four-replica topology adds Nginx between that client and the API.
Its upstream block caches 128 HTTP/1.1 connections but does not set an upstream
keepalive timeout.

The ADR 0037 fresh-seat candidate passed 750 RPS for ten minutes with no errors.
The 30-minute confirmation completed all scheduled requests with zero admission
rejections, generator drops or client transport errors and met latency targets,
but two of 1,282,500 availability reads received HTTP 502 from Nginx. They had no
application request ID and no corresponding API 502 or application connection-close
counter. Nginx access logging is disabled and the container retained no error-log
evidence, so the exact socket event is not directly observed.

The leading inference is stale upstream connection reuse: the explicit Uvicorn
idle timeout is ten seconds, while the Nginx upstream cache has no shorter
retirement setting. This is a load-balancer-to-server boundary separate from the
generator-to-load-balancer margin validated in ADR 0032.

## Decision

Set keepalive_timeout to 5s in the Nginx ticketing_api upstream block. Keep
Uvicorn at ten seconds and the generator client pool expiry at five seconds. This
gives cached upstream connections a five-second margin before the API server may
close an idle socket.

Keep proxy_next_upstream off and workload retry disabled so any remaining failure
stays visible. Preserve four API replicas, DB pool three per replica, admission
four per replica, PgBouncer backend pool 24, workload, fixture and all
PostgreSQL/Redis/Kafka semantics.

Validate the rendered Nginx configuration before traffic. Run 750 RPS for ten
minutes as a safety candidate, then a 30-minute confirmation only if request,
latency and correctness gates pass. Accept the timeout only if the 30-minute stage
has zero unexpected HTTP responses, transport errors and generator drops, plus
exact acknowledged persistence, zero held-seat overlap and drained queues.

## Alternatives considered

- Enable Nginx retry for read requests. Rejected because it hides the failed
  upstream attempt from this capacity gate and changes offered backend traffic.
- Raise Uvicorn keep-alive further. Deferred because ADR 0032 already provides a
  bounded ten-second server value; the new boundary exists in the upstream cache.
- Disable upstream keep-alive. Rejected because it would add a backend TCP
  connection per request and materially change CPU and latency.
- Ignore two failures as statistically small. Rejected because the strict
  availability gate is zero unexpected responses.
- Enable full access logging during the load. Deferred because per-request logs
  would add material I/O at 750 RPS. A later diagnostic can add sampled or
  error-only upstream telemetry.

## Consequences

Nginx retires idle upstream sockets sooner and may create more backend TCP
connections when a cached connection is unused for five seconds. At this load the
four APIs receive continuous traffic, so expected churn is bounded; connection
attempts and generator/network pressure remain measured.

The change affects only Nginx-to-API connection reuse. It does not change client
keep-alive, request retries, seat ownership, transaction boundaries, hold TTL,
idempotency or event delivery.

## Failure and recovery behavior

If nginx -t or readiness fails, restore the prior configuration before traffic.
If either load stage records any 502, reset, unexpected response, drop, latency
failure, durability mismatch, overlap or undrained queue, retain the evidence and
do not claim 750 RPS as clean. Removing the directive restores the prior
upstream-cache behavior.

## Validation evidence

The rendered Nginx configuration passed `nginx -t`, and the deployed upstream
configuration exposed a five-second `keepalive_timeout`. Proxy retry remained
disabled.

The ten-minute safety stage completed all 450,000 requests with zero unexpected
responses, transport errors or generator drops. Read p95 was 8.236 ms, hold p95
was 50.443 ms and all 22,500 acknowledged holds passed post-expiry persistence,
overlap and queue-drain checks.

The 30-minute confirmation completed all 1,350,000 scheduled requests. It
returned 1,282,500 successful conditional/read responses and 67,500 HTTP 201
holds with zero unexpected HTTP responses, transport errors, admission
rejections or generator drops. Worst-worker read/hold p95 was 8.251/51.050 ms.
PgBouncer sampled one 1.387 ms wait in 9,001 samples, RDS connections peaked at
20, active connections at 13 and lock waiters at zero. The post-expiry audit
found 67,500 durable linked records, zero overlapping hold intervals and zero
outbox, refresh or dead-letter backlog.

This accepts the five-second upstream timeout for the measured topology. The
two earlier 502 responses lacked socket-level Nginx evidence, so stale upstream
reuse remains the supported inference rather than a directly observed root
cause.
