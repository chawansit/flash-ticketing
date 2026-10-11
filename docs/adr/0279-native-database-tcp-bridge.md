# ADR0279 Native database TCP bridge

## Status

Accepted for implementation and one bounded five-minute 84 offered journeys/s comparison under the standing work envelope and the user's Continue instruction. Implemented and locally validated; cloud results remain pending. No improvement or hourly qualification is claimed yet.

## Context

ADR0278 measured 20,279 unique paid-and-issued tickets inside 300 seconds (67.5967/s), 4,456 generator drops and 556 initially errored customer journeys, all recovered. The Python database bridge used 0.9236 of its one-core budget; four APIs used 1.8855 of four cores and fourteen workers 1.0588 of 3.5 cores. Database acquisition failures and payment backlog remain; these observations identify a candidate bottleneck, not its sole proven cause. Failure-slot capture is incomplete and must remain labeled partial.

## Decision

Replace only the candidate's Python PostgreSQL TCP bridge with nginx's native stream proxy. Reuse the already cached nginx 1.27.5 image at immutable image/repository digest sha256:65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10. Read-only inspection verified its built-in --with-stream module. Require this exact cached image, command, configuration hash, labels, network and execution identity before use and removal. Keep the historical bridge as the default unless ADR0279 is explicitly selected.

Use one nginx worker, 1 vCPU and 64 MiB, a read-only root filesystem, a small bounded tmpfs for the PID, an unprivileged user and the existing private port/network. Forward opaque TCP bytes to the existing PgBouncer endpoint. Preserve the five-second connect timeout, 300-second inactivity timeout and 64-KiB buffer. Do not terminate TLS, alter authentication, add SQL/protocol logic, retry connections, expose a public database listener, bypass PgBouncer or add database connections. Leave the Kafka bridge unchanged.

Compare against retained ADR0278 evidence at the same 84/s for 300 seconds, 500 active journeys across two generator shards, four APIs, fourteen workers, application image, 24 PgBouncer connections, admission and application/consumer pool budgets, gateway delays, polling and customer recovery policy. The current ADR0278 reporting-only correction retains complete final reports with quality-failing exit 1; it changes no offered workload or application behavior. Bind and report that source identity difference.

## Alternatives

Increasing a Python container's CPU limit does not parallelize its single event loop and changes the resource budget. Adding API pods does not directly address the measured bridge usage. A new HAProxy image is reasonable but requires an additional dependency when the cached nginx already provides native forwarding. Direct access to RDS would bypass the controlled pool budget. Keep the original bridge if the candidate cannot demonstrate a useful improvement.

## Consequences

Native forwarding should reduce Python scheduling and allocation overhead. This is a hypothesis to measure. Nginx closes an inactive stream as a session rather than maintaining the original per-direction coroutine clocks; the configured inactivity bound remains 300 seconds and actual disconnect/timeout behavior must be tested. It does not fix database, Kafka, status-cache or generator capacity limits by itself.

## Failure and recovery behavior

An upstream refusal/disconnect is surfaced; the bridge does not replay SQL. Existing idempotency, seat expiry, authorization, payment durability and customer recovery remain responsible for correctness. Refuse unknown image/configuration ownership and changed resources. Before paid load, execute concurrent TCP/TLS passthrough and disconnect/idle/upstream-refusal checks with a mock upstream and zero customer requests. Remove only owned helpers and configuration after exact identity checks. After the paid experiment retain errors, inspect financial integrity, drain all queues and restore the captured normal runtime. A failed quality gate prevents load escalation and hourly qualification; never reset or replay a consumed experiment.

## Supersession

Supersedes only ADR0228's Python database bridge for explicitly selected ADR0279 candidates. Extends ADR0271's fixed worker placement and ADR0278's retained evidence without changing their historical scopes, images or results. Does not supersede payment, locking, persistence, expiry or Kafka semantics.

## Validation evidence

Read-only ECS inspection on 11 October 2026 verified the exact cached nginx digest and built-in stream module. No customer traffic was sent by that inspection. Executed 127 focused tests covering opt-in binding, immutable helper identity, changed configuration refusal, lifecycle cleanup and historical runner behavior; all passed. Ruff and offline reproduction verification passed (535 historical inputs, 25 orchestration overlays). A bounded ECS test passed 16 concurrent certificate-verified TLS sessions and 8 MiB of checked payload, abrupt disconnect followed by a fresh connection, a one-second diagnostic idle timeout, upstream refusal and owned container/file cleanup. It sent zero customer requests and zero database queries. See [TCP validation](../capacity/cce/native-database-bridge-tcp-validation-2026-10-11.json). The paid comparison remains pending.

References: [nginx stream proxy](https://nginx.org/en/docs/stream/ngx_stream_proxy_module.html), [stream core](https://nginx.org/en/docs/stream/ngx_stream_core_module.html), [retained baseline](../capacity/cce/timeout-baseline-result-2026-10-11.json).
