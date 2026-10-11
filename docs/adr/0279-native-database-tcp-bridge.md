# ADR0279 Native database TCP bridge

## Status

Accepted for implementation and one bounded five-minute 84 offered journeys/s comparison under the standing work envelope and the user's Continue instruction. Implemented and validated. One unchanged 84/s five-minute candidate completed on 11 October 2026. In-window paid issuance improved 21.1%, but the customer quality gate failed on 183 generator drops. Independent actual-cohort financial audit passed and runtime was restored. The ledger is FAILED_RESTORED; no hourly qualification or production capacity is claimed.

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

Read-only ECS inspection on 11 October 2026 verified the exact cached nginx digest and built-in stream module. No customer traffic was sent by that inspection. Executed 127 focused tests covering opt-in binding, immutable helper identity, changed configuration refusal, lifecycle cleanup and historical runner behavior; all passed. Ruff and offline reproduction verification passed (535 historical inputs, 25 orchestration overlays). A bounded ECS test passed 16 concurrent certificate-verified TLS sessions and 8 MiB of checked payload, abrupt disconnect followed by a fresh connection, a one-second diagnostic idle timeout, upstream refusal and owned container/file cleanup. It sent zero customer requests and zero database queries. See [TCP validation](../capacity/cce/native-database-bridge-tcp-validation-2026-10-11.json). The paid comparison is complete. See the [candidate result](../capacity/cce/native-database-bridge-result-2026-10-11.json) and [independent terminal audit](../capacity/cce/native-database-bridge-terminal-recovery-2026-10-11.json).

References: [nginx stream proxy](https://nginx.org/en/docs/stream/ngx_stream_proxy_module.html), [stream core](https://nginx.org/en/docs/stream/ngx_stream_core_module.html), [retained baseline](../capacity/cce/timeout-baseline-result-2026-10-11.json).

### Measured comparison

Both runs offered 25,200 journeys at 84/s for 300 seconds with the same application image, four APIs, fourteen workers and 24 physical database connections.

| Metric | ADR0278 Python bridge | ADR0279 native bridge |
| --- | ---: | ---: |
| Unique paid tickets inside 300 seconds | 20,279 | 24,558 |
| In-window paid ticket rate | 67.5967/s | 81.86/s |
| Dispatched and completed customer journeys | 20,744 | 25,017 |
| Undispatched journeys | 4,456 (17.683%) | 183 (0.726%) |
| Initially errored customer journeys | 556 (2.680%) | 3 (0.012%) |
| Final failed customer journeys | 0 | 0 |
| Maximum shard journey p95 | 24.159 seconds | 11.303 seconds |
| Database bridge CPU | 0.9236 cores | 0.3711 cores |
| Mean API database pool acquisition | 29.281 ms | 5.696 ms |
| Observed API pool acquisition failures | 625 | 8 |

All three initial customer payment errors recovered with three retries. Five additional callback HTTP 503 responses were observed separately. All 25,017 orders, payments, bookings, tickets and callback records reconciled after expiry, with no lost payment or double-booking. All queues drained; CCE namespace, helpers, native configuration and private inputs were removed, and normal runtime restored. The original financial gate expected all 25,200 scheduled journeys; its failure was retained and independently audited against the actual cohort, without waiving the load gate.

The native bridge reduced CPU 59.8% despite processing more dispatched journeys. API aggregate CPU remained about 1.97 of four cores; worker aggregate about 1.22 of 3.5. The remaining delay includes payment-to-ticket p95 of 10.08 seconds, paid-unfulfilled backlog up to 359 and fulfillment Kafka lag up to 1,590 records. This points to fulfillment scheduling as the next candidate for attribution; it does not prove Kafka broker capacity is exhausted.

Complete bounded slot evidence captured eight failures in one short burst around 93 seconds after offered start. Payment requests were observed in commit, with commit-phase ages up to 151.9 ms; general status transactions also held connections. Cluster WALWrite contention was observed, but provider WAL write/sync timings were unavailable and snapshots do not reconstruct the entire wait. Do not claim a proven storage root cause.

This is one matched offered-load comparison. The reporting-only coordinator correction and its different sealed digest are explicit in the result. Keep load unchanged for the next isolated correction; do not scale or start hourly qualification from this failed quality gate. The 81.86/s result remains below 83.33/s needed for 300,000/hour and below the desired 84/s successful-ticket headroom.
