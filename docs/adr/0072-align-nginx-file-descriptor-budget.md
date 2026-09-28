# ADR 0072: Align the Nginx file-descriptor limit with its bounded connection budget

Status: Proposed

## Context

The corrected seat-delta implementation passed a three-minute 1,000 RPS control, but the first corrected 15-minute stage failed after 775,280 of 900,000 scheduled requests. It recorded 124,720 generator drops, 168 read timeouts, worst-worker read p95 of 963.488 ms and hold p95 of 1,355.540 ms. PostgreSQL remained below the observed bottleneck: sampled queries stayed below 11.949 ms, PgBouncer ended with no waiting clients, queues drained, and no overlapping holds were found.

The persistent load-balancer logs identify the actual availability failure. From 10:41:31 through 10:53:03 UTC Nginx emitted 746 `accept4()` or `socket()` failures with `EMFILE` (`No file descriptors available`). Its configuration allows 4,096 worker connections and 128 upstream keepalive connections, but the container soft `nofile` limit is only 1,024. A reverse-proxied in-flight request can consume both a downstream and an upstream descriptor, so the operating-system limit can be exhausted before the configured Nginx connection bound. The resulting admission failure produced Nginx HTML 500 responses, client read timeouts and a positive feedback loop of generator backlog.

## Decision

Keep the existing bounded Nginx connection policy (`worker_connections 4096`, upstream keepalive 128) and set the load-balancer container soft and hard `nofile` limits to 65,535 through Docker Compose.

The high process limit is only an operating-system ceiling. Nginx remains bounded by its smaller configured worker-connection limit, so this change makes the declared budget effective without admitting unbounded traffic. The Huawei deployment helper must verify after deployment that the effective soft limit is at least 4,096 and fail before load if it is lower.

## Alternatives considered

1. **Reduce generator concurrency below the existing 512 total in-flight requests.** Rejected because it would hide an edge configuration defect and would no longer validate the intended offered load.
2. **Raise `worker_connections` and add more Nginx workers at the same time.** Rejected for this correction because the measured failure occurs below the already declared 4,096 connection budget. Changing both would prevent a controlled comparison and could shift overload into the API or Redis.
3. **Shorten client keepalive or disable upstream keepalive.** Rejected because it increases connection churn and does not correct the mismatch between the declared Nginx budget and its process limit.
4. **Retry failed requests.** Rejected as a capacity fix because retries add traffic during descriptor exhaustion and conceal the first-attempt availability failure.

## Consequences

- Nginx can use its configured 4,096 worker connections instead of failing at approximately 1,024 open descriptors.
- The load-balancer can absorb the generator's bounded 512-client in-flight ceiling plus upstream sockets and normal process overhead.
- The host must allow the container hard limit, and monitoring should continue to detect `EMFILE` rather than treating the larger ceiling as proof of capacity.
- API, Redis and PostgreSQL limits are unchanged; a later bottleneck must be measured independently.

## Failure/recovery behavior

If Docker cannot apply the limit or the effective limit remains below 4,096, deployment verification stops before load. Existing services remain reachable or are restored by the stage runner. If descriptors still exhaust with the corrected limit, the stage fails without retries; Nginx logs and client accounting identify the failure. Rolling back the Compose override restores the former 1,024 process limit and its known sustained-load risk.

## Validation evidence

Before implementation, the failing 15-minute run at commit `2c2c2f5` showed:

- first `EMFILE` at 2026-09-28 10:41:31 UTC and last at 10:53:03 UTC;
- 746 Nginx descriptor failures;
- effective container soft `nofile` 1,024 versus `worker_connections 4096`;
- 775,280 physical attempts, 124,720 generator drops and 168 `ReadTimeout` failures;
- no admission rejection, no overlapping hold interval and fully drained queues;
- six timeout-after-commit outcomes: durable records exceeded observed HTTP 202 acknowledgements by six across four worker runs, so the strict acknowledgement-equality audit failed even though no acknowledged hold was missing.

The implementation adds a Compose soft/hard `nofile` limit of 65,535, recreates the load balancer during candidate deployment, waits for health and verifies an effective soft limit of at least 4,096. Docker Compose configuration validation, POSIX shell syntax validation, Ruff, the focused current-source regression suite (42 tests) and the complete current-source unit/integration suite (254 tests; two dependency deprecation warnings) passed on 2026-09-28. Cloud validation of the new effective limit remains pending.
Acceptance requires configuration validation, repository tests, a sustained control that crosses the previous failure onset, and a corrected 15-minute 1,000 RPS run with zero unexpected responses, zero drops, latency gates, exact durability accounting, zero overlap and drained queues.
