# ADR0251: Decouple callback dispatch concurrency from the database pool

## Status
Accepted for implementation and local qualification. Cloud benefit is unmeasured.

## Context
ADR0249's accepted-binary control failed at 84 offered journeys/s. Callback due-to-claim mean grew to 4996.63 ms, with 431 pending deliveries and eight active dispatch jobs. Mean claim, HTTP delivery and acknowledgement phases were 13.53, 87.59 and 13.02 ms. Their sum suggests roughly 70 deliveries/s at eight jobs, but this estimate is not a measured capacity ceiling. API process CPU was 2.05 cores across four pods, while the background host averaged 84.13 percent CPU. Increased order-status polling amplified database pressure.

simulate_one commits and releases its claim connection before HTTP delivery, then acquires a connection only for acknowledgement. Settings nevertheless rejects SIMULATOR_CONCURRENCY above DB_POOL_MAX. HTTP occupancy and database occupancy are different resources.

## Decision
Validate simulator concurrency independently as an integer between 1 and 32, with the existing default of four unchanged. Retain bounded refill scheduling, one HTTP connection per dispatch slot, the existing DB pool and waiter limits, SQL timeouts, 15-second fenced payment leases, callback idempotency, payment durability and current gateway delay settings. Do not acquire a DB connection for the HTTP wait.

The first cloud correction will use 12 simulator jobs and the existing simulator DB pool of ten, changing no API resources or database connection budgets. Preparation must extend the existing proven runner's image/configuration binding for this simulator-only factor. Preserve the historical API pair by excluding this worker-only validation change from its source builder; its reviewed config hashes must remain identical. A failed old control remains failed and consumed; no old candidate is dispatched. Fresh corrected qualification must retain all customer latency/error, safety, post-TTL, durability, zero-double-booking, queue and restoration checks.

## Supersession
Partially supersedes ADR0008's dispatcher sizing assumption and the later SIMULATOR_CONCURRENCY <= DB_POOL_MAX validation. Existing bounded scheduling, transaction release before provider calls, leases and replay behavior remain accepted. This does not supersede financial locking, TTL, callback delivery semantics or production payment-provider decisions.

## Alternatives
Increase the DB pool to fit twelve jobs: adds unnecessary connection capacity for an HTTP wait. Unbounded threads: unsafe under a slow gateway. Reduce gateway latency or relax quality gates: invalidates the workload. Combine more API replicas, changed polling and worker placement: obscures attribution. Change to asynchronous confirmation simultaneously: a separate architectural factor.

## Consequences
More HTTP requests may be in flight while at most the configured database pool can execute claim/ack transactions. Queueing can move to the callback API or DB pool; twelve jobs are a bounded hypothesis, not a throughput promise. The simulator remains development-only. No application default or production topology is automatically scaled.

## Failure and recovery behavior
An HTTP timeout or ambiguous response leaves the existing lease for later redelivery with the same callback identity; no inline retry is introduced. A stale lease token cannot acknowledge another owner. Stop prevents new submissions and drains already submitted jobs. Pool exhaustion remains bounded by existing waiters/timeouts. Roll back the experiment's simulator image and concurrency to the saved normal runtime if any gate fails; preserve failed evidence.

## Validation evidence
Executed 36 dispatcher, real HTTP transport and backend-default unit tests; all passed. Executed 72 PostgreSQL/Redis integration tests covering the new twelve-job/two-connection test, duplicate callbacks, committed-response loss, stale lease fencing, reservations, payment context and ticket issuance; all passed after correcting a missing TEST_REDIS_URL in the initial test environment. The first environment run remains retained as 69 passed / 3 failed. Financial tests use the accepted explicit-BEGIN transaction implementation, matching the pinned runtime.

Executed 88 image-source, CCE profile, historical reproduction and transaction-guard tests; all passed. Another 133 CCE API-adapter, paid-entry and stage tests passed. Historical reproduction verified 535 files, six declared overlays and 78 frozen generator files. Ruff passed. Built the worker image offline from the accepted simulator parent; verified 21 copied/installed/imported/bytecode modules, inherited runtime configuration and parent layers, twelve-job/ten-connection settings and the runner image verifier. The image receipt is [simulator-dispatch-image-2026-10-09.json](../capacity/cce/simulator-dispatch-image-2026-10-09.json). The original API pair remains unchanged. No cloud load, measured capacity improvement or production qualification is claimed.

The runner selects a separately registered correction arm, retains the failed ADR0249 reference as failed, and binds the immutable simulator image and twelve-job/ten-connection settings. Other role images, settings, restore parents and API budgets remain unchanged. No old control/candidate scope is reopened.
