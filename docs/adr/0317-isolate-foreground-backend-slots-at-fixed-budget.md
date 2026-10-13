# ADR0317: Isolate foreground and background backend slots at a fixed budget

Status: Rejected for adoption. Local verification and the cloud comparison were executed; the failed workload was independently reconciled and the normal runtime restored.

## Context

Four writers with batches of four issued 30,370 tickets inside five minutes at 102 offered journeys/s, but 71 journeys were undispatched during a database stall. Failure captures included payment commits and status reads waiting in BEGIN. Increasing the batch size to eight worsened results and was rejected by ADR0316. API CPU did not justify adding replicas.

The existing PgBouncer 1.24.1 uses transaction pooling, one ticketing database alias, default_pool_size=24, reserve_pool_size=0 and verified RDS TLS. Foreground requests and background work share its backend capacity. The existing observer filters only the original alias.

## Decision

Test one scheduling change with the same application image, four API pods (two general and two payment connections each), four reservation writers with batch size four, other worker counts/settings, resources, admission limit 20, generator concurrency 500, gateway delays and customer recovery. Offer 102 journeys/s for 300 seconds.

Use three aliases of the same PostgreSQL database and data user:

| Alias | Clients | Backend ceiling |
| --- | --- | ---: |
| ticketing_foreground | APIs, including callbacks | 16 |
| ticketing_background | All native workers | 7 |
| ticketing | Audit and cohort diagnostics | 1 |

Set per-alias pool_size/max_db_connections and a shared max_user_connections=24; keep reserve_pool_size=0. These are routing boundaries, not new databases, users, schemas or physical capacity. Preserve authority, authentication, TLS, connection options, locks, TTL, idempotency and commit durability.

Prepare the new aliases before native API readiness, initially retaining the original alias's ceiling of 24 under the shared user ceiling of 24. After ECS clients stop and before native workers start, clamp the original alias to one. Require verified final limits before paid dispatch.

The Unix admin socket is disabled. Use the existing OS signal interface: SIGUSR1 pauses traffic and releases backends, SIGHUP reloads configuration, SIGUSR2 resumes. Keep admin permissions and socket settings unchanged. Use the existing stats user for read-only observations. Verify a diagnostics connection before pausing.

Save exact original bytes, mode, owner and container identity before mutation. Record each attempt before execution. Require native client absence before restoring the pooler, then restart the captured ECS services. Add per-alias observations and enforce each ceiling and the aggregate budget. Historical single-pool observation remains unchanged.

## Alternatives

Increasing physical connections changes the comparison budget. Adding API replicas or writers does not reserve database service for customers and can increase contention. Larger writer batches were rejected by ADR0316. A persistence rewrite remains an option only if measured transaction cost warrants it.

## Consequences

Background clients cannot consume the final foreground reserve, but unused reserved slots cannot be borrowed. Background queuing can delay ticket issuance and cause more customer polling. This tests scheduling; it does not guarantee recovery from intrinsic WAL or storage stalls.

## Failure and recovery

Require zero generator drops, current latency gates, the approved 0.01% final customer failure limit, zero duplicate charge/double-booking/payment loss, at least 30,000 unique paid-issued tickets inside 300 seconds, complete queue drain and restoration. A failed comparison cannot trigger a load increase. Preserve raw failures and independently reconcile the exact fixture. Stop on unknown aliases/users, changed original configuration, excessive backends, missing measurements or failed restoration.

The first setup attempt, adr0151-b053a8f1546a, paused the pooler but installed no alias configuration and dispatched zero paid capacity journeys. Diagnostics duplicated connect_timeout already supplied by DATABASE_URL. The fix overrides connection options in one dictionary and checks the stats connection before pause. Recovery resumed the unchanged pooler and applied the saved normal topology. That outer topology restoration recreated PgBouncer using its original model. Independent safety payment, runtime and cleanup checks passed; the original failed result remains unchanged (SHA 690c3f39aa6c136e9bdd64155281ac7421183b9ca9e3284aedcd4eb76599cb32). [Recovery evidence](../capacity/cce/backend-partition-pre-dispatch-recovery-2026-10-14.json).

The paid candidate is adr0151-c816742577d3, ledger bounded_cce_ticket_target__b7177356a0c3, raw SHA bb26a572a41bd4dad7d2f119a80202e00eb419b447686e0ff4fd649b4d2e6174. Extend ADR0313 closure only for this retained case: 15,398 recorded orders/payments/tickets; 15,399 dispatched journeys include one failed hold with no recorded order. Require both safety payments, valid ticket relationships, full queue drain, namespace/helper absence and fresh original pooler bytes/alias settings. Preserve the raw financial expectation failure (goal 30,600 versus actual 15,398); it is not a passing load or proof that a recorded payment was lost.

## Validation evidence

Read-only inspection pinned original pooler configuration SHA f77cde46c5f069a445030d6091495ba7777ade8e3da7a707d00be96fe263d859 and image sha256:3db3d7223e93af52b4116f642951a1a5fa44702a88c2a59cf7562cac19320c9e.

Executed local verification used the exact PgBouncer image and PostgreSQL 17.6 with verified backend TLS. All 16 foreground transactions acquired while seven background and one diagnostic backend were occupied and an eighth background transaction queued. The actual diagnostics program with connect_timeout in its DSN, runtime write/signal methods, live SHOW validation and original-byte restoration passed without restarting the local pooler. Owned local containers/network were removed. This is configuration evidence, not paid-capacity evidence. [Local proof](../capacity/cce/backend-partition-local-proof-2026-10-14.json).

Executed 268 focused unit checks covered routing/authority preservation, budget enforcement, lost acknowledgement bookkeeping, timeout option handling, rollback ordering and independent failed-cohort closure. Lint passed.

| Same five-minute workload | Shared pool, ADR0315 | Fixed 16/7/1, ADR0317 |
| --- | ---: | ---: |
| Dispatched journeys | 30,529 | 15,399 |
| Undispatched journeys | 71 | 15,201 |
| Tickets issued inside the window | 30,370 | 14,914 |
| Final customer failures | 0 | 1 |
| Reported total journey p95 | 3,353 ms | 15,827 ms |

Both generator shards retained final evidence. The candidate's ticket-wait p95 was 14,577 ms. In 299 offered-window samples, background averaged 6.98 active backends and 9.71 waiting clients (peak 17). Foreground averaged 9.28 idle backends and no waiting clients. Background wait per assignment was 24.09 ms; foreground was 0.0013 ms. The allocation protected API access while restricting work needed to issue tickets. RDS read/WAL variation may also contribute; one comparison does not isolate every cause.

Reject this allocation. Independent audit confirmed all 15,398 actual payments had unique issued tickets, both safety payments were durable, relationships were valid, queues drained, owned resources were removed and original pooler/runtime settings restored. The original failed capacity and financial-expectation flags remain unchanged. [Full comparison](../capacity/cce/backend-partition-comparison-2026-10-14.json).

The earlier hourly qualification applies to its original configuration. This candidate is not production-qualified. A revised allocation must be recorded before implementation and compared at the same load.

[PgBouncer configuration semantics](https://www.pgbouncer.org/config.html) and [signal pause/reload/resume semantics](https://www.pgbouncer.org/usage.html) support the chosen controls.
