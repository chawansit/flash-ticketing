# ADR 0034: Fixed-budget horizontal API scaling experiment

Status: Proposed; implementation and cloud validation pending

## Context

ADR 0006 established that API replicas must share PostgreSQL, Redis and Kafka
and that aggregate connection and admission budgets must be bounded. ADR 0033
accepts admission twelve with one API process and a DB pool maximum of twelve.
The accepted topology passes 500 RPS for 30 minutes. At 600 RPS, API CPU
averaged 94.672% of one core and produced 352 hold admission 503 responses, 69
server concurrency 503 responses and 68 generator drops, while PostgreSQL
averaged 44.809% of one core, sampled active DB connections peaked at four and
sampled lock waiters remained zero.

The evidence supports distributing API scheduling and protocol work across
cores before moving PostgreSQL. Scaling must not silently multiply database
connections or weaken cross-instance seat ownership.

## Decision

Add an opt-in Nginx layer and scale the existing stateless API service to two
containers on the same backend ECS. Remove host port publication from the API
replicas; publish only the load balancer on the private benchmark address. Use
round-robin upstream selection and persistent upstream connections. Disable
proxy retries so connection and application failures remain visible.

For two replicas, configure DB pool six and hold admission six per instance.
The aggregate API pool and admission budgets therefore remain twelve. All
replicas use the same PostgreSQL authority, Redis hold shield and Kafka cluster.
No sticky sessions are required because request ownership and idempotency state
are shared.

Before sustained load, send 100 concurrent authenticated hold requests for one
fresh seat through the load balancer and require exactly one HTTP 201 and one
durable owner. Then run 500 RPS as a control and 600 RPS for 30 minutes with the
same 95/5 workload, generator, no-retry diagnostics, keep-alive ordering and
post-expiry verification used by ADR 0033. Collect CPU per replica and API
metrics from each replica; keep the aggregate connection budget fixed.

Do not test four replicas until two replicas pass correctness and the 600 RPS
gate. A later four-replica experiment must use pool three and admission three
per instance so aggregate budgets remain twelve.

## Alternatives considered

- Move PostgreSQL first. The failed 600 RPS run shows API CPU near one core while
  PostgreSQL remains below half a core and sampled lock waits are zero.
- Increase Uvicorn concurrency or admission again. This admits more work to one
  saturated process and does not distribute Python scheduling.
- Use multiple Uvicorn workers inside one container. That shares the machine but
  obscures per-instance resource and health evidence needed for the planned
  horizontal topology.
- Let each replica retain pool and admission twelve. This would double aggregate
  database and hold pressure and make comparison invalid.
- Enable Nginx retry. It could hide a failed request and is unsafe as a general
  policy for non-idempotent traffic without method-specific rules.

## Consequences

The load balancer adds one network hop and a new failure surface. Persistent
client connections can create imperfect distribution, so per-replica requests
and CPU must be inspected. Aggregate API capacity can use more backend cores
without increasing configured database connections.

Process-local admission becomes six per replica. A brief uneven connection
distribution may reject on one replica while aggregate capacity remains; that
outcome must remain a failure rather than being retried by the proxy.

This experiment validates same-host horizontal execution. It does not establish
multi-host availability, load-balancer HA or PostgreSQL/Redis/Kafka HA.

## Failure and recovery behavior

If the load balancer or either API replica fails readiness, stop before load and
restore the single API service. If the 100-contender test produces zero or more
than one durable winner, stop immediately and restore the accepted
single-instance topology.

Retain all 503, transport errors and generator drops. Stop rate escalation on a
failed gate. After each stage, wait beyond hold TTL and verify exact
idempotency/hold/order persistence, zero overlapping seat intervals and drained
outbox, refresh and dead-letter queues.

Removing the horizontal Compose override and recreating the API restores the
single-instance topology with pool/admission twelve.

## Validation evidence

Pending.
