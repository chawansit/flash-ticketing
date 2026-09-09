# ADR 0015: Bounded hold-path diagnostics before admission tuning

Status: Accepted; locally validated.

## Context
The 352-RPS sustained test returned 20 hold 503s. Immediate admission rejection
bypasses the business outcome counter, and the generator retained only status.
This prevents direct confirmation of ADMISSION_FULL. Short occupancy peaks can
also disappear between Prometheus scrapes.

## Decision
Keep admission concurrency, PostgreSQL locking/transaction boundaries, Redis
shield TTL, payment idempotency, outbox and Kafka semantics unchanged. Instrument
admission decisions and occupancy at arrival, current per-process occupancy and
configured limit. Add request-correlated hold phase durations with fixed labels:
dispatch (including validation/auth/thread scheduling), rate_limit, redis_enter,
db_enter, database_body, db_exit and redis_exit. Context enter/exit instrumentation
must preserve exception suppression/propagation and cleanup order. DB exit includes
commit or rollback; it is not a pure commit metric. Per-request phase maps use a
shared ContextVar value across the request's synchronous worker thread.

Add error_code and occupancy to existing structured logs; do not add payloads,
authentication tokens, seat/user identifiers or extra per-query logs. The HTTP load
generator records allowlisted error-code counts and at most 20 request-ID examples.
Unknown/malformed error bodies collapse to OTHER; never retain raw bodies.

## Alternatives
Raising concurrency now could transfer congestion to PostgreSQL. Sampled occupancy
alone misses bursts. Full request/error-body tracing risks unbounded data and secrets.

## Consequences
Small measurement overhead requires a new measured baseline before claiming a
performance improvement. Metrics are process-local; horizontal deployments must
retain instance labels. Phase durations include I/O and are not CPU utilization.
Existing DB query/pool metrics complement, rather than replace, these timings.

## Failure and recovery
Admission slots are released in finally, including exceptions/cancellation. Failed
phase entry must not call exit; body failures must still roll back and release the
shield. Diagnostics do not retry requests, persist ownership, or change recovery.
Only the additional instrumentation needs removal to roll back this decision.

## Validation evidence
Executed: 23 local unit tests passed. Rebuilt Docker Compose full suite: 78 passed
(no skips), including PostgreSQL/Redis integration and real API/Kafka end-to-end
tests. Covered admission accounting, failure/cancellation cleanup, context isolation,
context-manager propagation/suppression and error-code redaction. Real API logs
confirmed all seven phases and ADMISSION_FULL at occupancy 8/8. Ruff and diff checks
passed. Two dependency deprecation warnings remain. See [evidence](../capacity/hold-diagnostics-validation.json). No cloud rerun or capacity
improvement is claimed. Extends ADRs 0007/0009/0014; supersedes no ownership decision.
