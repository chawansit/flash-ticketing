# ADR 0016: Isolate deadline-sensitive reconciliation from maintenance

Status: Accepted; local/cloud correctness passed; sustained load has remaining admission failures.

## Context
The diagnostic 352-RPS run had 160 SEATMAP_WARMING reads. Reconciliation age
reached 33.694 seconds against a 30-second map TTL. Maintenance currently performs
up to ten dirty refreshes and 100 hold expirations before each reconciliation pass.
Those counts bound work volume, not elapsed time; reconciliation's 500-ms budget
starts only after those operations finish. This is an avoidable source of delay,
although the benchmark does not prove it was the sole cause of the incident.

## Decision
Run scheduling and bounded reconciliation passes in a dedicated `reconciler`
process. Keep `maintenance` responsible for dirty refresh and hold expiration.
Compose launches both; Prometheus scrapes both. Retain the existing lease token
fencing, SKIP LOCKED claims, per-pass budget, retry/backoff and version-checked Redis
writes. Keep interval 20 seconds, map TTL 30 seconds, hold TTL 120 seconds and
admission 8. No stale-read fallback, read-triggered DB rebuild or TTL extension.

This supersedes the shared-loop placement in ADRs 0008/0011 only; their bounded
work, ownership, recovery and fencing decisions remain accepted. PostgreSQL remains
the seat ownership authority; payment idempotency and outbox/Kafka semantics do not change.

## Alternatives
Increasing TTL changes tolerated stale availability. Shorter intervals add DB/Redis
work without removing head-of-line blocking. Parallel snapshot execution increases
instantaneous pressure and complicates budget accounting. Increasing admission does
not address projection expiry. Start with process isolation as one measured change.

## Consequences and scaling
One extra process and DB pool consume resources; instance-level metrics remain
necessary. Multiple reconcilers coordinate through existing durable leases; this
change introduces no unlimited thread/task queues. CPU/DB/Redis saturation can still
cause expired maps: isolation is not a zero-error guarantee or a capacity result.

## Failure and recovery
A stalled/crashed maintenance process no longer prevents full reconciliation.
Expiry cleanup can lag; reconciliation projects DB ownership as before and does not
release holds itself. A crashed reconciler's leases recover after expiry. Redis loss
is rebuilt by periodic reconciliation. Incomplete maps still fail closed.
Deploy the new role together with updated maintenance. Mixed old/new workers are
fenced but duplicate scheduling overhead is possible. Roll back both images and
remove the extra role together; running only new maintenance leaves maps unreconciled.
No schema migration or data rewrite is required.

## Validation evidence
Executed full rebuilt Compose suite: 80 passed, no skips, two dependency deprecation
warnings. The regression launches the actual reconciler process against isolated
PostgreSQL/Redis data, expires its map, and verifies a new incarnation is rebuilt
while maintenance is absent and the overdue database hold remains ACTIVE. Existing
concurrency, fencing, idempotency and payment tests passed. Ruff/diff checks passed.
The [same-machine 352-RPS, 30-minute cloud rerun](../capacity/huawei-isolated-reconciliation/README.md) completed 633,600 requests: SEATMAP_WARMING decreased from 160 to zero and maximum sampled reconciliation age from 33.694 to 21.099 seconds. However, 83 holds returned ADMISSION_FULL and read/hold p95 increased to 54.340/130.332 ms. The overall zero-error gate failed; this is not a production maximum or an overall performance improvement. All 31,597 accepted holds passed post-expiry durability checks. Full cloud suite: 80 passed, two dependency warnings. The extra process/resource tradeoff needs further measurement before changing admission or scaling assumptions.
