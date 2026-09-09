# ADR 0016: Isolate deadline-sensitive reconciliation from maintenance

Status: Accepted; local validation passed, cloud validation pending.

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
A new same-machine sustained cloud run remains required to quantify the improvement.
