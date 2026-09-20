# Diagnose the 750 RPS commit spike

The 2026-09-20 750 RPS safety stage failed strict availability when five holds
returned DATABASE_UNAVAILABLE around 01:42:22 UTC. The API pools were
occupied while commit calls crossed 100–500 ms. Huawei's one-minute charts
showed no sustained RDS CPU or storage saturation but cannot resolve that
subsecond event. This diagnostic adds measurements only; it does not change
locking, persistence, timeout, retry or admission behavior.

## Evidence to capture

- Each API emits a structured slow_db_phase log when a commit or connection
  return takes at least 100 ms. The event has a UTC log timestamp, phase,
  duration and outcome, without SQL, identifiers, DSNs or request payloads.
  A bounded summary per replica is collected as db-slow-N.json.
- The existing API observer samples commit, pool acquisition, connection
  hold, return and event-loop lag histograms every 0.5 seconds.
- The existing PgBouncer observer samples client waits and server occupancy
  every 0.2 seconds.
- A new observer uses the **direct RDS URL** from the private migration
  service, issuing read-only aggregate queries to pg_stat_activity and
  pg_stat_wal every 0.2 seconds. Raw samples remain in the backend ECS
  tmp/unattended-<run-id>/raw/rds-waits.jsonl; only
  rds-waits-summary.json is copied into public stage evidence. A missing or
  failed RDS observer fails the evidence gate. The observer does not read SQL
  text or connection identifiers.

The direct RDS preflight on 2026-09-20 found that the private migration
container can reach RDS by TCP, but its configured CA file makes the
certificate check fail. A TLS connection with sslmode=require and that CA
setting removed succeeded. The observer alone clears PGSSLROOTCERT inside
its disposable container; application and PgBouncer settings are unchanged.
This diagnostic connection remains encrypted but does **not** verify RDS
identity. It is evidence collection only, not a production TLS validation.
Before any stage, build the API and migration images from the same verified
commit as the backend checkout; a matching Git checkout alone does not prove
the running container contains that revision. Confirm four healthy APIs,
PgBouncer and workers, direct RDS access, a fresh isolated fixture, matching
ECS clocks, and an empty baseline queue. Run the unattended stage's dry-run
first. Keep credentials and the manifest in the existing private paths.

First run a **600 RPS, three-minute instrument rehearsal** to establish normal
phase and observer overhead. If it passes, run one **750 RPS, three-minute
diagnostic** with the same no-retry 95% read / 5% unique-hold workload and
four generator workers. This short run cannot qualify 750 RPS for capacity
sizing, even if it has no errors. Apply the same immediate error, latency,
durability and queue-drain stop gates as a safety stage. Do not run 800 RPS.

## Interpretation

Correlate UTC windows, allowing for observer wake lag and clock skew.
A commit event at the API is a client call to PgBouncer and RDS, not a direct
measurement of WAL flush. Compare its duration with simultaneous API
event-loop lag, pool-return timing, PgBouncer waiting clients/server
occupancy, RDS wait types and changes in WAL sync counters. A sampled RDS
IO or Lock wait is evidence of a wait at sample time; absence cannot
exclude a shorter wait. WAL counters are instance-wide and may include
unrelated traffic. Provider one-minute maxima cannot rule out subsecond
storage latency. Record an explicit hypothesis and uncertainty before
changing a transaction or scaling pattern; write and link an ADR first if
that pattern changes.

A 750 RPS retry for a passing capacity claim requires a fresh 10-minute
safety run followed by a fresh 30-minute confirmation, with zero unexpected
responses, drops and double-bookings, latency within the runbook gates,
exact post-TTL durability and drained queues. The last repeatable clean
30-minute baseline remains 600 RPS.
