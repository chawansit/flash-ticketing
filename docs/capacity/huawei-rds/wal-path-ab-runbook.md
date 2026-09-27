# Isolated WAL path A/B probe

Purpose: determine whether intermittent client-observed commit spikes occur on
both the direct RDS path and the PgBouncer path when the SQL, write volume,
connection count and offered rate are held constant. This is a diagnostic,
not a capacity qualification or a replacement for the ticketing workload.

Run `scripts/wal_path_probe.py` only on the isolated Huawei test RDS with the
existing pooled `DATABASE_URL` and direct `TEST_DATABASE_URL`. Start with
`--preflight`: it checks both paths reach the same database and role with
`synchronous_commit=on`, `fsync=on` and read-write transactions. It prints no
DSN. A disposable direct connection uses encrypted `sslmode=require` because
the current private CA configuration does not validate the RDS endpoint; this
exception is limited to the diagnostic, not the application deployment.

The live probe uses a uniquely named logged table in `public`, 16 connections,
40 inserts/second, and four 90-second blocks in direct–pooled–pooled–direct
order. Each insert has an incompressible 8 KiB payload. It measures SQL and
commit latency separately, records schedule/connection wait, and fails if the
rate drifts or any transaction errors. It creates the table only after preflight
and drops that exact table in `finally`. A private recovery marker records the
table name before creation so an interrupted container can be cleaned up.
Use a fresh ignored output path. Keep the raw transaction rows and DSNs out of
the repository; publish only compact, redacted summaries. Observe
`pg_stat_activity`, `pg_stat_wal` and `pg_stat_checkpointer` at 100 ms during
all blocks, with the same direct RDS observer used for the capacity stages.
Do not overlap with a capacity stage, backup or failover exercise. Verify the
API remains ready and no load generator job is active first.

Interpretation: repeated spikes on both paths make PgBouncer less likely to
be the initiator and justify provider-side WAL/storage investigation. Spikes
only on the pooled path favor PgBouncer or its network/connection behavior.
A clean synthetic probe does **not** exonerate RDS or prove the application is
at fault: the hold transaction has a different statement mix. Compare the
probe's WAL bytes, wait events, checkpoints and rate with the real 750 RPS
hold stage before acting. Do not weaken synchronous durability or change WAL
settings based on this A/B probe alone. Any persistence or scaling pattern
change requires an ADR first.