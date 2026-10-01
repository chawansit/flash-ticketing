# DCS primary-failover drill

Date: not yet executed
Status: procedure and tooling only; see [ADR 0060](../../../adr/0060-dcs-primary-failover-validation.md)

## Prerequisites

- Isolated Huawei RDS/DCS topology switched to `RESERVATION_MODE=redis-first` with the
  measured reservation-writer count, per the unattended-stage runbook.
- Dedicated drill seats `DRILL000`-`DRILL{n}` on a fixture event; never reuse load-test
  inventory.
- `TEST_DATABASE_URL`, `TEST_REDIS_URL` (DCS read/write hostname), an API origin and a
  bearer token. Keep credentials out of committed manifests.
- An operator ready to start the managed DCS primary/standby switchover from the console.

## Execution

```sh
python scripts/redis_failover_drill.py \
  --origin https://<api-host> \
  --token <token> \
  --event-id <fixture-event-id> \
  --commands 50 \
  --output docs/capacity/huawei-rds/<date>-dcs-failover/drill.json
```

When the script prints `FAILOVER WINDOW`, start the switchover. The script probes intake and
replication once per second, detects promotion, replays the unknown-outcome probe key once,
and audits every acknowledged pre-failover command for PostgreSQL durability, linkage,
ownership overlap and stream drain. It then proves fresh post-failover intake persists with
replica acknowledgement on the new master.

## Verdict

The drill exits non-zero unless every ADR 0060 gate passes. Append the passing evidence to
ADR 0060 and this directory before claiming the primary-loss portion of the Redis-first
activation gates. The 30-minute sustained confirmation remains a separate stage.
