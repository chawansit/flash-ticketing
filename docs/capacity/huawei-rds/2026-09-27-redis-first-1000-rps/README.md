# Redis-first 1,000 RPS safety validation

Date: 27 September 2026  
Revision: `8c687c9`  
Result: **strict five-minute safety stage passed**

## Final topology and workload

- four API replicas, three application DB connections per API and admission four per API;
- Huawei DCS Redis-first intake with one replica acknowledgement;
- two reservation persistence writers;
- two Kafka consumers across the existing six partitions;
- one dedicated refresh worker and one batched expiry worker;
- eight generator processes on the separate 8-vCPU generator ECS;
- Huawei RDS PostgreSQL behind PgBouncer with verified TLS;
- 1,000 RPS for 300 seconds, 95% conditional seat-map reads and 5% unique holds;
- no client retry and a fixed 180-second post-load audit.

The generator delivered all 300,000 scheduled requests. There were zero generator drops, late
deliveries, first-attempt failures, retry attempts and transport errors. Worst-worker read p95
was 103.682 ms and hold p95 was 235.254 ms, inside the 150/300 ms gates. Start skew was
8.92 ms and all eight workers exited successfully.

Every one of the 15,000 HTTP 202 provisional holds had exactly one PostgreSQL idempotency row,
hold, order and `DURABLE` reservation command. Broken links, active/overdue holds, pending
orders and overlapping seat intervals were zero. Unpublished outbox events, pending refreshes,
dead letters, Redis reservation stream entries and consumer-group pending deliveries were all
zero at the fixed audit.

Kafka lag peaked at 118 total and 34 on one partition and returned to zero. Refresh generation
and completion both increased by 30,000. Pending refresh peaked at 539, oldest refresh age at
15.886 seconds, overdue holds at 33 and oldest overdue age at 0.261 seconds; every value returned
to zero. The RDS observer completed 5,162 samples with no sample errors and observed WAL-related
waits without a request failure.

Rollback restored PostgreSQL reservation mode, zero reservation writers, one combined
maintenance worker, no dedicated refresh/expiry workers, one Kafka consumer and admission four.
Cleanup removed private manifests.

## Diagnostic sequence

The measured sequence kept failures visible rather than relaxing the gate:

1. One writer accepted all 15,000 provisional holds but persisted only 9,604 before the
   120-second leases expired; 5,396 commands ended as `HOLD_EXPIRED`. This isolated writer
   throughput at roughly 32 commands per second below the offered 50 holds per second.
2. Two writers made every delivered command durable, but four generator processes dropped
   6,208 schedules. That run failed load fidelity.
3. Eight generators delivered all requests and two writers made all 15,000 commands durable.
   The combined maintenance topology left 80 pending refresh rows at the fixed audit. A later
   exact re-audit found zero, but the original strict verdict remained failed.
4. The final run retained two writers and eight generators and used the already accepted
   two-consumer, split refresh/expiry topology. Every strict gate passed.

## Evidence

Final passing evidence:

- [stage verdict](stage-result.json)
- [load summary](load-summary.json)
- [durability audit](durability.json)
- [deployment](deployment.json)
- [admission](admission.json)
- [drain trace](drain-trace-summary.json)
- [refresh pipeline](refresh-pipeline-summary.json)
- [RDS waits](rds-waits-summary.json)
- [rollback](rollback.json)

The compact failed-stage summaries are retained beside this report with `one-writer`,
`two-writer-4gen` and `two-writer-8gen-combined` prefixes. The combined-topology follow-up audit
is also retained to show eventual drain without changing its failed fixed-window verdict.
Raw high-volume telemetry, fixture identifiers, credentials and private manifests are excluded.

## Scope

This passes the isolated five-minute 1,000 RPS safety gate for the measured topology. It is not
a sustained production capacity certification. DCS primary-failover recovery, an intentional
reservation-writer restart with pending commands, and a longer confirmation remain required
before production activation or a per-instance capacity commitment.