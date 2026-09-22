# 1,000 RPS maintenance-drain diagnostic, 22 September 2026

**Verdict: not a validated 1,000 RPS capacity point.** The 15-minute stage met request-latency, load-fidelity, durability and double-booking checks, but had ten final admission errors and 237 pending seat-refresh requests at the fixed 180-second post-load audit. The strict capacity and the candidate recovery-SLO diagnostic therefore both failed. We stopped before 30 minutes.

The measured topology used four API replicas, hold admission five per API during load, PgBouncer pool 24, two maintenance replicas during load, and eight generator workers. Admission and maintenance were restored to four and one after each stage. The workload was 95% conditional seat-map reads and 5% unique seat holds, with a bounded second attempt using the same idempotency key. It does not represent all-hold demand.

| Stage | Scheduled | Generator drops | First-attempt admission errors | Recovered retries | Final errors | Read / hold p95, worst worker | Durable acknowledged holds | Held-seat overlaps | Fixed post-load audit |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- |
| 5 min, 8 generators | 300,000 | 0 | 65 | 55 | 10 | 20.98 / 98.39 ms | 14,990 | 0 | All queues zero |
| 15 min, 8 generators | 900,000 | 0 | 20 | 10 | 10 | 13.33 / 78.03 ms | 44,990 | 0 | 237 pending refresh; outbox and dead letters zero |

The final request-error fraction in the 15-minute diagnostic was 10/900,000 = **0.00111%**, below the experimental 0.01% request budget. That budget does not apply to correctness or queue drain. All acknowledged holds matched idempotency records and orders; no active hold was overdue at the audit. The two-worker run saw at most 54 overdue holds with an oldest age of 0.994 seconds, compared with 6,425 and 128.5 seconds in the earlier one-worker 1,000 RPS trace. Refresh work remains the failed gate. A later read-only check found all queues at zero, but does not change the timed verdict.

The initial two-maintenance-worker five-minute attempt used four generator workers. It dropped 584 scheduled requests because the generator was late, so we repeated the safety stage with eight workers at the same total offered RPS. The eight-worker run had no generator drop or late delivery. Sampled RDS waits included WAL sync/write; WAL timing was disabled, so these samples cannot establish device WAL latency or prove it caused every admission error.

Evidence: [five-minute stage](safety8-stage-result.json), [five-minute drain summary](safety8-drain-summary.json), [15-minute stage](diagnostic15-stage-result.json), [15-minute drain summary](diagnostic15-drain-summary.json), [durability audit](diagnostic15-durability.json), and [rollback verification](diagnostic15-rollback.json). The ADRs are [maintenance scaling](../../../adr/0050-scale-maintenance-expiry-drain.md) and [recovery-SLO diagnostic](../../../adr/0051-1000-rps-recovery-slo-diagnostic.md). Private manifests, credentials and raw telemetry are excluded.
