# Hold rejection diagnostics

ADR: [0015](../adr/0015-hold-path-diagnostics.md).

This change adds measurement; it does not raise the admission limit or claim higher
capacity. Preserve the same deployment and workload when collecting the next baseline.

## Correlate a rejected hold

The HTTP generator now includes `error_codes` and at most 20 `error_examples` per
worker. Each example has operation, HTTP status, an allowlisted code and a validated
UUID request ID. Use that ID to locate the existing structured API request log.
No request bodies, JWTs, idempotency keys, actors or seat IDs are retained in examples.
Unknown or malformed codes become `OTHER`.

For admission rejection, expect `ADMISSION_FULL`, an increment of
`ticketing_hold_admission_total{outcome="rejected"}` and
`ticketing_outcomes_total{operation="request",outcome="ADMISSION_FULL"}`.
Structured logs now include `error_code`, `hold_arrival_occupancy`, `hold_limit`
and `hold_phase_ms`. Other handled failures retain their own error code.

## Measure the hold path

- `ticketing_hold_inflight`: currently admitted requests, per API process.
- `ticketing_hold_limit`: configured per-process maximum, refreshed at hold arrival.
- `ticketing_hold_arrival_occupancy`: histogram recording occupancy at *every* hold
  arrival, so short bursts remain visible between scrapes.
- `ticketing_hold_phase_seconds{phase,outcome}`: fixed phase names and `ok/error`.

| Phase | Meaning |
|---|---|
| dispatch | Admission to endpoint entry, including parsing/auth/dependencies/thread scheduling |
| rate_limit | Redis rate-limit operation |
| redis_enter | Redis shield acquisition |
| db_enter | Pool acquisition, transaction entry and local timeout setup |
| database_body | SQL/business work inside the transaction, excluding entry/commit |
| db_exit | Transaction commit or rollback and pool return |
| redis_exit | Shield release, including the existing suppressed Redis-error behavior |

`ok` means that phase returned normally; a successful rollback is not a successful
booking. Failed entry records its duration but does not execute an exit. Validation
or authentication failures may have no endpoint-phase data. The existing DB query,
pool and transaction metrics remain available for finer aggregate attribution.
Phase timings include I/O, not just CPU, and are not a complete decomposition of
HTTP time (response serialization and middleware return work are outside them).

For Prometheus, retain `instance` when inspecting peaks or percentiles. Do not add
process-local gauges together and compare them to one process's limit. An example:

```promql
sum by (instance, outcome) (increase(ticketing_hold_admission_total[5m]))
histogram_quantile(0.95, sum by (instance, phase, le) (rate(ticketing_hold_phase_seconds_bucket[5m])))
```

`scripts/export_cloud_metrics.py` includes these new hold series. Keep raw worker
results, server metrics and timestamped request logs from the same measured window.
A zero-error result still requires zero drops and unexpected responses, complete
accounting, durable-record verification and contention/payment correctness checks.
Do not increase concurrency solely because the average pool wait is small: inspect
the rejected request's time window and admitted slow requests first.

## Validation

Rebuilt local Docker Compose suite: **78 passed**, no skips, with two dependency
deprecation warnings. Unit coverage includes failure/cancellation cleanup and
error redaction. Real API logs confirm all seven phases for a successful hold and
ADMISSION_FULL at occupancy 8/8. [Validation evidence](hold-diagnostics-validation.json).
No new cloud capacity result exists yet; this change is ready for the diagnostic
rerun on the same cloud machines before any concurrency tuning.

Cloud follow-up: [352-RPS diagnostic rerun](huawei-diagnostics/README.md) completed
30 minutes. All holds succeeded, but 160 SEATMAP_WARMING read failures kept the
zero-error gate failed. Nine diagnostic/end-to-end tests passed on cloud.
