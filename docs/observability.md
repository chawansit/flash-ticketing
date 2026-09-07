# Database and worker performance measurements

The API `/metrics` and each worker's port 9101 expose Prometheus metrics. Compose
Prometheus is at http://localhost:9090 and scrapes every five seconds. Metrics have
bounded command/operation/outcome labels; SQL text and customer identifiers are excluded.

| Metric | Meaning |
|---|---|
| `ticketing_db_query_seconds{command}` | Client execute duration including network and PgBouncer; excludes later fetch decoding. Internal transaction-control calls are covered by transaction duration instead. |
| `ticketing_db_transaction_seconds` | Transaction wall time including commit/rollback, excluding application pool acquisition. |
| `ticketing_db_pool_acquire_seconds{outcome}` | Application pool acquisition duration, including timeout/error paths. |
| `ticketing_db_pool_acquiring`, `ticketing_db_pool_in_use` | Threads acquiring and connections checked out, per process. |
| `ticketing_db_pool_state{state}` | Last observed pool size, available connections, waiting requests and configured maximum. |
| `ticketing_db_errors_total{type}` | SQLSTATE or pool failures; 55P03 includes NOWAIT/lock-timeout failures. |
| `ticketing_worker_busy_seconds_total{operation}` | Accumulated operation wall time, including I/O and empty-work checks. |
| `ticketing_worker_active{operation}` | Concurrent operation calls. |
| `ticketing_worker_operations_total{operation,outcome}` | Calls completed successfully or with an exception. |
| `ticketing_cache_rows_total{mode}` | Rows submitted by full snapshots and incremental patches, including retries. |
| `ticketing_reconciliation_backlog` | Active events already past their reconciliation deadline. Saturates at 10,000 rather than scanning unbounded rows. |
| `ticketing_reconciliation_overdue_seconds` | Age of the oldest passed deadline. Zero when nothing is due. Compare against the 30-second seat-map TTL. |
| `ticketing_reconciliation_tracked_events` | Events currently held in the schedule, i.e. inside their sale window. Saturates at 50,000. |
| `ticketing_reconciliation_events_total{outcome}` | Scheduled reconciliations completed (`ok`) or failed (`error`). |
| `ticketing_reconciliation_failures_total{stage}` | Failures by stage: `snapshot`, `defer`, `acknowledge`, `schedule`. |
| `ticketing_reconciliation_seconds{outcome}` | Per-event reconciliation wall time including Redis I/O. |
| `ticketing_reconciliation_recovered_leases_total` | Expired reconciliation leases reclaimed from crashed or stalled workers. |
| `process_cpu_seconds_total` | Actual process CPU consumption, separate from I/O-inclusive busy time. |

Useful PromQL:

```promql
histogram_quantile(0.95, sum by (le, command) (rate(ticketing_db_query_seconds_bucket{job="api"}[5m])))
histogram_quantile(0.95, sum by (le) (rate(ticketing_db_transaction_seconds_bucket{job="api"}[5m])))
histogram_quantile(0.95, sum by (le, outcome) (rate(ticketing_db_pool_acquire_seconds_bucket{job="api"}[5m])))
sum by (instance) (ticketing_db_pool_in_use)
sum by (type) (rate(ticketing_db_errors_total[5m]))
rate(ticketing_worker_busy_seconds_total{operation="consume_event"}[5m])
rate(ticketing_worker_busy_seconds_total{operation="simulate_one"}[5m]) / 4
ticketing_reconciliation_overdue_seconds
histogram_quantile(0.95, sum by (le, outcome) (rate(ticketing_reconciliation_seconds_bucket[5m])))
sum by (stage) (rate(ticketing_reconciliation_failures_total[5m]))
rate(ticketing_reconciliation_events_total{outcome="ok"}[5m])
rate(process_cpu_seconds_total[5m])
sum by (mode) (rate(ticketing_cache_rows_total[5m]))
```

Busy time is not CPU usage. `snapshot` can be nested inside `refresh_one`, so do not
sum those durations into utilization. The simulator has four slots by default; adjust
the divisor if configuration changes. Empty polling queries also consume busy time.

The capacity probe samples `pg_stat_activity` and `pg_blocking_pids` every approximately
two seconds: connections, active sessions, lock waiters, blocked sessions and age of
the oldest currently waiting query. Query age is an upper bound on its lock wait,
not an exact lock-wait duration. Brief waits can be missed; correlate SQLSTATE counters.
Historical raw samples called that age `oldest_lock_wait_seconds`; the summary labels
it correctly. The development database user can see all sessions. Restricted production
observers need monitoring visibility such as pg_read_all_stats, without write privileges.

Database activity covers the current database, including other fixtures. Prometheus
metrics cover all work per process. PgBouncer queuing is included in execute duration
but is not separately timed. No pg_stat_statements or SQL-plan profiling is enabled.

Run increasing offered rates at fixed inventory with `scripts/capacity_test.py`.
Its results retain scraper timestamps, histogram buckets and observer errors.
`scripts/summarize_capacity.py docs/capacity/incremental` reproduces the summary.
Histogram summary p95 values are bucket upper bounds, not exact percentiles. Missing
samples cannot establish zero contention. Generator drops and overload responses
invalidate a claim that the service sustained the offered rate.

Integration clarification: reconciliation `ok` counts successful token-fenced SQL
acknowledgements. `stale` and `ack_error` are separate outcomes. A deadline is checked
before each snapshot, with unstarted leases released; in-flight I/O is not preempted.

## Sustained read/expiry observations

`scripts/seatmap_load.py --modes conditional --observe` samples the probe's own shows
approximately every two seconds. It records actual Redis TTLs (including missing keys),
expired-hold cleanup counts, overdue ACTIVE holds and their oldest expiry age, dirty
projection rows, and age since the last acknowledged reconciliation. These SQL and
Redis reads add measurement overhead. TTL samples can miss brief gaps between samples;
HTTP 503/error counts are complementary evidence. Reconciliation age is not a substitute
for actual cache presence. `never_reconciled` counts existing schedule rows with no
acknowledgement, not events absent from the schedule. A positive HTTP gate is separate
from these recovery observations. Use a duration longer than the configured hold TTL
to exercise expiry during traffic, and keep failed runs.
