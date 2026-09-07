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
