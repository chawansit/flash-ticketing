"""Summarize retained load probes without treating histogram estimates as exact percentiles."""

import argparse
import gzip
import json
import re
from pathlib import Path


def series(items):
    if not isinstance(items, list):
        return {}
    return {tuple(sorted(row["metric"].items())): float(row["value"][1]) for row in items}


def summarize(path):
    raw = gzip.decompress(path.read_bytes()).decode() if path.suffix == ".gz" else path.read_text()
    run = json.loads(raw)
    start, end = series(run.get("metrics_start")), series(run.get("metrics_end"))
    delta = [
        (dict(key), value - start.get(key, 0)) for key, value in end.items() if value >= start.get(key, 0)
    ]

    def total(name, **labels):
        return sum(
            value
            for key, value in delta
            if key.get("__name__") == name and all(key.get(k) == v for k, v in labels.items())
        )

    def upper_p95(prefix, **labels):
        count = total(prefix + "_count", **labels)
        if not count:
            return None
        buckets = {}
        for key, value in delta:
            if key.get("__name__") == prefix + "_bucket" and all(key.get(k) == v for k, v in labels.items()):
                boundary = float(key["le"])
                buckets[boundary] = buckets.get(boundary, 0) + value
        return next(
            (boundary * 1000 for boundary, value in sorted(buckets.items()) if value >= count * 0.95), None
        )

    activity = [s["db_activity"] for s in run["backlog_samples"] if "db_activity" in s]
    gauges = [
        row for sample in run["backlog_samples"] for row in sample.get("metrics", []) if isinstance(row, dict)
    ]

    def maximum(name, **labels):
        return max(
            (
                float(row["value"][1])
                for row in gauges
                if row["metric"].get("__name__") == name
                and all(row["metric"].get(k) == v for k, v in labels.items())
            ),
            default=None,
        )

    query_count = total("ticketing_db_query_seconds_count")
    return {
        "file": path.name,
        "rate": run["target_journeys_per_second"],
        "inventory": run.get("inventory_seats"),
        "statuses": run["statuses"],
        "generator_drops": run["generator_dropped"],
        "latency_ms": run["latency_ms_by_operation_status"],
        "projection_drained": run["projection_drained"],
        "drain_seconds": run["drain_seconds"],
        "correctness_pass": run["correctness_pass"],
        "db_max_connections": max((s["connections"] for s in activity), default=None),
        "db_max_lock_waiters": max((s["lock_waiters"] for s in activity), default=None),
        "db_max_oldest_waiting_query_seconds": max(
            (s.get("oldest_waiting_query_seconds", s.get("oldest_lock_wait_seconds", 0)) for s in activity),
            default=None,
        ),
        "api_pool_max_size": maximum("ticketing_db_pool_state", job="api", state="pool_size"),
        "api_pool_max_acquiring": maximum("ticketing_db_pool_acquiring", job="api"),
        "api_pool_max_in_use": maximum("ticketing_db_pool_in_use", job="api"),
        "query_count": query_count,
        "query_mean_ms": total("ticketing_db_query_seconds_sum") * 1000 / query_count
        if query_count
        else None,
        "api_query_p95_bucket_upper_ms": upper_p95("ticketing_db_query_seconds", job="api"),
        "api_transaction_p95_bucket_upper_ms": upper_p95("ticketing_db_transaction_seconds", job="api"),
        "api_pool_p95_bucket_upper_ms": upper_p95(
            "ticketing_db_pool_acquire_seconds", job="api", outcome="ok"
        ),
        "pool_acquisition_errors": total("ticketing_db_pool_acquire_seconds_count", outcome="error"),
        "sql_lock_errors": total("ticketing_db_errors_total", type="55P03"),
        "cache_full_rows": total("ticketing_cache_rows_total", mode="full"),
        "cache_patch_rows": total("ticketing_cache_rows_total", mode="patch"),
        "worker_busy_seconds": {
            op: total("ticketing_worker_busy_seconds_total", operation=op)
            for op in ["snapshot", "refresh_one", "publish_batch", "consume_event", "simulate_one"]
        },
        "metric_note": "Counters cover scrape-aligned start through drain; p95 is bucket upper bound, not exact. Worker operations can nest. Samples can miss waits.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    paths = sorted(args.directory.glob("*-*.json")) + sorted(args.directory.glob("*-*.json.gz"))
    print(
        json.dumps(
            [
                summarize(p)
                for p in paths
                if re.fullmatch(r"(?:before|after|final)-[0-9]+\.json(?:\.gz)?", p.name)
            ],
            indent=2,
        )
    )
