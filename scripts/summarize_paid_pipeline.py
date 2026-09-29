"""Summarize a bounded paid-pipeline observer trace without private IDs."""

import argparse
import json
import math
from pathlib import Path


def percentile(values, fraction):
    return sorted(values)[math.ceil(len(values) * fraction) - 1] if values else None


def summarize(rows):
    result = {"samples": len(rows)}
    for field in (
        "pending_orders",
        "paid_unfulfilled",
        "fulfilled_orders",
        "pending_callback_deliveries",
        "pending_payment_attempts",
        "delivered_callbacks",
        "issued_tickets",
        "unpublished_outbox",
        "db_lock_waiters",
    ):
        values = [row[field] for row in rows if field in row]
        result[field] = {
            "max": max(values, default=None),
            "last": values[-1] if values else None,
            "delta": values[-1] - values[0] if values else None,
            "p95": percentile(values, 0.95),
        }
    for role in ("simulator", "consumer", "publisher"):
        first = next((row for row in rows if f"{role}_calls" in row), None)
        last = next((row for row in reversed(rows) if f"{role}_calls" in row), None)
        result[role] = {
            "completed_operations": (
                last[f"{role}_calls"] - first[f"{role}_calls"] if first and last else None
            ),
            "busy_seconds": (
                last[f"{role}_busy_seconds"] - first[f"{role}_busy_seconds"]
                if first and last and f"{role}_busy_seconds" in first and f"{role}_busy_seconds" in last
                else None
            ),
            "max_active": max((row.get(f"{role}_active", 0) for row in rows), default=None),
            "metrics_errors": sum(f"{role}_metrics_error" in row for row in rows),
        }
    result["database_errors"] = sum("database_error" in row for row in rows)
    result["api_metrics_errors"] = sum("api_metrics_error" in row for row in rows)
    replica_samples = {}
    for row in rows:
        for address, metrics in row.get("api_replicas", {}).items():
            replica_samples.setdefault(address, []).append(metrics)
    counters = {}
    pool_peaks = {}
    counter_reset = False
    for samples in replica_samples.values():
        first, last = samples[0], samples[-1]
        for name in set(first) | set(last):
            if name.startswith(("http_503:", "db_503:", "pool_acquire:")):
                delta = last.get(name, 0) - first.get(name, 0)
                if delta < 0:
                    counter_reset = True
                else:
                    counters[name] = counters.get(name, 0) + delta
        for metrics in samples:
            for name, value in metrics.items():
                if name.startswith("pool_state:") or name in ("pool_acquiring", "pool_in_use"):
                    pool_peaks[name] = max(pool_peaks.get(name, 0), value)
    result["api"] = {
        "observed_replicas": len(replica_samples),
        "counter_deltas": counters,
        "pool_peaks_per_replica": pool_peaks,
        "counter_reset_detected": counter_reset,
    }
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("trace", type=Path)
    p.add_argument("--output", required=True, type=Path)
    a = p.parse_args()
    if a.output.exists():
        p.error("Fresh output required")
    rows = [json.loads(line) for line in a.trace.read_text(encoding="utf-8").splitlines() if line]
    result = summarize(rows)
    a.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
