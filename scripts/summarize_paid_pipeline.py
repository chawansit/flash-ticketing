"""Summarize a bounded paid-pipeline observer trace without private IDs."""

import argparse
import json
import math
from itertools import pairwise
from pathlib import Path


def percentile(values, fraction):
    return sorted(values)[math.ceil(len(values) * fraction) - 1] if values else None


def summarize_consumer_phases(rows):
    samples = [row["consumer_phases"] for row in rows if "consumer_phases" in row]
    if not samples:
        return {"observed": False, "phases": {}, "counter_reset_detected": False}
    first, last = samples[0], samples[-1]
    reset = any(current.get(key, 0) < value
                for previous, current in pairwise(samples)
                for key, value in previous.items())
    if reset:
        return {"observed": True, "phases": {}, "counter_reset_detected": True,
                "note": "Aggregate phase counters decreased; duration/quantile attribution is invalid."}
    deltas = {}
    for key in set(first) | set(last):
        delta = last.get(key, 0) - first.get(key, 0)
        if delta < 0:
            reset = True
        else:
            deltas[key] = delta
    phases = {}
    for key, count in deltas.items():
        if not key.endswith(":count") or count <= 0:
            continue
        prefix = key.removesuffix(":count")
        total = deltas.get(prefix + ":sum")
        if total is None:
            continue
        buckets = sorted((float(k.removeprefix(prefix + ":bucket:")), value)
                         for k, value in deltas.items() if k.startswith(prefix + ":bucket:"))
        bound = next((bound for bound, value in buckets if value >= .95 * count), None)
        phases[prefix] = {
            "calls": count, "total_seconds": total, "mean_ms": 1000 * total / count,
            "p95_upper_bound_ms": 1000 * bound if bound is not None and math.isfinite(bound) else None,
        }
    return {"observed": True, "phases": phases, "counter_reset_detected": reset,
            "note": "Nested phases cannot be added. Histogram p95 is an upper bucket bound, not an exact quantile. Poll wall time includes idle waits."}


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
            "max_replicas": max((row.get(f"{role}_replicas", 0) for row in rows), default=None),
            "metrics_errors": sum(f"{role}_metrics_error" in row for row in rows),
        }
    result["consumer_phases"] = summarize_consumer_phases(rows)
    result["database_errors"] = sum("database_error" in row for row in rows)
    result["api_metrics_errors"] = sum("api_metrics_error" in row for row in rows)
    replica_samples = {}
    for row in rows:
        for address, metrics in row.get("api_replicas", {}).items():
            replica_samples.setdefault(address, []).append(metrics)
    counters = {}
    pool_peaks = {}
    counter_reset = False
    event_loop_lag_peak = 0.0
    for samples in replica_samples.values():
        first, last = samples[0], samples[-1]
        for name in set(first) | set(last):
            if name.startswith(("http_503:", "db_503:", "pool_acquire:", "order_cache:", "duration:")):
                delta = last.get(name, 0) - first.get(name, 0)
                if delta < 0:
                    counter_reset = True
                else:
                    counters[name] = counters.get(name, 0) + delta
        for metrics in samples:
            for name, value in metrics.items():
                if name.startswith("pool_state:") or name in ("pool_acquiring", "pool_in_use"):
                    pool_peaks[name] = max(pool_peaks.get(name, 0), value)
                if name == "event_loop_lag_current":
                    event_loop_lag_peak = max(event_loop_lag_peak, value)
    durations = {}
    for key, count in counters.items():
        if not key.startswith("duration:") or not key.endswith(":count") or count <= 0:
            continue
        prefix = key.rsplit(":", 1)[0]
        total = counters.get(prefix + ":sum")
        if total is not None:
            durations[prefix.removeprefix("duration:")] = 1000 * total / count
    result["api"] = {
        "observed_replicas": len(replica_samples),
        "counter_deltas": {key: value for key, value in counters.items() if not key.startswith("duration:")},
        "duration_mean_ms": durations,
        "event_loop_lag_current_peak_ms": 1000 * event_loop_lag_peak,
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
