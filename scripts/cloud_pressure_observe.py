"""Capture sub-second, per-replica admission and DB-pool pressure metrics."""

from __future__ import annotations

import argparse
import json
import math
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

METRIC_PREFIXES = (
    "ticketing_hold_inflight",
    "ticketing_hold_limit",
    "ticketing_hold_admission_total",
    "ticketing_hold_arrival_occupancy_",
    "ticketing_db_pool_acquiring",
    "ticketing_db_pool_in_use",
    "ticketing_db_pool_state",
    "ticketing_db_pool_acquire_seconds_",
)


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    return sorted(values)[math.ceil(len(values) * fraction) - 1]


def parse_metrics(text: str) -> dict[str, float]:
    metrics: dict[str, float] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or " " not in line:
            continue
        metric, raw_value = line.rsplit(None, 1)
        name = metric.split("{", 1)[0]
        if not name.startswith(METRIC_PREFIXES):
            continue
        try:
            metrics[metric] = float(raw_value)
        except ValueError:
            continue
    return metrics


def scrape(url: str) -> dict[str, float]:
    request = Request(url, headers={"Accept": "text/plain; version=0.0.4"})
    with urlopen(request, timeout=1) as response:
        return parse_metrics(response.read().decode("utf-8"))


def metric_value(metrics: dict[str, float], name: str, labels: str = "") -> float:
    return metrics.get(f"{name}{labels}", 0.0)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", action="append", required=True, help="Direct /metrics URL; repeat per replica")
    parser.add_argument("--seconds", type=float, required=True)
    parser.add_argument("--interval", type=float, default=0.2)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 7200:
        parser.error("Use 1..7200 seconds")
    if not 0.1 <= args.interval <= 10:
        parser.error("Use a 0.1..10 second interval")
    if args.output.exists():
        parser.error("Use a fresh output file")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    due = start
    end = start + args.seconds
    samples = 0
    errors = 0
    wake_lags: list[float] = []
    first: dict[str, dict[str, float]] = {}
    last: dict[str, dict[str, float]] = {}
    peaks: dict[str, dict[str, float]] = {}

    with args.output.open("x", encoding="utf-8") as output:
        output.write(json.dumps({
            "type": "metadata",
            "utc": datetime.now(UTC).isoformat(),
            "seconds": args.seconds,
            "interval_seconds": args.interval,
            "replica_count": len(args.url),
        }, separators=(",", ":")) + "\n")
        output.flush()

        while time.monotonic() < end:
            now = time.monotonic()
            lag = max(0.0, (now - due) * 1000)
            wake_lags.append(lag)
            row: dict[str, Any] = {
                "type": "sample",
                "utc": datetime.now(UTC).isoformat(),
                "elapsed_seconds": round(now - start, 6),
                "observer_wake_lag_ms": round(lag, 3),
                "replicas": {},
            }
            for index, url in enumerate(args.url):
                replica = f"api_{index}"
                try:
                    metrics = scrape(url)
                    row["replicas"][replica] = metrics
                    first.setdefault(replica, metrics)
                    last[replica] = metrics
                    peak = peaks.setdefault(replica, {})
                    peak["hold_inflight"] = max(
                        peak.get("hold_inflight", 0.0), metric_value(metrics, "ticketing_hold_inflight")
                    )
                    peak["pool_acquiring"] = max(
                        peak.get("pool_acquiring", 0.0), metric_value(metrics, "ticketing_db_pool_acquiring")
                    )
                    peak["pool_in_use"] = max(
                        peak.get("pool_in_use", 0.0), metric_value(metrics, "ticketing_db_pool_in_use")
                    )
                    peak["pool_requests_waiting"] = max(
                        peak.get("pool_requests_waiting", 0.0),
                        metric_value(metrics, "ticketing_db_pool_state", '{state="requests_waiting"}'),
                    )
                except Exception as exc:  # noqa: BLE001
                    row["replicas"][replica] = {"error": type(exc).__name__}
                    errors += 1
            output.write(json.dumps(row, separators=(",", ":")) + "\n")
            output.flush()
            samples += 1
            due += args.interval
            time.sleep(max(0.0, due - time.monotonic()))

        deltas = {}
        for replica in sorted(last):
            start_metrics = first[replica]
            end_metrics = last[replica]
            deltas[replica] = {
                "admitted": max(
                    0.0,
                    metric_value(end_metrics, "ticketing_hold_admission_total", '{outcome="admitted"}')
                    - metric_value(start_metrics, "ticketing_hold_admission_total", '{outcome="admitted"}'),
                ),
                "rejected": max(
                    0.0,
                    metric_value(end_metrics, "ticketing_hold_admission_total", '{outcome="rejected"}')
                    - metric_value(start_metrics, "ticketing_hold_admission_total", '{outcome="rejected"}'),
                ),
                "pool_acquire_count": max(
                    0.0,
                    metric_value(end_metrics, "ticketing_db_pool_acquire_seconds_count", '{outcome="ok"}')
                    - metric_value(start_metrics, "ticketing_db_pool_acquire_seconds_count", '{outcome="ok"}'),
                ),
                "pool_acquire_seconds": max(
                    0.0,
                    metric_value(end_metrics, "ticketing_db_pool_acquire_seconds_sum", '{outcome="ok"}')
                    - metric_value(start_metrics, "ticketing_db_pool_acquire_seconds_sum", '{outcome="ok"}'),
                ),
            }
        summary = {
            "type": "summary",
            "utc": datetime.now(UTC).isoformat(),
            "samples": samples,
            "scrape_errors": errors,
            "observer_wake_lag_p95_ms": percentile(wake_lags, 0.95),
            "observer_wake_lag_p99_ms": percentile(wake_lags, 0.99),
            "observer_wake_lag_max_ms": max(wake_lags, default=None),
            "peaks": peaks,
            "counter_deltas": deltas,
        }
        output.write(json.dumps(summary, separators=(",", ":")) + "\n")
        output.flush()
    print(json.dumps(summary))
    return 0 if errors == 0 and len(last) == len(args.url) else 1


if __name__ == "__main__":
    raise SystemExit(main())
