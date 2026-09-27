#!/usr/bin/env python3
"""Summarize Kafka and database refresh flow without retaining identifiers."""

import argparse
import json
from datetime import datetime
from pathlib import Path


def _delta_rate(rows, field):
    measured = [row for row in rows if field in row]
    if len(measured) < 2:
        return {"delta": None, "per_second": None}
    seconds = (
        datetime.fromisoformat(measured[-1]["utc"])
        - datetime.fromisoformat(measured[0]["utc"])
    ).total_seconds()
    delta = max(0, int(measured[-1][field]) - int(measured[0][field]))
    return {"delta": delta, "per_second": delta / seconds if seconds > 0 else None}


def summarize(backend_rows, kafka_rows):
    backend = [
        row for row in backend_rows if isinstance(row, dict) and "error" not in row
    ]
    kafka = [
        row for row in kafka_rows if isinstance(row, dict) and "error_type" not in row
    ]
    if not backend or not kafka:
        raise ValueError("Backend and Kafka traces both need valid samples")
    peak = max(kafka, key=lambda row: int(row.get("total_lag", 0)))
    after_peak = [row for row in kafka if row["utc"] > peak["utc"]]
    drained = next(
        (row for row in after_peak if int(row.get("total_lag", 0)) == 0),
        None,
    )
    drain_seconds = (
        (
            datetime.fromisoformat(drained["utc"])
            - datetime.fromisoformat(peak["utc"])
        ).total_seconds()
        if drained
        else None
    )
    return {
        "backend_samples": len(backend),
        "backend_sample_errors": len(backend_rows) - len(backend),
        "kafka_samples": len(kafka),
        "kafka_sample_errors": len(kafka_rows) - len(kafka),
        "kafka_total_lag": {
            "max": int(peak.get("total_lag", 0)),
            "peak_utc": peak["utc"],
            "last": int(kafka[-1].get("total_lag", 0)),
        },
        "kafka_max_partition_lag": max(
            int(row.get("max_partition_lag", 0)) for row in kafka
        ),
        "kafka_drain_after_peak_seconds": drain_seconds,
        "kafka_members": max(int(row.get("members", 0)) for row in kafka),
        "refresh_generated": _delta_rate(backend, "refresh_generation_total"),
        "refresh_completed": _delta_rate(
            backend, "refresh_completed_generation_total"
        ),
        "outbox_rows_inserted": _delta_rate(backend, "outbox_rows_inserted"),
        "consumer_inbox_rows_inserted": _delta_rate(
            backend, "consumer_inbox_rows_inserted"
        ),
        "pending_refresh_last": int(backend[-1].get("pending_refresh", 0)),
        "pending_refresh_max": max(
            int(row.get("pending_refresh", 0)) for row in backend
        ),
        "oldest_refresh_seconds_max": max(
            float(row.get("oldest_refresh_seconds", 0)) for row in backend
        ),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("backend", type=Path)
    parser.add_argument("kafka", type=Path)
    args = parser.parse_args()
    backend = json.loads(args.backend.read_text(encoding="utf-8"))
    kafka = [
        json.loads(line)
        for line in args.kafka.read_text(encoding="utf-8").splitlines()
        if line
    ]
    print(json.dumps(summarize(backend, kafka), sort_keys=True))


if __name__ == "__main__":
    main()
