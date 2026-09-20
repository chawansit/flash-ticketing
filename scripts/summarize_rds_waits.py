#!/usr/bin/env python3
"""Reduce private RDS wait samples to a bounded, identifier-free summary."""

import json
import sys
from collections import Counter
from pathlib import Path

IGNORED_WAITS = frozenset({"CPU", "Client"})


def summarize(lines) -> dict:
    samples = errors = 0
    max_query_ms = max_wake_lag_ms = max_wal_sync_delta_ms = 0.0
    max_interesting_waiters = 0
    wait_type_samples = Counter()
    wait_event_samples = Counter()
    wal_timing_enabled = None
    events = []
    previous_wal_sync_ms = None
    for line in lines:
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if not isinstance(row, dict):
            continue
        if row.get("type") == "metadata":
            wal_timing_enabled = row.get("wal_timing_enabled")
            continue
        if row.get("type") != "sample":
            continue
        samples += 1
        if row.get("error_type"):
            errors += 1
            continue
        query_ms = float(row.get("query_ms", 0))
        wake_lag_ms = float(row.get("observer_wake_lag_ms", 0))
        max_query_ms = max(max_query_ms, query_ms)
        max_wake_lag_ms = max(max_wake_lag_ms, wake_lag_ms)
        waits = row.get("activity", {}).get("wait_types", {})
        interesting = sum(
            int(count) for kind, count in waits.items() if kind not in IGNORED_WAITS
        )
        max_interesting_waiters = max(max_interesting_waiters, interesting)
        for kind, count in waits.items():
            if kind not in IGNORED_WAITS and int(count) > 0:
                wait_type_samples[kind] += 1
        wait_events = row.get("activity", {}).get("wait_events", {})
        for event, count in wait_events.items():
            if int(count) > 0:
                wait_event_samples[event] += 1
        current_wal_sync_ms = float(row.get("wal", {}).get("sync_ms", 0))
        delta = (
            max(0.0, current_wal_sync_ms - previous_wal_sync_ms)
            if previous_wal_sync_ms is not None else 0.0
        )
        previous_wal_sync_ms = current_wal_sync_ms
        max_wal_sync_delta_ms = max(max_wal_sync_delta_ms, delta)
        if interesting or delta >= 50 or query_ms >= 100:
            event = {
                "utc": row.get("utc"),
                "wait_types": {
                    kind: int(count) for kind, count in waits.items()
                    if kind not in IGNORED_WAITS and int(count) > 0
                },
                "wait_events": {name: int(count) for name, count in wait_events.items()
                                if int(count) > 0},
                "wal_sync_delta_ms": round(delta, 3),
                "query_ms": query_ms,
            }
            events.append((max(interesting, delta / 50, query_ms / 100), event))
            events.sort(key=lambda item: item[0], reverse=True)
            del events[20:]
    return {
        "samples": samples,
        "sample_errors": errors,
        "max_query_ms": max_query_ms,
        "max_observer_wake_lag_ms": max_wake_lag_ms,
        "max_wal_sync_delta_ms": max_wal_sync_delta_ms,
        "max_interesting_waiters": max_interesting_waiters,
        "wait_type_sample_counts": dict(wait_type_samples),
        "wait_event_sample_counts": dict(wait_event_samples),
        "wal_timing_enabled": wal_timing_enabled,
        "top_anomalies": [item[1] for item in events],
        "caveat": "Samples can miss shorter waits; WAL counters are global to the RDS instance.",
    }


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: summarize_rds_waits.py private-rds-waits.jsonl")
    with Path(sys.argv[1]).open(encoding="utf-8") as source:
        result = summarize(source)
    print(json.dumps(result))
    return 0 if result["samples"] and not result["sample_errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
