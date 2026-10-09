#!/usr/bin/env python3
"""Aggregate final Prometheus snapshots from reservation-writer replicas."""

import json
import math
import re
import sys
from collections import defaultdict

LABEL = re.compile(r'([A-Za-z0-9_]+)="([^"]*)"')


def parse_metrics(lines) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    metrics = defaultdict(float)
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or " " not in line:
            continue
        metric, raw_value = line.rsplit(None, 1)
        try:
            value = float(raw_value)
        except ValueError:
            continue
        name, _, raw_labels = metric.partition("{")
        labels = tuple(sorted(LABEL.findall(raw_labels)))
        metrics[(name, labels)] += value
    return dict(metrics)


def labels_dict(labels: tuple[tuple[str, str], ...]) -> dict[str, str]:
    return dict(labels)


def labeled_totals(metrics, name: str, label: str) -> dict[str, float]:
    result = defaultdict(float)
    for (metric_name, labels), value in metrics.items():
        if metric_name != name:
            continue
        label_value = labels_dict(labels).get(label)
        if label_value is not None:
            result[label_value] += value
    return dict(sorted(result.items()))


def histogram(metrics, name: str, required: dict[str, str] | None = None) -> dict:
    required = required or {}
    count = total = 0.0
    buckets = defaultdict(float)
    for (metric_name, labels), value in metrics.items():
        current = labels_dict(labels)
        if any(current.get(key) != expected for key, expected in required.items()):
            continue
        if metric_name == f"{name}_count":
            count += value
        elif metric_name == f"{name}_sum":
            total += value
        elif metric_name == f"{name}_bucket" and "le" in current:
            bound = math.inf if current["le"] == "+Inf" else float(current["le"])
            buckets[bound] += value

    def upper(fraction: float) -> float | None:
        if count <= 0:
            return None
        target = count * fraction
        for bound, cumulative in sorted(buckets.items()):
            if cumulative >= target:
                return bound
        return None

    return {
        "count": count,
        "avg": total / count if count else None,
        "p95_upper": upper(0.95),
        "p99_upper": upper(0.99),
    }


def summarize(metrics) -> dict:
    phase_keys = set()
    for metric_name, labels in metrics:
        if metric_name == "ticketing_reservation_persistence_phase_seconds_count":
            current = labels_dict(labels)
            if "phase" in current and "outcome" in current:
                phase_keys.add((current["phase"], current["outcome"]))
    phases = {}
    for phase, outcome in sorted(phase_keys):
        result = histogram(
            metrics,
            "ticketing_reservation_persistence_phase_seconds",
            {"phase": phase, "outcome": outcome},
        )
        phases[f"{phase}:{outcome}"] = {
            "count": result["count"],
            "avg_ms": result["avg"] * 1000 if result["avg"] is not None else None,
            "p95_upper_ms": result["p95_upper"] * 1000
            if result["p95_upper"] is not None
            else None,
            "p99_upper_ms": result["p99_upper"] * 1000
            if result["p99_upper"] is not None
            else None,
        }

    command_age = histogram(metrics, "ticketing_reservation_command_age_seconds")
    batch = histogram(metrics, "ticketing_reservation_persistence_batch_size")
    transaction = histogram(metrics, "ticketing_db_transaction_seconds")
    commit = histogram(metrics, "ticketing_db_commit_seconds")
    return {
        "persistence_outcomes": labeled_totals(
            metrics, "ticketing_reservation_persistence_total", "outcome"
        ),
        "failure_codes": labeled_totals(
            metrics, "ticketing_reservation_persistence_failures_total", "code"
        ),
        "command_age": {
            "count": command_age["count"],
            "avg_seconds": command_age["avg"],
            "p95_upper_seconds": command_age["p95_upper"],
            "p99_upper_seconds": command_age["p99_upper"],
        },
        "batch_size": batch,
        "phases": phases,
        "db_transaction": transaction,
        "db_commit": commit,
    }


def main() -> None:
    metrics = parse_metrics(sys.stdin)
    if not metrics:
        raise SystemExit("No Prometheus metrics were provided")
    print(json.dumps(summarize(metrics), separators=(",", ":")))


if __name__ == "__main__":
    main()
