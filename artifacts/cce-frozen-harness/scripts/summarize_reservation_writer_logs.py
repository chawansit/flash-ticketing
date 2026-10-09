#!/usr/bin/env python3
"""Summarize reservation-writer failures without retaining command identifiers or payloads."""

import argparse
import json
import math
import sys
from collections import Counter, deque


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    return sorted(values)[math.ceil(len(values) * fraction) - 1]


def summarize(lines, limit: int = 20) -> dict:
    events = Counter()
    failure_codes = Counter()
    command_ages: list[float] = []
    last_failures = deque(maxlen=limit)
    structured_lines = 0

    for line in lines:
        try:
            record = json.loads(line)
        except (TypeError, ValueError):
            continue
        if not isinstance(record, dict):
            continue
        structured_lines += 1
        event = record.get("event") or record.get("message")
        if not isinstance(event, str):
            continue
        if event not in {
            "reservation_command_failed",
            "reservation_command_metadata_expired",
            "worker_iteration_failed",
        }:
            continue
        events[event] += 1
        code = record.get("error_code")
        if event == "reservation_command_metadata_expired":
            code = "METADATA_EXPIRED"
        if isinstance(code, str):
            failure_codes[code] += 1
        age = record.get("command_age_seconds")
        if isinstance(age, (int, float)) and not isinstance(age, bool):
            command_ages.append(float(age))
        last_failures.append(
            {
                "time": record.get("time"),
                "event": event,
                "error_code": code if isinstance(code, str) else None,
                "command_age_seconds": float(age)
                if isinstance(age, (int, float)) and not isinstance(age, bool)
                else None,
            }
        )

    return {
        "structured_log_lines": structured_lines,
        "events": dict(sorted(events.items())),
        "failure_codes": dict(sorted(failure_codes.items())),
        "failed_command_age": {
            "count": len(command_ages),
            "avg_seconds": sum(command_ages) / len(command_ages) if command_ages else None,
            "p95_seconds": percentile(command_ages, 0.95),
            "max_seconds": max(command_ages, default=None),
        },
        "last_failures": list(last_failures),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.limit <= 100:
        parser.error("limit must be between 1 and 100")
    print(json.dumps(summarize(sys.stdin, args.limit), separators=(",", ":")))


if __name__ == "__main__":
    main()
