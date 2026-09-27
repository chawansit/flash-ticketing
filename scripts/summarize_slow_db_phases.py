#!/usr/bin/env python3
"""Summarize timestamped slow DB phases without retaining request data."""

import argparse
import json
import math
import sys
from collections import Counter, deque

PHASES = frozenset({"commit", "pool_return"})
OUTCOMES = frozenset({"ok", "error"})


def summarize(lines, limit: int) -> dict:
    counts = Counter()
    latest = deque(maxlen=limit)
    longest = {}
    malformed = 0
    for line in lines:
        try:
            row = json.loads(line)
        except (TypeError, ValueError):
            continue
        if not isinstance(row, dict) or row.get("event") != "slow_db_phase":
            continue
        phase, outcome, duration = (
            row.get("phase"), row.get("outcome"), row.get("duration_ms")
        )
        if (
            phase not in PHASES
            or outcome not in OUTCOMES
            or not isinstance(duration, (int, float))
            or isinstance(duration, bool)
            or not math.isfinite(duration)
            or duration < 100
        ):
            malformed += 1
            continue
        event = {
            "time": row.get("time") if isinstance(row.get("time"), str) else None,
            "phase": phase,
            "outcome": outcome,
            "duration_ms": duration,
        }
        counts[f"{phase}|{outcome}"] += 1
        latest.append(event)
        if duration > longest.get(phase, {}).get("duration_ms", 0):
            longest[phase] = event
    return {
        "counts": dict(counts),
        "longest_by_phase": longest,
        "last_events": list(latest),
        "malformed_events": malformed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()
    if not 1 <= args.limit <= 100:
        parser.error("limit must be between 1 and 100")
    print(json.dumps(summarize(sys.stdin, args.limit)))


if __name__ == "__main__":
    main()
