#!/usr/bin/env python3
"""Summarize candidate API error responses without retaining request payloads."""

import argparse
import json
import sys
from collections import Counter, deque


def summarize(lines, limit: int) -> dict:
    counts = Counter()
    examples = deque(maxlen=limit)
    requests = 0
    for line in lines:
        try:
            record = json.loads(line)
        except (TypeError, ValueError):
            continue
        if not isinstance(record, dict) or record.get("message") != "request":
            continue
        requests += 1
        code = record.get("error_code")
        if not code:
            continue
        counts[str(code)] += 1
        examples.append(
            {key: record.get(key) for key in (
                "time", "request_id", "route", "status", "error_code", "duration_ms"
            )}
        )
    return {"request_log_lines": requests, "error_codes": dict(counts), "last_errors": list(examples)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.limit <= 100:
        parser.error("limit must be between 1 and 100")
    result = summarize(sys.stdin, args.limit)
    print(json.dumps(result))
    if result["request_log_lines"] == 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
