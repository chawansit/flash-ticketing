"""Summarize one bounded Kafka consumer-group lag trace without member IDs."""

import argparse
import json
from datetime import datetime
from pathlib import Path


def summarize(rows):
    valid = [row for row in rows if "error_type" not in row and "total_lag" in row]
    if not valid:
        raise ValueError("Kafka lag trace needs at least one valid sample")
    peak = max(valid, key=lambda row: row["total_lag"])
    later_zero = next(
        (row for row in valid if row["utc"] > peak["utc"] and row["total_lag"] == 0),
        None,
    )
    drain_seconds = (
        0
        if peak["total_lag"] == 0
        else (
            (datetime.fromisoformat(later_zero["utc"]) - datetime.fromisoformat(peak["utc"])).total_seconds()
            if later_zero
            else None
        )
    )
    partition_peaks = {}
    for row in valid:
        for partition in row.get("partitions", []):
            key = str(partition["partition"])
            partition_peaks[key] = max(partition_peaks.get(key, 0), partition["lag"])
    return {
        "samples": len(valid),
        "sample_errors": len(rows) - len(valid),
        "max_total_lag": peak["total_lag"],
        "max_partition_lag": max(row["max_partition_lag"] for row in valid),
        "partition_lag_peaks": partition_peaks,
        "last_total_lag": valid[-1]["total_lag"],
        "members_min": min(row["members"] for row in valid),
        "members_max": max(row["members"] for row in valid),
        "drain_after_peak_seconds": drain_seconds,
        "ownership_observed": any("member_partitions" in row for row in valid),
        "max_partitions_per_member": max(
            (len(parts) for row in valid for parts in row.get("member_partitions", {}).values()),
            default=None,
        ),
        "last_member_partition_groups": sorted(valid[-1].get("member_partitions", {}).values()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Fresh output required")
    rows = [json.loads(line) for line in args.trace.read_text(encoding="utf-8").splitlines() if line]
    result = summarize(rows)
    args.output.write_text(json.dumps(result, indent=2) + chr(10), encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
