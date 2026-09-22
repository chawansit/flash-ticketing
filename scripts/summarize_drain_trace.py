"""Summarize bounded hold-expiry and queue-drain observations without raw credentials."""

import argparse
import json
from datetime import datetime
from pathlib import Path

FIELDS = (
    "overdue_active_holds",
    "oldest_overdue_seconds",
    "pending_refresh",
    "oldest_refresh_seconds",
    "unpublished_outbox",
    "dead_letters",
)


def summarize(samples):
    valid = [sample for sample in samples if isinstance(sample, dict) and "error" not in sample]
    if not valid:
        raise ValueError("No valid backend samples")
    report = {
        "samples": len(valid),
        "sample_errors": sum(isinstance(sample, dict) and "error" in sample for sample in samples),
        "first_utc": valid[0]["utc"],
        "last_utc": valid[-1]["utc"],
    }
    for name in FIELDS:
        rows = [sample for sample in valid if name in sample]
        if rows:
            peak = max(rows, key=lambda sample: sample[name])
            report[name] = {
                "max": peak[name],
                "peak_utc": peak["utc"],
                "last": rows[-1][name],
            }
    overdue = [sample for sample in valid if "overdue_active_holds" in sample]
    if overdue:
        peak = max(overdue, key=lambda sample: sample["overdue_active_holds"])
        after = [sample for sample in overdue if sample["utc"] > peak["utc"]]
        drained = next((sample for sample in after if sample["overdue_active_holds"] == 0), None)
        report["observed_drain_after_peak_seconds"] = (
            (datetime.fromisoformat(drained["utc"]) - datetime.fromisoformat(peak["utc"])).total_seconds()
            if drained
            else None
        )
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace", type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(json.loads(args.trace.read_text())), sort_keys=True))


if __name__ == "__main__":
    main()
