"""Summarize bounded live Redis delta-chain observations without show identifiers."""

import json
import sys
from collections import Counter
from pathlib import Path


def summarize(path):
    totals = Counter()
    examples = []
    samples = 0
    prior_server_run_id = None
    server_identity_changes = 0
    min_server_uptime_seconds = None
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        samples += 1
        run_id = row.get("server_run_id")
        if run_id and prior_server_run_id and run_id != prior_server_run_id:
            server_identity_changes += 1
        if run_id:
            prior_server_run_id = run_id
        uptime = row.get("server_uptime_seconds")
        if uptime is not None:
            min_server_uptime_seconds = min(
                int(uptime), min_server_uptime_seconds
            ) if min_server_uptime_seconds is not None else int(uptime)
        totals.update(row["counts"])
        for example in row["examples"]:
            if len(examples) < 20:
                examples.append(example)
    return {
        "samples": samples,
        "counts": dict(totals),
        "examples": examples,
        "server_identity_changes": server_identity_changes,
        "min_server_uptime_seconds": min_server_uptime_seconds,
    }


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: summarize_seat_delta_chain.py INPUT")
    print(json.dumps(summarize(sys.argv[1]), indent=2, sort_keys=True))
