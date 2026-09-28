"""Summarize bounded live Redis delta-chain observations without show identifiers."""

import json
import sys
from collections import Counter
from pathlib import Path


def summarize(path):
    totals = Counter()
    examples = []
    samples = 0
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        samples += 1
        totals.update(row["counts"])
        for example in row["examples"]:
            if len(examples) < 20:
                examples.append(example)
    return {"samples": samples, "counts": dict(totals), "examples": examples}


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: summarize_seat_delta_chain.py INPUT")
    print(json.dumps(summarize(sys.argv[1]), indent=2, sort_keys=True))
