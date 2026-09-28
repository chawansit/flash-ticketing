"""Sample Redis seat-map versions and recent delta edges during a load stage.

This is read-only diagnostic evidence, not a capacity gate. It deliberately
rotates through a bounded subset of fixture shows to limit probe traffic.
"""

import argparse
import json
import os
import time
from collections import Counter
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path

from redis import Redis


def inspect_sample(show_ids, replies, previous):
    counts = Counter()
    examples = []
    for show_id, (meta, rows) in zip(show_ids, replies, strict=True):
        version_raw, incarnation = meta
        if version_raw is None or incarnation is None:
            counts["map_missing"] += 1
            continue
        version = int(version_raw)
        prior = previous.get(show_id)
        if prior and prior[0] == incarnation and version < prior[1]:
            counts["same_incarnation_regression"] += 1
            examples.append({"kind": "regression", "prior": prior[1], "version": version})
        if prior and prior[0] != incarnation:
            counts["incarnation_change"] += 1
        previous[show_id] = incarnation, version
        edges = []
        for raw in rows:
            entry = json.loads(raw)
            start, end = int(entry["from_version"]), int(entry["version"])
            edges.append((start, end))
            if end <= start:
                counts["nonpositive_range"] += 1
            if end - start > 1:
                counts["wide_range"] += 1
            counts["entries"] += 1
        for left, right in pairwise(edges):
            if right[0] < left[1]:
                counts["internal_overlap"] += 1
                examples.append({"kind": "overlap", "left": left, "right": right})
            elif right[0] > left[1]:
                counts["internal_gap"] += 1
                examples.append({"kind": "gap", "left": left, "right": right})
        if edges and edges[-1][1] != version:
            counts["tail_mismatch"] += 1
            examples.append({"kind": "tail", "edge": edges[-1], "version": version})
        counts["maps"] += 1
    return dict(counts), examples[:10]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=float, required=True)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()
    if args.batch_size < 1 or args.interval <= 0 or args.seconds <= 0:
        parser.error("seconds, batch-size and interval must be positive")
    show_ids = json.loads(args.manifest.read_text(encoding="utf-8"))["show_ids"]
    redis_url = os.getenv("TEST_REDIS_URL") or os.getenv("REDIS_URL")
    if not redis_url:
        parser.error("TEST_REDIS_URL or REDIS_URL is required")
    client = Redis.from_url(redis_url, decode_responses=True, socket_timeout=2)
    previous = {}
    offset = 0
    deadline = time.monotonic() + args.seconds
    with args.output.open("w", encoding="utf-8") as output:
        while time.monotonic() < deadline:
            started = time.monotonic()
            batch = [show_ids[(offset + index) % len(show_ids)] for index in range(min(args.batch_size, len(show_ids)))]
            offset = (offset + len(batch)) % len(show_ids)
            pipeline = client.pipeline(transaction=False)
            for show_id in batch:
                slot = "{" + show_id + "}"
                pipeline.hmget("seatmap:v2:" + slot, "version", "incarnation")
                pipeline.zrange("seatdelta:v1:" + slot, -16, -1)
            raw = pipeline.execute()
            counts, examples = inspect_sample(batch, list(zip(raw[::2], raw[1::2], strict=True)), previous)
            output.write(json.dumps({"utc": datetime.now(UTC).isoformat(), "counts": counts, "examples": examples}) + "\n")
            output.flush()
            time.sleep(max(0, args.interval - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
