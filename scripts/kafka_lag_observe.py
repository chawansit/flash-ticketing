#!/usr/bin/env python3
"""Sample one Kafka consumer group's partition lag without changing group state."""

import argparse
import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path


def parse_describe(output: str) -> dict:
    lines = [line.split() for line in output.splitlines() if line.strip()]
    header_index = next(
        (
            index
            for index, row in enumerate(lines)
            if "TOPIC" in row and "PARTITION" in row and "LAG" in row
        ),
        None,
    )
    if header_index is None:
        raise ValueError("Consumer-group describe header not found")
    header = lines[header_index]
    positions = {name: header.index(name) for name in (
        "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID"
    )}
    partitions = []
    members = set()
    for row in lines[header_index + 1:]:
        try:
            current = int(row[positions["CURRENT-OFFSET"]])
            end = int(row[positions["LOG-END-OFFSET"]])
            lag = int(row[positions["LAG"]])
            partition = int(row[positions["PARTITION"]])
            member = row[positions["CONSUMER-ID"]]
        except (IndexError, ValueError):
            continue
        if member != "-":
            members.add(member)
        partitions.append(
            {
                "topic": row[positions["TOPIC"]],
                "partition": partition,
                "current_offset": current,
                "log_end_offset": end,
                "lag": lag,
            }
        )
    if not partitions:
        raise ValueError("No assigned consumer-group partitions in describe output")
    return {
        "total_lag": sum(item["lag"] for item in partitions),
        "max_partition_lag": max(item["lag"] for item in partitions),
        "assigned_partitions": len(partitions),
        "members": len(members),
        "partitions": partitions,
    }

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=int, required=True)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--container", default="flash-ticketing-kafka-1")
    parser.add_argument("--bootstrap-server", default="kafka:9092")
    parser.add_argument("--group", default="ticketing-fulfillment-v1")
    args = parser.parse_args()
    if args.seconds < 1 or args.interval <= 0:
        parser.error("seconds and interval must be positive")

    command = [
        "docker",
        "exec",
        args.container,
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        args.bootstrap_server,
        "--group",
        args.group,
        "--describe",
    ]
    deadline = time.monotonic() + args.seconds
    with args.output.open("w", encoding="utf-8") as stream:
        while time.monotonic() < deadline:
            sample = {"utc": datetime.now(UTC).isoformat(), "group": args.group}
            try:
                result = subprocess.run(
                    command, capture_output=True, text=True, timeout=10, check=True
                )
                sample.update(parse_describe(result.stdout))
            except (OSError, subprocess.SubprocessError, ValueError) as exc:
                sample["error_type"] = type(exc).__name__
            stream.write(json.dumps(sample, separators=(",", ":")) + chr(10))
            stream.flush()
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
