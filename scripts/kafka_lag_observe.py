#!/usr/bin/env python3
"""Sample one Kafka consumer group's partition lag without changing group state."""

import argparse
import hashlib
import json
import signal
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
    ownership = {}
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
            fingerprint = hashlib.sha256(member.encode()).hexdigest()[:12]
            ownership.setdefault(fingerprint, []).append(partition)
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
        "member_partitions": {key: sorted(value) for key, value in ownership.items()},
    }


def group_ownership(description, topic):
    """Reject transitional or incomplete ownership instead of inventing zero lag."""
    if description.error_code or description.state != "Stable" or description.protocol_type != "consumer":
        raise ValueError("Consumer group is not stable")
    owned, seen, member_ids = {}, set(), set()
    for member in description.members:
        member_id = member.member_id
        if not member_id or member_id in member_ids:
            raise ValueError("Invalid consumer membership")
        member_ids.add(member_id)
        assignment = member.member_assignment
        if not hasattr(assignment, "assignment"):
            raise ValueError("Missing decoded consumer assignment")
        parts = []
        for assigned_topic, partitions in assignment.assignment:
            if assigned_topic != topic:
                raise ValueError("Unexpected consumer topic")
            for partition in partitions:
                if not isinstance(partition, int) or partition < 0 or partition in seen:
                    raise ValueError("Invalid or duplicate partition ownership")
                seen.add(partition)
                parts.append(partition)
        fingerprint = hashlib.sha256(member_id.encode()).hexdigest()[:12]
        if fingerprint in owned:
            raise ValueError("Ambiguous membership fingerprint")
        owned[fingerprint] = sorted(parts)
    if not owned or not seen:
        raise ValueError("No assigned consumer partitions")
    return owned, seen


class ReadOnlyKafkaObserver:
    """Persistent clients never subscribe, join a group, fetch records or commit."""

    def __init__(self, admin, end_reader, topic):
        self.admin = admin
        self.end_reader = end_reader
        self.topic = topic

    def sample(self, group):
        from kafka import TopicPartition

        descriptions = self.admin.describe_consumer_groups([group])
        if len(descriptions) != 1:
            raise ValueError("Unexpected group descriptions")
        ownership, assigned = group_ownership(descriptions[0], self.topic)
        metadata = self.end_reader.partitions_for_topic(self.topic)
        if not metadata or set(metadata) != assigned:
            raise ValueError("Incomplete topic partition ownership")
        partitions = [TopicPartition(self.topic, p) for p in sorted(metadata)]
        committed = self.admin.list_consumer_group_offsets(group, partitions=partitions)
        ends = self.end_reader.end_offsets(partitions)
        if set(committed) != set(partitions) or set(ends) != set(partitions):
            raise ValueError("Missing partition offsets")
        rows = []
        for partition in partitions:
            offset = committed[partition].offset
            end = ends[partition]
            if not isinstance(offset, int) or not isinstance(end, int) or offset < 0 or end < offset:
                raise ValueError("Invalid partition offsets")
            rows.append({"topic": self.topic, "partition": partition.partition,
                         "current_offset": offset, "log_end_offset": end, "lag": end - offset})
        after = self.admin.describe_consumer_groups([group])
        if len(after) != 1 or group_ownership(after[0], self.topic) != (ownership, assigned):
            raise ValueError("Consumer ownership changed during observation")
        return {"total_lag": sum(row["lag"] for row in rows),
                "max_partition_lag": max(row["lag"] for row in rows),
                "assigned_partitions": len(rows), "members": len(ownership),
                "partitions": rows, "member_partitions": ownership}

    def close(self):
        try:
            self.end_reader.close(autocommit=False, timeout_ms=1000)
        finally:
            self.admin.close()


def python_observer(bootstrap, topic):
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient

    options = {"bootstrap_servers": bootstrap, "request_timeout_ms": 5000,
               "api_version_auto_timeout_ms": 2000, "client_id": "capacity-read-only-observer"}
    admin = KafkaAdminClient(**options)
    try:
        reader = KafkaConsumer(
            **options, group_id=None, enable_auto_commit=False, allow_auto_create_topics=False,
            session_timeout_ms=3000, heartbeat_interval_ms=1000,
        )
    except Exception:
        admin.close()
        raise
    return ReadOnlyKafkaObserver(admin, reader, topic)


def observe(args, stream):
    if args.backend == "python":
        from kafka.errors import KafkaError
    else:
        class KafkaError(Exception):
            pass

    command = ["docker", "exec", args.container, "/opt/kafka/bin/kafka-consumer-groups.sh",
               "--bootstrap-server", args.bootstrap_server, "--group", args.group, "--describe"]
    deadline = time.monotonic() + args.seconds
    observer = None
    stopped = False

    def stop(_signum, _frame):
        nonlocal stopped
        stopped = True

    old_handlers = {sig: signal.signal(sig, stop) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        while not stopped and time.monotonic() < deadline:
            started = time.monotonic()
            sample = {"utc": datetime.now(UTC).isoformat(), "group": args.group}
            try:
                if args.backend == "python":
                    if observer is None:
                        observer = python_observer(args.bootstrap_server, args.topic)
                    sample.update(observer.sample(args.group))
                else:
                    result = subprocess.run(command, capture_output=True, text=True, timeout=10, check=True)
                    sample.update(parse_describe(result.stdout))
            except (OSError, subprocess.SubprocessError, ValueError, KafkaError) as exc:
                sample["error_type"] = type(exc).__name__
                if observer is not None:
                    observer.close()
                    observer = None
            stream.write(json.dumps(sample, separators=(",", ":")) + chr(10))
            stream.flush()
            delay = args.interval - (time.monotonic() - started) if args.backend == "python" else args.interval
            remaining = min(delay, deadline - time.monotonic())
            while not stopped and remaining > 0:
                pause = min(0.1, remaining)
                time.sleep(pause)
                remaining -= pause
    finally:
        if observer is not None:
            observer.close()
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=int, required=True)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--container", default="flash-ticketing-kafka-1")
    parser.add_argument("--bootstrap-server", default="kafka:9092")
    parser.add_argument("--group", default="ticketing-fulfillment-v1")
    parser.add_argument("--backend", choices=["cli", "python"], default="cli")
    parser.add_argument("--topic", default="ticketing.events")
    args = parser.parse_args()
    if args.seconds < 1 or args.interval <= 0 or args.output.exists():
        parser.error("Fresh output, positive seconds and interval required")
    with args.output.open("x", encoding="utf-8") as stream:
        observe(args, stream)


if __name__ == "__main__":
    main()
