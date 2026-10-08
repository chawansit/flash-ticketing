"""Passive, bounded per-host CPU observations for an ADR0147 shared UTC window."""

import argparse
import hashlib
import json
import math
import re
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

HOST_ROLES = {"primary", "secondary"}
BACKGROUND_ROLES = {
    "consumer",
    "reservation-writer",
    "maintenance",
    "publisher",
    "reconciler",
    "simulator",
    "confirmation",
    "kafka",
    "pgbouncer",
    "load-balancer",
}


def timestamp(value):
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("UTC-aware window required")
    return result.astimezone(UTC)


def placement(spec):
    arm = spec.get("arm")
    if arm not in {"control", "candidate"}:
        raise ValueError("Unknown logical CPU arm")
    value = spec.get("placement", "four-primary" if arm == "control" else "two-plus-two")
    if value == "cce-api-isolation":
        if (arm != "candidate" or spec.get("decision") != "ADR0228"
                or not re.fullmatch(r"[0-9a-f]{64}", spec.get("inventory_sha256", ""))):
            raise ValueError("Explicit native CCE receipt binding required")
        return value
    if value == "worker-separation":
        if spec.get("decision") != "ADR0184" or not re.fullmatch(r"[0-9a-f]{64}", spec.get("inventory_sha256", "")):
            raise ValueError("Explicit worker-placement inventory binding required")
        return value
    if value not in {"four-primary", "two-plus-two", "one-plus-three"} or (value == "four-primary" and arm != "control") or (value == "one-plus-three" and arm != "candidate"):
        raise ValueError("Unknown or incompatible CPU placement")
    return value


def validate_spec(spec):
    if spec.get("schema") != 1 or spec.get("host_role") not in HOST_ROLES:
        raise ValueError("Unknown host CPU specification")
    if not re.fullmatch(r"[0-9a-f]{64}", spec.get("instance_uuid_sha256", "")):
        raise ValueError("Verified instance identity required")
    physical = placement(spec)
    rows = spec.get("containers", [])
    if not isinstance(rows, list) or (
        not rows and not (spec["host_role"] == "secondary" and (physical in {"four-primary", "cce-api-isolation"} or (physical == "worker-separation" and spec["arm"] == "control")))
    ):
        raise ValueError("Observed containers required")
    ids = set()
    for entry in rows:
        if not re.fullmatch(r"[0-9a-f]{64}", entry.get("id", "")) or entry["id"] in ids:
            raise ValueError("Distinct full container IDs required")
        if entry.get("role") not in ({"cce-audit", "cce-pooler", *BACKGROUND_ROLES} if physical == "cce-api-isolation" else {"api", *BACKGROUND_ROLES}):
            raise ValueError("Unexpected container role")
        if spec["host_role"] == "secondary" and entry["role"] != "api" and physical != "worker-separation":
            raise ValueError("Secondary must be API-only")
        ids.add(entry["id"])
    if physical == "cce-api-isolation":
        from collections import Counter
        expected = ({"consumer": 6, "reservation-writer": 3, "maintenance": 1,
                     "publisher": 1, "reconciler": 1, "simulator": 1, "confirmation": 1,
                     "kafka": 1, "pgbouncer": 1, "load-balancer": 1,
                     "cce-audit": 1, "cce-pooler": 1} if spec["host_role"] == "primary" else {})
        if dict(Counter(r["role"] for r in rows)) != expected:
            raise ValueError("Exact native background and helper counts required")
        for entry in rows:
            helper = entry["role"].startswith("cce-")
            owner = entry.get("owner")
            if helper and (not isinstance(owner, str) or not re.fullmatch(r"adr0151-[0-9a-f]{12}", owner)):
                raise ValueError("Owned native helper identity required")
            project = "cce-" + owner if helper else "flash-ticketing"
            if (not re.fullmatch(r"sha256:[0-9a-f]{64}", entry.get("image_id", ""))
                    or entry.get("project") != project or not isinstance(entry.get("started_at"), str)):
                raise ValueError("Native background CPU identity required")
            timestamp(entry["started_at"])
            if entry["role"].startswith("cce-") and not re.fullmatch(r"adr0151-[0-9a-f]{12}", entry.get("owner", "")):
                raise ValueError("Owned native helper identity required")
        return rows
    if physical == "worker-separation":
        from collections import Counter

        from worker_separation_topology import INFRA, PROJECT, WORKERS
        expected = ({"primary": {**INFRA, **WORKERS}, "secondary": {}} if spec["arm"] == "control"
                    else {"primary": INFRA, "secondary": WORKERS})[spec["host_role"]]
        if dict(Counter(r["role"] for r in rows)) != expected:
            raise ValueError("Exact host-aware CPU role counts required")
        for entry in rows:
            project = "flash-ticketing" if spec["host_role"] == "primary" else PROJECT
            if (not re.fullmatch(r"sha256:[0-9a-f]{64}", entry.get("image_id", ""))
                    or entry.get("project") != project or not isinstance(entry.get("started_at"), str)):
                raise ValueError("Full worker-placement CPU identity required")
            timestamp(entry["started_at"])
        return rows
    count = sum(entry["role"] == "api" for entry in rows)
    expected = ({"primary": 4, "secondary": 0} if physical == "four-primary" else
                {"primary": 1, "secondary": 3} if physical == "one-plus-three" else
                {"primary": 2, "secondary": 2})[spec["host_role"]]
    if spec.get("arm") not in {"control", "candidate"} or count != expected:
        raise ValueError("Placement differs from arm")
    return rows


def proc_identity(pid):
    # Linux comm can contain spaces/parentheses: parse after the final ')'.
    fields = (Path("/proc") / str(pid) / "stat").read_text().rsplit(")", 1)[1].split()
    return int(fields[19])  # field22 starttime, with field3 now at index0


def inspect_containers(entries):
    if not entries:
        return {}
    rows = json.loads(
        subprocess.check_output(["docker", "inspect", *(e["id"] for e in entries)], text=True, timeout=10)
    )
    by_id = {row["Id"]: row for row in rows}
    result = {}
    for entry in entries:
        row = by_id[entry["id"]]
        if (
            not row["State"]["Running"]
            or (row["Config"]["Labels"].get("codex-purpose") != entry["role"]
                or row["Config"]["Labels"].get("codex-owner") != entry.get("owner")
                or row["Config"]["Labels"].get("com.docker.compose.service")
                != {"cce-audit": "audit-helper", "cce-pooler": "pooler-bridge"}[entry["role"]]
                if entry["role"] in {"cce-audit", "cce-pooler"}
                else row["Config"]["Labels"].get("com.docker.compose.service") != entry["role"])
        ):
            raise ValueError("Observed container disappeared or changed role")
        if any(key in entry for key in ("image_id", "started_at", "project")) and (
                row["Image"] != entry.get("image_id") or row["State"]["StartedAt"] != entry.get("started_at")
                or row["Config"]["Labels"].get("com.docker.compose.project") != entry.get("project")):
            raise ValueError("CPU container image/start/project changed")
        pid = row["State"]["Pid"]
        if type(pid) is not int or pid <= 0:
            raise ValueError("Invalid container PID")
        relative = (Path("/proc") / str(pid) / "cgroup").read_text().split("0::", 1)[1].strip()
        base = Path("/sys/fs/cgroup").resolve()
        path = (base / relative.lstrip("/") / "cpu.stat").resolve()
        if not path.is_relative_to(base) or not path.is_file():
            raise ValueError("Missing unified cgroup")
        result[entry["id"]] = {
            "role": entry["role"],
            **{k: entry[k] for k in ("image_id", "started_at", "project", "owner") if k in entry},
            "pid": pid,
            "process_start_ticks": proc_identity(pid),
            "path": path,
        }
    return result


def collect(spec, start_at, *, seconds=300, interval=5):
    entries = validate_spec(spec)
    start = timestamp(start_at)
    if (
        type(seconds) is not int
        or not 1 <= seconds <= 300
        or not 0.25 <= interval <= 10
        or seconds % interval
    ):
        raise ValueError("Bounded whole sampling intervals required")
    uuid = Path("/sys/class/dmi/id/product_uuid").read_text().strip().lower()
    actual = hashlib.sha256(uuid.encode()).hexdigest()
    if actual != spec["instance_uuid_sha256"]:
        raise ValueError("Instance identity differs")
    delay = (start - datetime.now(UTC)).total_seconds()
    if not 0 <= delay <= 60:
        raise ValueError("Shared start must be within next60seconds")
    containers = inspect_containers(entries)
    # Convert the common UTC deadline once, then sample on a monotonic clock.
    deadline = time.monotonic() + (start - datetime.now(UTC)).total_seconds()
    rows = []
    for i in range(round(seconds / interval) + 1):
        time.sleep(max(0, deadline + i * interval - time.monotonic()))
        utc = datetime.now(UTC).isoformat()
        row = {
            "utc": utc,
            "elapsed_seconds": time.monotonic() - deadline,
            "host_ticks": [int(v) for v in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]],
            "container_cpu_usec": {},
        }
        try:
            for cid, entry in containers.items():
                if proc_identity(entry["pid"]) != entry["process_start_ticks"]:
                    raise ValueError("Container process restarted")
                values = dict(line.split() for line in entry["path"].read_text().splitlines())
                row["container_cpu_usec"][cid] = int(values["usage_usec"])
        except (OSError, ValueError):
            row["collection_error"] = "container_identity_or_cgroup_lost"
        rows.append(row)
        if "collection_error" in row:
            break
    return {
        "schema": 1,
        "host_role": spec["host_role"],
        "arm": spec["arm"],
        "placement": placement(spec),
        **{k: spec[k] for k in ("decision", "inventory_sha256") if k in spec},
        "instance_uuid_sha256": actual,
        "requested_start_utc": start.isoformat(),
        "seconds": seconds,
        "interval": interval,
        "containers": [
            {"id": cid, **{k: v for k, v in entry.items() if k != "path"}}
            for cid, entry in containers.items()
        ],
        "samples": rows,
    }


def summarize(data):
    seconds, interval = data["seconds"], data["interval"]
    if (
        type(seconds) is not int
        or not 1 <= seconds <= 300
        or not 0.25 <= interval <= 10
        or seconds % interval
    ):
        raise ValueError("Invalid sampling window")
    spec = {
        "schema": 1,
        "host_role": data["host_role"],
        "arm": data["arm"],
        "instance_uuid_sha256": data["instance_uuid_sha256"],
        "containers": data["containers"],
        **({"placement": data["placement"]} if "placement" in data else {}),
        **{k: data[k] for k in ("decision", "inventory_sha256") if k in data},
    }
    entries = validate_spec(spec)
    ids = {entry["id"] for entry in entries}
    rows = data["samples"]
    if len(rows) != round(seconds / interval) + 1:
        raise ValueError("Incomplete CPU samples")
    requested = timestamp(data["requested_start_utc"])
    previous = None
    for index, row in enumerate(rows):
        elapsed, ticks, counters = row["elapsed_seconds"], row["host_ticks"], row["container_cpu_usec"]
        utc = timestamp(row["utc"])
        if (
            "collection_error" in row
            or type(elapsed) not in {int, float}
            or not math.isfinite(elapsed)
            or abs(elapsed - index * interval) > 1
            or abs((utc - requested).total_seconds() - elapsed) > 1
            or len(ticks) != 8
            or set(counters) != ids
            or any(type(v) is not int or v < 0 for v in [*ticks, *counters.values()])
        ):
            raise ValueError("Invalid, late or clock-skewed CPU sample")
        if previous and (
            elapsed <= previous["elapsed_seconds"]
            or any(counters[cid] < previous["container_cpu_usec"][cid] for cid in ids)
            or any(new < old for new, old in zip(ticks, previous["host_ticks"], strict=True))
        ):
            raise ValueError("Counter reset or nonmonotonic sample")
        previous = row
    first, last = rows[0], rows[-1]
    elapsed = last["elapsed_seconds"] - first["elapsed_seconds"]
    ticks = [n - o for n, o in zip(last["host_ticks"], first["host_ticks"], strict=True)]
    if elapsed <= 0 or sum(ticks) <= 0:
        raise ValueError("Missing CPU interval")
    cores = {}
    for entry in entries:
        cid, role = entry["id"], entry["role"]
        cores[role] = (
            cores.get(role, 0)
            + (last["container_cpu_usec"][cid] - first["container_cpu_usec"][cid]) / 1e6 / elapsed
        )
    return {
        "host_role": data["host_role"],
        "arm": data["arm"],
        "placement": placement(spec),
        **{k: spec[k] for k in ("decision", "inventory_sha256") if k in spec},
        "instance_uuid_sha256": data["instance_uuid_sha256"],
        "start_utc": first["utc"],
        "end_utc": last["utc"],
        "samples": len(rows),
        "host_cpu_percent": 100 * (sum(ticks) - ticks[3] - ticks[4]) / sum(ticks),
        "cpu_cores_by_role": cores,
        "pass": True,
    }


def compare_windows(primary, secondary, *, offered_start_utc, offered_end_utc):
    if (
        primary["host_role"] != "primary"
        or secondary["host_role"] != "secondary"
        or primary["arm"] != secondary["arm"]
        or placement(primary) != placement(secondary)
        or (placement(primary) in {"worker-separation", "cce-api-isolation"} and primary["inventory_sha256"] != secondary["inventory_sha256"])
        or primary["instance_uuid_sha256"] == secondary["instance_uuid_sha256"]
    ):
        raise ValueError("Distinct matched host roles required")
    if any(
        abs((timestamp(primary[k]) - timestamp(secondary[k])).total_seconds()) > 1
        for k in ("start_utc", "end_utc")
    ):
        raise ValueError("Host sampling windows differ")
    offered_start, offered_end = timestamp(offered_start_utc), timestamp(offered_end_utc)
    if not 0 < (offered_end - offered_start).total_seconds() <= 300:
        raise ValueError("Bounded offered interval required")
    if any(
        abs((timestamp(h["start_utc"]) - offered_start).total_seconds()) > 1
        or abs((timestamp(h["end_utc"]) - offered_end).total_seconds()) > 1
        for h in (primary, secondary)
    ):
        raise ValueError("CPU windows do not match dispatched offered interval")
    if primary.get("pass") is not True or secondary.get("pass") is not True:
        raise ValueError("Both observations must pass")
    return {
        "both_hosts_cpu_observed": True,
        "same_offered_measurement_window": True,
        "aggregate_api_cpu_cores": sum(h["cpu_cores_by_role"].get("api", 0) for h in (primary, secondary)),
        "host_cpu_percent": {h["host_role"]: h["host_cpu_percent"] for h in (primary, secondary)},
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start-at", required=True)
    parser.add_argument("--seconds", type=int, default=300)
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("Fresh owned output required")
    data = collect(json.loads(args.spec.read_text()), args.start_at, seconds=args.seconds)
    with args.output.open("x", encoding="utf-8") as f:
        f.write(json.dumps(data) + "\n")
    summarize(data)
    print(json.dumps({"host_role": data["host_role"], "samples": len(data["samples"]), "pass": True}))


if __name__ == "__main__":
    main()
