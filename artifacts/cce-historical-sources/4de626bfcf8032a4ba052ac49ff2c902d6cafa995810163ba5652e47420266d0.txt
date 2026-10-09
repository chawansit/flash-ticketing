"""Collect one bounded, read-only CPU window for an existing paid-control run."""

import argparse
import json
import math
import re
import subprocess
import time
from pathlib import Path

SAMPLE_COUNT = 49
SAMPLE_SECONDS = 240
EXPECTED_REPLICAS = {"api": 4, "consumer": 6, "reservation-writer": 3}

# The unchanged remote sampling program is embedded below.
REMOTE_CODE = 'import json,subprocess,time,sys\nfrom pathlib import Path\nfrom datetime import datetime,timezone\ncontainers=[]\nfor line in subprocess.check_output([\'docker\',\'ps\',\'--format\',\'{{.ID}} {{.Label "com.docker.compose.service"}}\'],text=True).splitlines():\n cid,role=line.split();pid=int(subprocess.check_output([\'docker\',\'inspect\',\'-f\',\'{{.State.Pid}}\',cid],text=True))\n path=(Path(\'/proc\')/str(pid)/\'cgroup\').read_text().split(\'0::\',1)[1].strip()\n cpu=Path(\'/sys/fs/cgroup\')/path.lstrip(\'/\')/\'cpu.stat\'\n if not cpu.exists():raise RuntimeError(\'Cgroup missing\')\n containers.append((role,cpu))\ncounts={}\nfor role,cpu in containers:counts[role]=counts.get(role,0)+1\nif counts.get(\'api\')!=4 or counts.get(\'consumer\')!=6 or counts.get(\'reservation-writer\')!=3:raise RuntimeError(\'Candidate role count differs\')\nrows=[];start=time.monotonic()\nfor i in range(49):\n row={\'utc\':datetime.now(timezone.utc).isoformat(),\'elapsed_seconds\':time.monotonic()-start,\'cpu_usec_by_role\':{},\'host_ticks\':[int(v) for v in Path(\'/proc/stat\').read_text().splitlines()[0].split()[1:9]]}\n try:\n  for role,path in containers:\n   values=dict(x.split() for x in path.read_text().splitlines())\n   row[\'cpu_usec_by_role\'][role]=row[\'cpu_usec_by_role\'].get(role,0)+int(values[\'usage_usec\'])\n except FileNotFoundError:row[\'collection_error\']=\'candidate_cgroup_disappeared\'\n rows.append(row)\n if \'collection_error\' in row:break\n if i<48:time.sleep(max(0,start+(i+1)*5-time.monotonic()))\nprint(json.dumps({\'samples\':rows,\'replica_counts\':counts,\'pass\':len(rows)==49 and not any(\'collection_error\' in x for x in rows),\'scope\':\'Read-only cumulative Linux cgroup CPU counters; no database/API/Redis/Kafka requests.\'}))\n'


def wait_for_dispatch(root, *, timeout=180, interval=2, clock=time.monotonic, sleep=time.sleep):
    """Wait for a complete dispatch checkpoint within one monotonic deadline."""
    if not 0 < timeout <= 180 or not 0 < interval <= 2:
        raise ValueError("Invalid bounded readiness wait")
    deadline = clock() + timeout
    while clock() < deadline:
        try:
            state = json.loads((root / "state.json").read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError):
            state = None
        if state is not None:
            if (
                not isinstance(state, dict)
                or state.get("run") != root.name
                or not isinstance(state.get("phases"), list)
                or not all(isinstance(phase, str) for phase in state["phases"])
                or "error" not in state
            ):
                raise ValueError("Invalid CPU sampling checkpoint")
            if state["error"]:
                raise RuntimeError("Control failed before CPU sampling")
            if any(phase in state["phases"] for phase in ("probe", "rollback", "simulator-restore")):
                raise RuntimeError("Dispatch sampling window has already ended")
            if "generator-script-directory" in state["phases"]:
                return state
        remaining = deadline - clock()
        if remaining > 0:
            sleep(min(interval, remaining))
    raise TimeoutError("Dispatch checkpoint did not become ready within the startup bound")


def summarize_cpu(data):
    """Reject incomplete or reset counters before deriving rates."""
    if not isinstance(data, dict) or data.get("pass") is not True:
        raise ValueError("CPU collection failed")
    if any(data.get("replica_counts", {}).get(role) != count for role, count in EXPECTED_REPLICAS.items()):
        raise ValueError("Candidate role count differs")
    rows = data.get("samples", [])
    if len(rows) != SAMPLE_COUNT:
        raise ValueError("Incomplete CPU sampling window")
    roles = set(rows[0].get("cpu_usec_by_role", {}))
    if not roles or not set(EXPECTED_REPLICAS) <= roles:
        raise ValueError("Missing CPU role counters")
    previous = None
    for row in rows:
        elapsed = row.get("elapsed_seconds")
        counters = row.get("cpu_usec_by_role", {})
        ticks = row.get("host_ticks", [])
        if (
            "collection_error" in row
            or not isinstance(elapsed, (int, float))
            or not math.isfinite(elapsed)
            or elapsed < 0
            or set(counters) != roles
            or len(ticks) != 8
            or not isinstance(row.get("utc"), str)
            or not all(isinstance(value, int) and value >= 0 for value in [*counters.values(), *ticks])
        ):
            raise ValueError("Invalid CPU sample")
        if previous and (
            elapsed <= previous["elapsed_seconds"]
            or any(counters[role] < previous["cpu_usec_by_role"][role] for role in roles)
            or any(new < old for new, old in zip(ticks, previous["host_ticks"], strict=True))
        ):
            raise ValueError("CPU counter reset or nonmonotonic sampling")
        previous = row
    first, last = rows[0], rows[-1]
    elapsed = last["elapsed_seconds"] - first["elapsed_seconds"]
    if not SAMPLE_SECONDS - 1 <= elapsed <= 265:
        raise ValueError("CPU sampling duration differs")
    ticks = [new - old for new, old in zip(last["host_ticks"], first["host_ticks"], strict=True)]
    if sum(ticks) <= 0:
        raise ValueError("Missing host CPU interval")
    return {
        "observed": True,
        "pass": True,
        "samples": len(rows),
        "sample_start_utc": first["utc"],
        "sample_end_utc": last["utc"],
        "window_seconds": elapsed,
        "host_cpu_mean_percent": 100 * (sum(ticks) - ticks[3] - ticks[4]) / sum(ticks),
        "aggregate_cpu_cores_by_role": {
            role: (last["cpu_usec_by_role"][role] - first["cpu_usec_by_role"][role]) / elapsed / 1e6
            for role in sorted(roles)
        },
        "scope": "Cumulative host/cgroup CPU counters over240s; API includes observers; no function attribution.",
    }


def collect(root, backend_host, identity_file, *, readiness_timeout=180):
    if not re.fullmatch(r"checkout-[0-9]{8}T[0-9]{6}Z-[0-9a-f]{6}", root.name):
        raise ValueError("Invalid run identity")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.@-]*", backend_host):
        raise ValueError("Invalid backend host")
    if not identity_file.is_file():
        raise ValueError("SSH identity missing")
    wait_for_dispatch(root, timeout=readiness_timeout)
    command = [
        "ssh", "-i", str(identity_file), "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes",
        "-o", "ConnectTimeout=10", "-o", "ConnectionAttempts=1",
        "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
        backend_host, "python3", "-",
    ]
    stderr_path = root / "container-cpu-sampling.stderr"
    try:
        result = subprocess.run(command, input=REMOTE_CODE, text=True, capture_output=True, timeout=265, check=False)
    except subprocess.TimeoutExpired as exc:
        stderr = exc.stderr or ""
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", errors="replace")
        stderr_path.write_text(stderr, encoding="utf-8")
        raise RuntimeError("CPU sampling transport timed out") from exc
    stderr_path.write_text(result.stderr, encoding="utf-8")
    if result.returncode:
        raise RuntimeError("CPU sampling transport failed; stderr retained")
    data = json.loads(result.stdout)
    (root / "container-cpu-samples.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    summary = summarize_cpu(data)
    (root / "container-cpu-summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--backend-host", required=True)
    parser.add_argument("--identity-file", type=Path, required=True)
    args = parser.parse_args(argv)
    summary = collect(args.run_dir, args.backend_host, args.identity_file)
    print(json.dumps({"container_cpu_samples": summary["samples"], "pass": summary["pass"]}))


if __name__ == "__main__":
    main()
