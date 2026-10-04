"""One bounded no-buyer ADR0146 observer/profiler overhead protocol."""

import argparse
import json
import math
import os
import signal
import subprocess
import time
from pathlib import Path

PHASES = ("idle_before", "observers_only", "idle_middle", "observers_plus_profiler", "idle_after")
EXPECTED = {"api": 4, "consumer": 6, "reservation-writer": 3}


def summarize_window(before, after):
    seconds = after["elapsed"] - before["elapsed"]
    roles = set(before["cpu"])
    if not 59 <= seconds <= 90 or roles != set(after["cpu"]):
        raise ValueError("Missing roles or invalid phase duration")
    counters = [*before["cpu"].values(), *after["cpu"].values(), before["api_process"], after["api_process"]]
    if not all(isinstance(v, (int, float)) and math.isfinite(v) and v >= 0 for v in counters):
        raise ValueError("Invalid CPU counter")
    if any(after["cpu"][r] < before["cpu"][r] for r in roles) or after["api_process"] < before["api_process"]:
        raise ValueError("CPU counter reset")
    if not all(isinstance(v, int) and v >= 0 for v in [*before["ticks"], *after["ticks"]]):
        raise ValueError("Invalid host CPU counter")
    ticks = [b - a for a, b in zip(before["ticks"], after["ticks"], strict=True)]
    if len(ticks) != 8 or any(v < 0 for v in ticks) or sum(ticks) <= 0:
        raise ValueError("Invalid host CPU interval")
    return {
        "seconds": seconds,
        "host_cpu_percent": 100 * (sum(ticks) - ticks[3] - ticks[4]) / sum(ticks),
        "cpu_cores_by_role": {r: (after["cpu"][r] - before["cpu"][r]) / seconds / 1e6 for r in sorted(roles)},
        "api_process_cpu_cores": (after["api_process"] - before["api_process"]) / seconds,
        "steal_ticks": ticks[7],
    }


def effects(phases):
    if [p["phase"] for p in phases] != list(PHASES):
        raise ValueError("Missing, reordered or duplicate protocol phase")
    result = {}
    for name, active, left, right in [("observers", 1, 0, 2), ("observers_plus_profiler", 3, 2, 4)]:
        rows = [phases[i] for i in (active, left, right)]
        if not all(
            math.isfinite(r["host_cpu_percent"]) and math.isfinite(r["api_process_cpu_cores"]) for r in rows
        ):
            raise ValueError("Nonfinite summary")
        if not all(math.isfinite(v) and v >= 0 for r in rows for v in r["cpu_cores_by_role"].values()):
            raise ValueError("Invalid role CPU summary")
        values = [sum(r["cpu_cores_by_role"].values()) for r in rows]
        delta = values[0] - (values[1] + values[2]) / 2
        drift = abs(values[1] - values[2])
        result[name] = {
            "container_cpu_delta_cores": delta,
            "bracketing_idle_drift_cores": drift,
            "positive_direction_above_idle_drift": delta > drift,
            "host_cpu_delta_percentage_points": rows[0]["host_cpu_percent"]
            - (rows[1]["host_cpu_percent"] + rows[2]["host_cpu_percent"]) / 2,
            "api_process_delta_cores": rows[0]["api_process_cpu_cores"]
            - (rows[1]["api_process_cpu_cores"] + rows[2]["api_process_cpu_cores"]) / 2,
        }
    return result


def run(repo, fixture, output, observer):
    from sample_api_stacks import targets

    repo = repo.resolve()
    output = output.resolve()
    if output.parent != repo / "tmp" or output.exists() or not output.name.startswith("adr0146-"):
        raise ValueError("Fresh owned output inside repository tmp required")
    output.mkdir(mode=0o700)
    compose = [
        "docker",
        "compose",
        "--env-file",
        ".env.rds",
        "-f",
        "compose.yaml",
        "-f",
        "compose.rds.yaml",
        "-f",
        "compose.horizontal.yaml",
        "-f",
        "compose.keepalive10.yaml",
    ]

    def call(args, timeout=20):
        return subprocess.check_output(
            args, cwd=repo, text=True, timeout=timeout, stderr=subprocess.DEVNULL
        ).strip()

    api_targets = targets(repo)
    containers = []
    counts = {}
    images = {}
    for cid in call(["docker", "ps", "-q"]).split():
        row = json.loads(call(["docker", "inspect", cid]))[0]
        if row["Config"]["Labels"].get("com.docker.compose.project") != "flash-ticketing":
            continue
        role = row["Config"]["Labels"]["com.docker.compose.service"]
        pid = row["State"]["Pid"]
        counts[role] = counts.get(role, 0) + 1
        images.setdefault(role, set()).add(row["Image"])
        cg = Path(f"/proc/{pid}/cgroup").read_text().split("0::", 1)[1].strip()
        cpu = Path("/sys/fs/cgroup") / cg.lstrip("/") / "cpu.stat"
        if not cpu.is_file():
            raise ValueError("Missing CPU cgroup")
        containers.append((cid, role, cpu))
    if any(counts.get(role) != n for role, n in EXPECTED.items()):
        raise ValueError("Wrong frozen footprint")
    api = api_targets[0]["container"]
    hz = os.sysconf("SC_CLK_TCK")
    start = time.monotonic()

    def snapshot():
        cpu = {}
        for cid, role, path in containers:
            values = dict(line.split() for line in path.read_text().splitlines())
            cpu[role] = cpu.get(role, 0) + int(values["usage_usec"])
        process = 0
        for target in api_targets:
            fields = Path(f"/proc/{target['pid']}/stat").read_text().rsplit(")", 1)[1].split()
            if fields[19] != target["start"]:
                raise ValueError("API PID identity changed")
            process += (int(fields[11]) + int(fields[12])) / hz
        return {
            "elapsed": time.monotonic() - start,
            "cpu": cpu,
            "ticks": [int(v) for v in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]],
            "api_process": process,
        }

    inside = "/tmp/" + output.name
    owned = [
        inside + suffix
        for suffix in (
            "-fixture.json",
            "-observer.py",
            "-O-pipeline.jsonl",
            "-O-kafka.jsonl",
            "-OP-pipeline.jsonl",
            "-OP-kafka.jsonl",
        )
    ]
    children = []
    logs = []
    phases = []
    failure = None
    clean = False
    profile = None
    # Inspect versions without emitting environment or connection strings.
    versions = json.loads(
        call(
            [
                "docker",
                "exec",
                api,
                "python",
                "-c",
                "import json,sys;from importlib.metadata import version;print(json.dumps({'python':sys.version.split()[0],**{n:version(n) for n in ['fastapi','psycopg','redis','kafka-python','prometheus-client']}}))",
            ]
        )
    )
    # Check the whole namespace before assuming ownership. A collision must
    # never enter cleanup and remove another run's files or processes.
    for name in owned:
        subprocess.run(
            ["docker", "exec", api, "test", "!", "-e", name],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=15,
        )
    try:
        subprocess.run(
            ["docker", "cp", str(fixture), api + ":" + inside + "-fixture.json"],
            check=True,
            capture_output=True,
            timeout=15,
        )
        subprocess.run(
            ["docker", "cp", str(observer), api + ":" + inside + "-observer.py"],
            check=True,
            capture_output=True,
            timeout=15,
        )
        observer_hash = call(["docker", "exec", api, "sha256sum", inside + "-observer.py"]).split()[0]
        import hashlib

        if observer_hash != hashlib.sha256(observer.read_bytes()).hexdigest():
            raise ValueError("Observer upload byte mismatch")
        for phase in PHASES:
            print(json.dumps({"phase": phase, "status": "started"}), flush=True)
            before = snapshot()
            samples = [before]
            phase_children = []
            label = "OP" if phase == "observers_plus_profiler" else "O"
            if phase in ("observers_only", "observers_plus_profiler"):
                cmds = [
                    (
                        "pipeline",
                        [
                            "docker",
                            "exec",
                            api,
                            "sh",
                            "-lc",
                            'TEST_DATABASE_URL="$DATABASE_URL" python '
                            + inside
                            + "-observer.py --manifest "
                            + inside
                            + "-fixture.json --output "
                            + inside
                            + "-"
                            + label
                            + "-pipeline.jsonl --seconds 60 --interval 1",
                        ],
                    ),
                    (
                        "kafka",
                        [
                            "docker",
                            "exec",
                            api,
                            "python",
                            "/app/scripts/kafka_lag_observe.py",
                            "--backend",
                            "python",
                            "--seconds",
                            "60",
                            "--interval",
                            "2",
                            "--output",
                            inside + "-" + label + "-kafka.jsonl",
                        ],
                    ),
                ]
                if phase == "observers_plus_profiler":
                    cmds.append(
                        (
                            "profiler",
                            [
                                "python3",
                                str(repo / "scripts/sample_api_stacks.py"),
                                "--repo",
                                str(repo),
                                "--output",
                                str(output / "profiles"),
                                "--profiler",
                                "/root/flash-profiling-tools/bin/py-spy",
                                "--seconds",
                                "60",
                                "--rate",
                                "25",
                            ],
                        )
                    )
                for name, args in cmds:
                    log = (output / (phase + "-" + name + ".log")).open("w")
                    logs.append(log)
                    proc = subprocess.Popen(args, cwd=repo, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
                    children.append(proc)
                    phase_children.append((name, proc))
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                time.sleep(min(5, max(0, deadline - time.monotonic())))
                samples.append(snapshot())
            for name, proc in phase_children:
                proc.wait(timeout=20)
                if proc.returncode:
                    raise RuntimeError("Owned instrument exited nonzero: " + name)
            after = snapshot()
            window = summarize_window(before, after)
            window["phase"] = phase
            if phase_children:
                for name in ("pipeline", "kafka"):
                    path = output / (phase + "-" + name + ".jsonl")
                    path.write_text(
                        call(["docker", "exec", api, "cat", inside + "-" + label + "-" + name + ".jsonl"])
                        + "\n"
                    )
                    rows = [json.loads(line) for line in path.read_text().splitlines()]
                    if len(rows) < (35 if name == "pipeline" else 20):
                        raise ValueError("Insufficient observer samples")
                    if any(any(k.endswith(("error", "error_type")) for k in row) for row in rows):
                        raise ValueError("Observer collection error")
                    if name == "kafka" and any(
                        r.get("total_lag") != 0 or r.get("members") != 6 or r.get("assigned_partitions") != 6
                        for r in rows
                    ):
                        raise ValueError("Kafka lag or ownership changed")
                    if name == "pipeline" and any(len(r.get("api_replicas", {})) != 4 for r in rows):
                        raise ValueError("API observation footprint changed")
                    window[name + "_samples"] = len(rows)
            if phase == "observers_plus_profiler":
                profile = json.loads((output / "profiles/collection.json").read_text())
                if (
                    not profile["pass"]
                    or not profile["owned_profilers_exited"]
                    or profile["profile_count"] != 4
                ):
                    raise ValueError("Incomplete profiler capture")
                window["profiler_cpu_cores"] = (
                    profile["profiler_child_cpu_seconds"] / profile["elapsed_seconds"]
                )
                if window["profiler_cpu_cores"] > 0.05:
                    raise ValueError("Profiler overhead budget exceeded")
            (output / (phase + "-cpu.json")).write_text(json.dumps(samples + [after], indent=2) + "\n")
            phases.append(window)
            print(json.dumps({"phase": phase, "pass": True}), flush=True)
        if targets(repo) != api_targets:
            raise ValueError("API target changed")
        # Container identity freezes image and configuration across the five windows.
        for cid, role, path in containers:
            if call(["docker", "inspect", "-f", "{{.State.Running}}", cid]) != "true":
                raise ValueError("Container replaced during protocol")
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError, KeyboardInterrupt) as exc:
        failure = type(exc).__name__
    finally:
        for proc in children:
            if proc.poll() is None:
                proc.terminate()
        for proc in children:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        # A stopped docker-exec client alone is not proof its in-container child exited.
        cleanup = (
            "import os,signal,time;from pathlib import Path;prefix="
            + repr(inside)
            + ";owned=[]\nfor p in Path('/proc').glob('[0-9]*/cmdline'):\n try:args=p.read_bytes().split(bytes([0]))\n except OSError:continue\n if int(p.parent.name)!=os.getpid() and any(a.decode(errors='replace').startswith(prefix) for a in args):owned.append(int(p.parent.name))\nfor pid in owned:\n try:os.kill(pid,signal.SIGTERM)\n except ProcessLookupError:pass\ntime.sleep(0.5)\nfor pid in owned:\n if Path('/proc/'+str(pid)).exists():os.kill(pid,signal.SIGKILL)\nprint('owned instrument processes stopped')"
        )
        try:
            call(["docker", "exec", api, "python", "-c", cleanup])
            subprocess.run(
                ["docker", "exec", "-u", "0", api, "rm", "-f", *owned],
                check=True,
                capture_output=True,
                timeout=15,
            )
            for name in owned:
                subprocess.run(
                    ["docker", "exec", api, "test", "!", "-e", name],
                    check=True,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10,
                )
            clean = True
        except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
            failure = failure or type(exc).__name__
        for log in logs:
            log.close()
        result = {
            "pass": failure is None and len(phases) == 5 and clean,
            "failure_type": failure,
            "phases": phases,
            "effects": effects(phases) if len(phases) == 5 else None,
            "versions": versions,
            "images": {k: sorted(v) for k, v in images.items()},
            "replica_counts": counts,
            "observer_upload_sha256": observer_hash if "observer_hash" in locals() else None,
            "owned_processes_reaped": all(p.poll() is not None for p in children),
            "container_scratch_removed": clean,
            "profiler": profile,
            "no_buyer_traffic": True,
            "scope": "Idle mechanism measurement;not paid-load CPU attribution or production capacity.",
        }
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--observer-script", type=Path, required=True)
    args = parser.parse_args()

    def interrupted(*_):
        raise InterruptedError("Owned protocol interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGALRM, interrupted)
    signal.alarm(480)
    result = run(args.repo, args.fixture, args.output, args.observer_script)
    print(json.dumps({"protocol_pass": result["pass"], "failure_type": result["failure_type"]}), flush=True)
    raise SystemExit(0 if result["pass"] else 1)


if __name__ == "__main__":
    main()
