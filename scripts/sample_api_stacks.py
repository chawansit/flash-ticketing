"""Bounded host-side read-only stack sampling of four running API containers."""

import argparse
import json
import signal
import subprocess
import time
from pathlib import Path

COMPOSE = [
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


def process_start(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    return fields[19]


def require_uvicorn(pid):
    arguments = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    if b"ticketing.api:app" not in arguments or not any(
        value.rsplit(b"/", 1)[-1] == b"uvicorn" for value in arguments
    ):
        raise ValueError("API target is not the uvicorn application")


def targets(repo):
    ids = subprocess.check_output(COMPOSE + ["ps", "-q", "api"], cwd=repo, text=True, timeout=15).split()
    if len(ids) != 4 or len(set(ids)) != 4:
        raise ValueError("Exactly four API containers required")
    result = []
    for container in sorted(ids):
        state = json.loads(
            subprocess.check_output(
                ["docker", "inspect", "--format", "{{json .State}}", container], text=True, timeout=10
            )
        )
        pid = state["Pid"]
        if state["Status"] != "running" or state.get("Health", {}).get("Status") != "healthy" or pid <= 0:
            raise ValueError("API target unhealthy")
        require_uvicorn(pid)
        result.append({"container": container, "pid": pid, "start": process_start(pid)})
    if len({row["pid"] for row in result}) != 4:
        raise ValueError("API process identity duplicated")
    return result


def command(profiler, pid, path, seconds, rate):
    return [
        str(profiler),
        "record",
        "--pid",
        str(pid),
        "--output",
        str(path),
        "--format",
        "speedscope",
        "--duration",
        str(seconds),
        "--rate",
        str(rate),
        "--gil",
        "--nonblocking",
        "--threads",
    ]


def validate_bounds(seconds, rate):
    if not 1 <= seconds <= 120 or not 1 <= rate <= 25:
        raise ValueError("Profiler duration/rate outside approved bounds")


def child_cpu():
    import resource

    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def collect(repo, output, profiler, seconds=120, rate=25):
    validate_bounds(seconds, rate)
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    processes, files, result = [], [], {"pass": False, "profile_count": 0}
    try:
        selected = targets(repo)
        version = subprocess.check_output([str(profiler), "--version"], text=True, timeout=10).strip()
        start, cpu_start = time.monotonic(), child_cpu()
        for index, target in enumerate(selected):
            path = output / f"api-{index + 1}.speedscope.json"
            files.append(path)
            log = (output / f"api-{index + 1}.log").open("w")
            try:
                process = subprocess.Popen(
                    command(profiler, target["pid"], path, seconds, rate), stdout=log, stderr=log
                )
            finally:
                log.close()
            processes.append(process)
        deadline = start + seconds + 20
        while any(process.poll() is None for process in processes):
            if time.monotonic() >= deadline:
                raise TimeoutError("Profiler deadline exceeded")
            for target in selected:
                if process_start(target["pid"]) != target["start"]:
                    raise ValueError("API process identity changed during sampling")
            time.sleep(0.25)
        result.update(
            exit_codes=[process.returncode for process in processes],
            profiler_child_cpu_seconds=child_cpu() - cpu_start,
            elapsed_seconds=time.monotonic() - start,
            profiler_version=version,
            samples_per_second_per_api=rate,
            duration_seconds=seconds,
        )
        if any(process.returncode != 0 for process in processes):
            raise RuntimeError("Profiler collection failed")
        if targets(repo) != selected:
            raise ValueError("API container/process identity changed")
        if not all(path.is_file() and path.stat().st_size > 0 for path in files):
            raise ValueError("Missing profile")
        result.update(pass_=True, profile_count=len(files))
        result["pass"] = result.pop("pass_")
    except (
        OSError,
        ValueError,
        RuntimeError,
        TimeoutError,
        subprocess.SubprocessError,
        KeyboardInterrupt,
    ) as exc:
        result["error_type"] = type(exc).__name__
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        result["owned_profilers_exited"] = all(process.poll() is not None for process in processes)
        (output / "collection.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--profiler", required=True, type=Path)
    parser.add_argument("--seconds", type=int, default=120)
    parser.add_argument("--rate", type=int, default=25)
    args = parser.parse_args()

    def interrupted(_signum, _frame):
        raise InterruptedError("Profiler interrupted")

    signal.signal(signal.SIGTERM, interrupted)
    result = collect(args.repo, args.output, args.profiler, args.seconds, args.rate)
    print(json.dumps(result))
    raise SystemExit(0 if result["pass"] else 1)


if __name__ == "__main__":
    main()
