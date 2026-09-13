"""Collect bounded, credential-free Linux generator scheduling telemetry."""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * fraction) - 1]


def parse_cpu(text: str) -> dict[str, int]:
    line = next((row for row in text.splitlines() if row.startswith("cpu ")), None)
    if line is None:
        raise ValueError("Missing aggregate CPU row")
    names = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal", "guest", "guest_nice")
    values = [int(value) for value in line.split()[1:]]
    return dict(zip(names, values, strict=False))


def cpu_delta(previous: dict[str, int] | None, current: dict[str, int]) -> dict[str, float] | None:
    if previous is None:
        return None
    delta = {name: current.get(name, 0) - previous.get(name, 0) for name in current}
    # Linux already includes guest time in user/nice, so exclude the guest
    # columns from the aggregate to avoid counting those ticks twice.
    total = sum(value for name, value in delta.items() if name not in {"guest", "guest_nice"})
    if total <= 0:
        return None
    idle = delta.get("idle", 0) + delta.get("iowait", 0)
    return {
        "busy_pct": round((total - idle) * 100 / total, 3),
        "iowait_pct": round(delta.get("iowait", 0) * 100 / total, 3),
        "steal_pct": round(delta.get("steal", 0) * 100 / total, 3),
    }


def parse_pressure(text: str) -> dict[str, dict[str, float | int]]:
    result: dict[str, dict[str, float | int]] = {}
    for line in text.splitlines():
        parts = line.split()
        if not parts:
            continue
        values: dict[str, float | int] = {}
        for item in parts[1:]:
            key, raw = item.split("=", 1)
            values[key] = int(raw) if key == "total" else float(raw)
        result[parts[0]] = values
    return result


def parse_net_dev(text: str) -> dict[str, int]:
    totals = {"rx_bytes": 0, "rx_errors": 0, "rx_drops": 0, "tx_bytes": 0, "tx_errors": 0, "tx_drops": 0}
    for line in text.splitlines()[2:]:
        if ":" not in line:
            continue
        name, values_text = line.split(":", 1)
        if name.strip() == "lo":
            continue
        values = [int(value) for value in values_text.split()]
        if len(values) < 16:
            continue
        totals["rx_bytes"] += values[0]
        totals["rx_errors"] += values[2]
        totals["rx_drops"] += values[3]
        totals["tx_bytes"] += values[8]
        totals["tx_errors"] += values[10]
        totals["tx_drops"] += values[11]
    return totals


def parse_meminfo(text: str) -> dict[str, int]:
    wanted = {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}
    result: dict[str, int] = {}
    for line in text.splitlines():
        key, _, rest = line.partition(":")
        if key in wanted:
            result[f"{key.lower()}_kib"] = int(rest.strip().split()[0])
    return result


def _status_value(status: dict[str, str], name: str) -> int:
    raw = status.get(name, "0").split()[0]
    return int(raw)


def process_snapshot(match: str) -> dict[str, Any]:
    processes: list[dict[str, int]] = []
    needle = match.encode()
    for proc_dir in Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        try:
            command = (proc_dir / "cmdline").read_bytes()
            if needle not in command:
                continue
            stat = (proc_dir / "stat").read_text().strip()
            tail = stat[stat.rfind(")") + 2 :].split()
            status = {}
            for line in (proc_dir / "status").read_text().splitlines():
                key, separator, value = line.partition(":")
                if separator:
                    status[key] = value.strip()
            processes.append(
                {
                    "pid": int(proc_dir.name),
                    "cpu_ticks": int(tail[11]) + int(tail[12]),
                    "rss_kib": _status_value(status, "VmRSS"),
                    "threads": _status_value(status, "Threads"),
                    "voluntary_context_switches": _status_value(status, "voluntary_ctxt_switches"),
                    "nonvoluntary_context_switches": _status_value(status, "nonvoluntary_ctxt_switches"),
                }
            )
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError, IndexError):
            continue
    processes.sort(key=lambda row: row["pid"])
    return {"matched": len(processes), "processes": processes}


def process_delta(
    previous: dict[int, dict[str, int]], current: dict[str, Any], elapsed: float
) -> dict[str, float | int]:
    cpu_ticks = 0
    voluntary = 0
    nonvoluntary = 0
    for row in current["processes"]:
        old = previous.get(row["pid"])
        if old is None:
            continue
        cpu_ticks += max(0, row["cpu_ticks"] - old["cpu_ticks"])
        voluntary += max(0, row["voluntary_context_switches"] - old["voluntary_context_switches"])
        nonvoluntary += max(0, row["nonvoluntary_context_switches"] - old["nonvoluntary_context_switches"])
    ticks_per_second = os.sysconf("SC_CLK_TCK")
    return {
        "cpu_pct": round(cpu_ticks / ticks_per_second / elapsed * 100, 3) if elapsed > 0 else 0.0,
        "voluntary_context_switches": voluntary,
        "nonvoluntary_context_switches": nonvoluntary,
    }


def net_delta(previous: dict[str, int] | None, current: dict[str, int]) -> dict[str, int] | None:
    if previous is None:
        return None
    return {name: current[name] - previous[name] for name in current}


def read_pressure(kind: str) -> dict[str, dict[str, float | int]] | None:
    path = Path("/proc/pressure") / kind
    return parse_pressure(path.read_text()) if path.exists() else None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--match", required=True, help="Non-secret command-line substring for generator processes")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not Path("/proc/stat").exists():
        parser.error("Linux /proc is required")
    if not 1 <= args.seconds <= 7200 or not 0.2 <= args.interval <= 10:
        parser.error("Use 1..7200 seconds and a 0.2..10 second interval")
    if args.output.exists():
        parser.error("Use a fresh output file")
    if len(args.match) < 4:
        parser.error("Use a specific process match of at least four characters")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    end = start + args.seconds
    due = start
    previous_cpu: dict[str, int] | None = None
    previous_net: dict[str, int] | None = None
    previous_processes: dict[int, dict[str, int]] = {}
    previous_time: float | None = None
    wake_lags: list[float] = []
    errors = 0
    samples = 0

    with args.output.open("x", encoding="utf-8") as output:
        output.write(json.dumps({"type": "metadata", "utc": datetime.now(UTC).isoformat(), "seconds": args.seconds,
                                 "interval_seconds": args.interval, "match_configured": True}) + "\n")
        output.flush()
        while time.monotonic() < end:
            now = time.monotonic()
            wake_lag_ms = max(0.0, (now - due) * 1000)
            wake_lags.append(wake_lag_ms)
            row: dict[str, Any] = {
                "type": "sample",
                "utc": datetime.now(UTC).isoformat(),
                "elapsed_seconds": round(now - start, 6),
                "observer_wake_lag_ms": round(wake_lag_ms, 3),
            }
            try:
                cpu = parse_cpu(Path("/proc/stat").read_text())
                network = parse_net_dev(Path("/proc/net/dev").read_text())
                processes = process_snapshot(args.match)
                elapsed = now - previous_time if previous_time is not None else None
                row.update(
                    {
                        "loadavg": [float(value) for value in Path("/proc/loadavg").read_text().split()[:3]],
                        "memory": parse_meminfo(Path("/proc/meminfo").read_text()),
                        "cpu": cpu_delta(previous_cpu, cpu),
                        "pressure": {kind: read_pressure(kind) for kind in ("cpu", "memory", "io")},
                        "network": network,
                        "network_delta": net_delta(previous_net, network),
                        "generator": processes,
                        "generator_delta": process_delta(previous_processes, processes, elapsed)
                        if elapsed is not None and elapsed > 0
                        else None,
                    }
                )
                previous_cpu = cpu
                previous_net = network
                previous_processes = {row["pid"]: row for row in processes["processes"]}
                previous_time = now
            except Exception as exc:  # noqa: BLE001 - retain bounded observer failure as evidence
                row["error"] = type(exc).__name__
                errors += 1
            output.write(json.dumps(row, separators=(",", ":")) + "\n")
            output.flush()
            samples += 1
            due += args.interval
            time.sleep(max(0.0, due - time.monotonic()))

        summary = {
            "type": "summary",
            "utc": datetime.now(UTC).isoformat(),
            "samples": samples,
            "errors": errors,
            "observer_wake_lag_p95_ms": percentile(wake_lags, 0.95),
            "observer_wake_lag_p99_ms": percentile(wake_lags, 0.99),
            "observer_wake_lag_max_ms": max(wake_lags, default=None),
        }
        output.write(json.dumps(summary, separators=(",", ":")) + "\n")
        output.flush()
    print(json.dumps(summary))
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())


