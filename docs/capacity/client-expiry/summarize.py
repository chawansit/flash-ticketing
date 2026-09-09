"""Reproduce ABBA latency, connection churn and CPU evidence."""

import json
from collections import Counter
from datetime import datetime, timedelta
from itertools import pairwise
from pathlib import Path
from statistics import mean

ROOT = Path(__file__).parent


def read(path):
    return json.loads(path.read_text())


def window(rows, start, end):
    return [r for r in rows if start <= datetime.fromisoformat(r["utc"]) <= end]


def stats(values):
    return {"samples": len(values), "mean": mean(values), "max": max(values)} if values else {"samples": 0}


def summarize():
    cpu = read(ROOT / "backend/expiry-cpu-snapshot.json")
    observer = read(ROOT / "backend/expiry-observer.json")
    results = []
    for name, expiry in [("a1", 5), ("b1", 2), ("b2", 2), ("a2", 5)]:
        directory = ROOT / f"generator/expiry-{name}"
        coordination = read(directory / "summary.json")
        workers = [read(directory / f"worker-{i}.json") for i in range(4)]
        assert coordination["keepalive_expiry_seconds"] == expiry
        assert all(
            w["keepalive_expiry_seconds"] == expiry and w["transport_diagnostics_enabled"] for w in workers
        )
        start = min(datetime.fromisoformat(w["measured_started_utc"]) for w in workers)
        end = max(
            datetime.fromisoformat(w["measured_started_utc"]) + timedelta(seconds=w["elapsed_seconds"])
            for w in workers
        )
        samples = window(cpu, start, end)
        host_busy = []
        for left, right in pairwise(samples):
            a = list(map(int, left["host_cpu_ticks"].split()[1:9]))
            b = list(map(int, right["host_cpu_ticks"].split()[1:9]))
            delta = [y - x for x, y in zip(a, b, strict=True)]
            if sum(delta) > 0:
                host_busy.append(100 * (sum(delta) - delta[3] - delta[4]) / sum(delta))
        api_cpu = [
            float(c["CPUPerc"].rstrip("%"))
            for r in samples
            for c in r.get("containers", [])
            if c["Name"] == "flash-cloud-bench-api-1"
        ]
        observed = window(observer, start, end)
        valid = [r for r in observed if "error" not in r]
        statuses, connects, errors = Counter(), Counter(), Counter()
        for worker in workers:
            for op, counts in worker["statuses"].items():
                statuses.update({f"{op}:{status}": count for status, count in counts.items()})
            for counts in worker["measured_tcp_connect_events"].values():
                connects.update(counts)
            errors.update(worker["transport_error_types"])
            assert worker["accounting_pass"]
        completed = sum(statuses.values())
        assert (
            completed + sum(w["generator_drops"] + sum(w["task_error_types"].values()) for w in workers)
            == 120000
        )
        results.append(
            {
                "name": name,
                "expiry_seconds": expiry,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "gate_pass": coordination["gate_pass"],
                "statuses": dict(statuses),
                "transport_errors": dict(errors),
                "generator_drops": sum(w["generator_drops"] for w in workers),
                "worst_worker_read_p95_ms": coordination["worst_worker_read_p95_ms"],
                "worst_worker_hold_p95_ms": coordination["worst_worker_hold_p95_ms"],
                "connect_events": dict(connects),
                "connect_attempts_per_1000_completed": connects["started"] * 1000 / completed,
                "generator_cpu_seconds": sum(w["generator_cpu_seconds"] for w in workers),
                "api_cpu_percent_one_core_100": stats(api_cpu),
                "host_busy_percent": stats(host_busy),
                "cpu_sample_errors": sum("error" in r for r in samples),
                "observer_samples": len(observed),
                "observer_first_utc": observed[0]["utc"] if observed else None,
                "observer_last_utc": observed[-1]["utc"] if observed else None,
                "observer_errors": len(observed) - len(valid),
                "max_database_connections": max((r["database_connections"] for r in valid), default=None),
                "max_lock_waiters": max((r["lock_waiters"] for r in valid), default=None),
                "max_missing_maps": max((r["missing_maps"] for r in valid), default=None),
                "max_overdue_holds": max((r["overdue_active_holds"] for r in valid), default=None),
                "max_expiry_delay_seconds": max((r["oldest_overdue_seconds"] for r in valid), default=None),
            }
        )
    return results


if __name__ == "__main__":
    result = summarize()
    (ROOT / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
