"""Parallel load runner for cloud-like load. Split work across local workers and keep per-worker diagnostics."""

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

from http_load_generator import expiry_seconds

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--manifest", type=Path, required=True)
p.add_argument("--output", type=Path, required=True)
p.add_argument("--rate", type=int, required=True)
p.add_argument("--seconds", type=int, required=True)
p.add_argument("--workers", type=int, default=4)
p.add_argument("--seat-offset", type=int, default=0)
p.add_argument("--hot-reads", action="store_true")
p.add_argument("--mixed-hot-holds", action="store_true")
p.add_argument("--burst", action="store_true")
p.add_argument("--transport-diagnostics", action="store_true")
p.add_argument("--keepalive-expiry", type=expiry_seconds, default=5.0)
a = p.parse_args()
if a.burst:
    a.seconds = 240

generator = Path("http_load_generator.py")
if not generator.exists():
    generator = Path(__file__).resolve().parent / "http_load_generator.py"

m = json.loads(a.manifest.read_text(encoding="utf-8"))
if (
    a.workers < 1
    or a.rate < a.workers
    or a.seconds < 1
    or len(m["show_ids"]) % a.workers
    or len(m["viewer_tokens"]) % len(m["show_ids"])
):
    p.error("Positive limits, rate >= workers, and evenly partitioned shows/viewers required")
worker_rates = [a.rate // a.workers + (i < a.rate % a.workers) for i in range(a.workers)]


a.output.mkdir(parents=True, exist_ok=True)
start = (datetime.now(UTC) + timedelta(seconds=30)).isoformat()
processes = []
private_directory = TemporaryDirectory(prefix="flash-load-")
try:
    for i in range(a.workers):
        shard = dict(
            m,
            show_ids=m["show_ids"][i :: a.workers],
            viewer_tokens=m["viewer_tokens"][i :: a.workers],
            seat_offset=a.seat_offset,
        )
        if a.mixed_hot_holds:
            shard["hot_hold_show_id"] = m["show_ids"][0]
            shard["hot_hold_seat"] = 299
        if a.hot_reads:
            shard["hot_show_id"] = m["show_ids"][0]
        credentials = Path(private_directory.name) / f"private-worker-{i}.json"
        credentials.write_text(json.dumps(shard), encoding="utf-8")
        credentials.chmod(0o600)
        log = (a.output / f"worker-{i}.log").open("w", encoding="utf-8")
        result_path = a.output / f"worker-{i}.json"
        if result_path.exists():
            raise ValueError("Use a fresh output directory; retain earlier evidence")
        proc = subprocess.Popen(
            [
                sys.executable,
                str(generator),
                "--manifest",
                str(credentials),
                "--origin",
                m["origin"],
                "--rate",
                str(worker_rates[i]),
                "--seconds",
                str(a.seconds),
                "--start-at",
                start,
                "--keepalive-expiry",
                str(a.keepalive_expiry),
                "--topology",
                "separate-host",
                "--output",
                str(result_path),
            ]
            + (["--burst"] if a.burst else [])
            + (["--transport-diagnostics"] if a.transport_diagnostics else [])
            + (["--mixed-hot-holds"] if a.mixed_hot_holds else []),
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        processes.append((proc, log, credentials, result_path))

    codes = [proc.wait() for proc, _, _, _ in processes]
    results = [json.loads(path.read_text()) for _, _, _, path in processes if path.exists()]
    skew = None
    if len(results) == a.workers:
        starts = [datetime.fromisoformat(r["measured_started_utc"]) for r in results]
        skew = (max(starts) - min(starts)).total_seconds() * 1000

    transport_event_counts = {}
    for r in results:
        for op, events in (r.get("transport_phase_counts") or {}).items():
            transport_event_counts.setdefault(op, {})
            for phase, count in events.items():
                transport_event_counts[op][phase] = transport_event_counts[op].get(phase, 0) + count

    transport_error_types = set()
    for r in results:
        transport_error_types.update((r.get("transport_error_types") or {}).keys())

    coordination_gate_pass = len(results) == a.workers and not any(codes) and skew is not None and skew <= 100
    workload_gate_pass = coordination_gate_pass and all(
        result.get("workload_gate_pass") is True for result in results
    )
    summary = {
        "utc": datetime.now(UTC).isoformat(),
        "hot_read_share": 0.9 if a.hot_reads else 0,
        "burst": a.burst,
        "mixed_hot_holds": a.mixed_hot_holds,
        "keepalive_expiry_seconds": a.keepalive_expiry,
        "rate": a.rate,
        "seconds": a.seconds,
        "workers": a.workers,
        "worker_rates": worker_rates,
        "worker_exit_codes": codes,
        "start_skew_ms": skew,
        "generator_drops": sum(r.get("generator_drops", 0) for r in results),
        "worst_worker_read_p95_ms": max(
            (r.get("read_p95_ms") for r in results if r.get("read_p95_ms") is not None), default=None
        ),
        "worst_worker_hold_p95_ms": max(
            (r.get("hold_p95_ms") for r in results if r.get("hold_p95_ms") is not None), default=None
        ),
        "transport_phase_counts": transport_event_counts,
        "transport_errors": {
            error_type: sum(r.get("transport_error_types", {}).get(error_type, 0) for r in results)
            for error_type in sorted(transport_error_types)
        },
        "coordination_gate_pass": coordination_gate_pass,
        "workload_gate_pass": workload_gate_pass,
        "gate_pass": workload_gate_pass,
        "note": "Disjoint show/viewer partitions. Percentiles are worst-worker values, not merged percentiles.",
    }
    (a.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary), flush=True)
    if not summary["gate_pass"]:
        raise SystemExit(1)
finally:
    for proc, log, credentials, _ in processes:
        if proc.poll() is None:
            proc.terminate()
            proc.wait()
        log.close()
        credentials.unlink(missing_ok=True)

    private_directory.cleanup()
