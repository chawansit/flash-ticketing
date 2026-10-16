"""ADR0332 synthetic-only process calibration; never production capacity evidence."""

import argparse
import asyncio
import hashlib
import json
import math
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import time
from urllib.parse import urlsplit
from uuid import uuid4

from diagnose_paid_generator import Responder

RUNTIME_FILES = (
    "calibrate_generator_processes.py",
    "diagnose_paid_generator.py",
    "paid_ticket_load_generator.py",
    "paid_ticket_sharded_generator.py",
    "checkout_journey_probe.py",
    "paid_fixture_layout.py",
    "customer_recovery_client.py",
)


def prepare_runtime(directory):
    """Reconstruct the qualified paid leaf; never silently use the development leaf."""
    from cce_paid_stage import generator_bundle
    from customer_gateway_read_bundle import qualify as gateway
    from customer_recovery_bundle import qualify as recovery
    from customer_timeout_bundle import qualify as timeout

    bundle, coordinator = recovery(generator_bundle())
    bundle, coordinator = timeout(bundle, coordinator)
    bundle, coordinator = gateway(bundle, coordinator)
    directory.mkdir(mode=0o700)
    hashes = {}
    for name in RUNTIME_FILES:
        data = bundle.get("scripts/" + name)
        if data is None:
            data = Path(__file__).with_name(name).read_bytes()
        (directory / name).write_bytes(data)
        hashes[name] = hashlib.sha256(data).hexdigest()
    receipt = {
        "decision": "ADR0332",
        "workload": "sealed_paid_recovery_timeout_gateway_leaf",
        "source_sha256": hashes,
    }
    (directory / "runtime-source-receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    return receipt


def verify_runtime(directory):
    receipt = json.loads((directory / "runtime-source-receipt.json").read_text(encoding="utf-8"))
    hashes = receipt.get("source_sha256", {})
    if (
        receipt.get("decision") != "ADR0332"
        or set(hashes) != set(RUNTIME_FILES)
        or receipt.get("workload") != "sealed_paid_recovery_timeout_gateway_leaf"
        or any(
            (directory / name).is_symlink()
            or hashlib.sha256((directory / name).read_bytes()).hexdigest() != digest
            for name, digest in hashes.items()
        )
    ):
        raise ValueError("Exact prepared synthetic runtime required before any dispatch")
    return receipt


def allocation(origin, rate, seconds, shards):
    if shards not in (2, 4) or rate not in (4, 160, 200) or rate % shards:
        raise ValueError("Explicit bounded synthetic profile required")
    address = urlsplit(origin)
    if (
        not 1 <= seconds <= 45
        or address.scheme != "http"
        or address.hostname != "127.0.0.1"
        or address.username
        or address.password
        or not address.port
        or address.path
        or address.query
        or address.fragment
    ):
        raise ValueError("Loopback-only bounded calibration required")
    shows = shards * math.ceil(rate * seconds / (300 * shards))
    base = {
        "schema_version": 1,
        "environment": "development",
        "origin": origin,
        "expires_at": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
        "fixture_layout": "distributed",
        "seats_per_show": 300,
        "seat_offset": 0,
    }
    show_ids = [str(uuid4()) for _ in range(shows)]
    return [
        {
            **base,
            "show_ids": show_ids[i::shards],
            "viewer_tokens": [f"synthetic-{i}-{j}" for j in range(rate * seconds // shards)],
        }
        for i in range(shards)
    ]


def verdict(rows, codes, expected, stats):
    responder_valid = (
        stats["protocol_errors"] == stats["duplicate_seat_attempts"] == 0
        and isinstance(stats["loop_lag_p95_ms"], (int, float))
        and isinstance(stats["handler_p95_ms"], (int, float))
        and stats["loop_lag_p95_ms"] <= 10
        and stats["handler_p95_ms"] <= 50
    )
    complete = (
        len(rows) == len(codes)
        and len(rows) in (2, 4)
        and all(code == 0 for code in codes)
        and all(row.get("pass") is True for row in rows)
        and all(
            sum(row.get(k, -expected) for row in rows) == expected
            for k in (
                "scheduled",
                "dispatched",
                "completed",
                "fulfilled_by_deadline",
                "distinct_orders",
                "distinct_tickets",
            )
        )
        and sum(row.get("generator_drops", expected) for row in rows) == 0
        and stats["synthetic_orders"] == stats["unique_assigned_seats"] == expected
    )
    pacing = all(
        isinstance(row.get("dispatch_lag_p95_ms"), (int, float))
        and isinstance(row.get("dispatch_lag_max_ms"), (int, float))
        and row["dispatch_lag_p95_ms"] <= 100
        and row["dispatch_lag_max_ms"] <= 500
        for row in rows
    )
    return {
        "responder_valid": responder_valid,
        "complete_unique_accounting": complete,
        "dispatch_timing": pacing,
        "pass": responder_valid and complete and pacing,
    }


async def control(directory, rate, seconds, shards):
    directory.mkdir(mode=0o700)
    jobs, outputs, handles = [], [], []
    async with Responder(30, 0.5, 0.5) as server:
        parts = allocation(server.origin, rate, seconds, shards)
        start = time() + 8
        try:
            for i, part in enumerate(parts):
                source, output = directory / f"manifest-{i}.json", directory / f"result-{i}.json"
                source.write_text(json.dumps(part), encoding="utf-8")
                os.chmod(source, 0o600)
                outputs.append(output)
                error = (directory / f"stderr-{i}.log").open("wb")
                handles.append(error)
                command = [
                    sys.executable,
                    str(Path(__file__).with_name("paid_ticket_load_generator.py")),
                    "--manifest",
                    str(source),
                    "--origin",
                    server.origin,
                    "--output",
                    str(output),
                    "--rate",
                    str(rate // shards),
                    "--seconds",
                    str(seconds),
                    "--completion-deadline-seconds",
                    str(seconds + 30),
                    "--concurrency",
                    str(500 // shards),
                    "--http-max-connections",
                    str(500 // shards),
                    "--http-client-count",
                    str(16 // shards),
                    "--poll-seconds",
                    "1",
                    "--duplicates",
                    "1",
                    "--recovery-max-attempts",
                    "3",
                    "--lifecycle-diagnostics",
                    "--start-at-epoch",
                    str(start),
                ]
                jobs.append(
                    await asyncio.create_subprocess_exec(
                        *command, stdout=asyncio.subprocess.DEVNULL, stderr=error
                    )
                )
            codes = await asyncio.wait_for(asyncio.gather(*(job.wait() for job in jobs)), seconds + 50)
        finally:
            for job in jobs:
                if job.returncode is None:
                    job.terminate()
            try:
                await asyncio.wait_for(asyncio.gather(*(job.wait() for job in jobs)), 5)
            except TimeoutError:
                for job in jobs:
                    if job.returncode is None:
                        job.kill()
                await asyncio.gather(*(job.wait() for job in jobs))
            for handle in handles:
                handle.close()
        rows = [json.loads(path.read_text(encoding="utf-8")) if path.exists() else {} for path in outputs]
        stats = server.stats()
    report = {
        "rate": rate,
        "seconds": seconds,
        "shards": shards,
        "concurrency": 500,
        "http_clients": 16,
        "http_connections": 500,
        "poll_seconds": 1,
        "response_ms": 30,
        "command_seconds": 0.5,
        "ticket_seconds": 0.5,
        "exit_codes": codes,
        "responder": stats,
        "shard_results": rows,
        "gates": verdict(rows, codes, rate * seconds, stats),
    }
    (directory / "control.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


async def run(output, seconds):
    output.mkdir(mode=0o700)
    results = []
    for rate, shards in ((160, 2), (160, 4), (200, 4)):
        if rate == 200 and not results[-1]["gates"]["pass"]:
            break
        result = await control(output / f"rate-{rate}-shards-{shards}", rate, seconds, shards)
        results.append(result)
        print(json.dumps({"rate": rate, "shards": shards, "gates": result["gates"]}), flush=True)
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in Path(__file__).parent.glob("*.py")}
    report = {
        "decision": "ADR0332",
        "kind": "synthetic_generator_calibration",
        "production_capacity_evidence": False,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "logical_cpus": os.cpu_count(),
        "source_sha256": hashes,
        "controls": results,
        "application_or_cloud_resources_changed": False,
    }
    (output / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--output", type=Path)
    group.add_argument("--prepare-runtime", type=Path)
    parser.add_argument("--seconds", type=int, choices=range(1, 46), default=45)
    args = parser.parse_args()
    if args.prepare_runtime:
        print(json.dumps(prepare_runtime(args.prepare_runtime)), flush=True)
        return
    verify_runtime(Path(__file__).parent)
    if args.output.exists():
        parser.error("Fresh owned output directory required")
    asyncio.run(asyncio.wait_for(run(args.output, args.seconds), 300))


if __name__ == "__main__":
    main()
