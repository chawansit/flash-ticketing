"""ADR0232 fixed hourly coordinator; unchanged frozen leaf scheduling and aggregation."""

import argparse
import asyncio
import json
import math
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from time import time


def validate(args, manifest):
    if (
        args.rate != 84
        or args.seconds != 3600
        or args.completion_deadline_seconds != 3720
        or args.concurrency != 500
        or args.http_client_count != 8
        or args.poll_seconds != 1
        or args.duplicates != 1
        or args.origin != "http://10.1.137.69:8000"
        or not math.isfinite(args.start_at_epoch)
        or not 3 <= args.start_at_epoch - time() <= 60
    ):
        raise ValueError("Exact bounded hourly coordinator settings required")
    if (
        manifest.get("schema_version") != 1
        or manifest.get("environment") != "development"
        or manifest.get("origin") != args.origin
        or manifest.get("fixture_layout") != "distributed"
        or len(manifest.get("show_ids", [])) != 1008
        or len(set(manifest["show_ids"])) != 1008
        or len(manifest.get("viewer_tokens", [])) != 302400
        or len(set(manifest["viewer_tokens"])) != 302400
        or manifest.get("seats_per_show") != 300
        or manifest.get("seat_offset") != 0
    ):
        raise ValueError("Exact distinct hourly cohort required")


async def run(args, manifest):
    import paid_ticket_sharded_generator as frozen

    validate(args, manifest)
    parts = frozen.split_manifest(manifest, 2, 151200)
    jobs, outputs, results, exits = [], [], [], []
    with tempfile.TemporaryDirectory(prefix="paid-hourly-shards-", dir=Path(__file__).parent) as private:
        os.chmod(private, 0o700)
        try:
            for index, part in enumerate(parts):
                source, output = (
                    Path(private) / f"manifest-{index}.json",
                    Path(private) / f"result-{index}.json",
                )
                with source.open("x", encoding="utf-8") as stream:
                    os.chmod(source, 0o600)
                    json.dump(part, stream)
                outputs.append(output)
                command = [
                    sys.executable,
                    str(Path(__file__).with_name("cce_hourly_paid_leaf.py")),
                    "--manifest",
                    str(source),
                    "--origin",
                    args.origin,
                    "--output",
                    str(output),
                    "--rate",
                    "42",
                    "--seconds",
                    "3600",
                    "--completion-deadline-seconds",
                    "3720",
                    "--concurrency",
                    "250",
                    "--http-max-connections",
                    "250",
                    "--http-client-count",
                    "8",
                    "--poll-seconds",
                    "1",
                    "--duplicates",
                    "1",
                    "--start-at-epoch",
                    str(args.start_at_epoch),
                ]
                if args.lifecycle_diagnostics:
                    command.append("--lifecycle-diagnostics")
                jobs.append(
                    await asyncio.create_subprocess_exec(
                        *command, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
                    )
                )
            exits = list(await asyncio.wait_for(asyncio.gather(*(job.wait() for job in jobs)), timeout=3750))
        finally:
            for job in jobs:
                if job.returncode is None:
                    job.terminate()
            try:
                await asyncio.wait_for(asyncio.gather(*(job.wait() for job in jobs)), timeout=5)
            except TimeoutError:
                for job in jobs:
                    if job.returncode is None:
                        job.kill()
                await asyncio.gather(*(job.wait() for job in jobs))
        results = [json.loads(path.read_text()) if path.exists() else {} for path in outputs]
    result = frozen.aggregate(results, 84, 3600, 500, exits)
    starts = [datetime.fromisoformat(row["started_at_utc"]).timestamp() for row in results]
    result["shard_start_alignment_pass"] = len(starts) == 2 and all(abs(value - args.start_at_epoch) <= 2 for value in starts)
    result["pass"] = result.get("pass") is True and result["shard_start_alignment_pass"]
    result.update(
        fixture_layout="distributed",
        allocation="12_consecutive_84_show_blocks",
        common_start_epoch=args.start_at_epoch,
        http_clients_per_shard=[row.get("http_client_count", 1) for row in results],
        http_connection_budgets_per_shard=[row.get("http_connection_budgets") for row in results],
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--origin", required=True)
    for name in (
        "rate",
        "seconds",
        "completion-deadline-seconds",
        "concurrency",
        "http-client-count",
        "duplicates",
    ):
        parser.add_argument("--" + name, type=int, required=True)
    parser.add_argument("--poll-seconds", type=float, required=True)
    parser.add_argument("--start-at-epoch", type=float, required=True)
    parser.add_argument("--lifecycle-diagnostics", action="store_true")
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Fresh hourly output required")
    result = asyncio.run(run(args, json.loads(args.manifest.read_text())))
    with args.output.open("x") as stream:
        json.dump(result, stream, indent=2)
    print(
        json.dumps(
            {
                "pass": result.get("pass"),
                "scheduled": result.get("scheduled"),
                "fulfilled": result.get("fulfilled"),
            }
        ),
        flush=True,
    )
    if result.get("pass") is not True:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
