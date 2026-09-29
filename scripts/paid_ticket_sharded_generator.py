"""Run two synchronized, isolated paid-journey generators on one development ECS."""

import argparse
import asyncio
import json
import os
import sys
import tempfile
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from time import time

LATENCIES = (
    "dispatch_lag_p95_ms", "dispatch_lag_max_ms", "hold_http_p95_ms", "hold_app_p95_ms",
    "hold_client_excess_p95_ms", "command_durable_wait_p95_ms", "durable_p95_ms",
    "payment_http_p95_ms", "payment_app_p95_ms", "payment_client_excess_p95_ms",
    "ticket_wait_p95_ms", "payment_to_ticket_p95_ms", "hold_to_ticket_p95_ms",
)
COUNTS = (
    "scheduled", "dispatched", "generator_drops", "completed", "fulfilled",
    "fulfilled_by_deadline", "distinct_orders", "distinct_tickets", "retry_attempts",
    "hold_app_timing_samples", "payment_app_timing_samples",
)


def split_manifest(manifest, shards, scheduled_per_shard):
    if manifest.get("schema_version") != 1 or manifest.get("environment") != "development":
        raise ValueError("Development-only private manifest required")
    shows = manifest.get("show_ids", [])
    tokens = manifest.get("viewer_tokens", [])
    if len(shows) != len(set(shows)) or len(shows) < shards or len(tokens) < shards:
        raise ValueError("Fixture must have distinct shows and enough viewers")
    result = []
    for index in range(shards):
        subset = {**manifest, "show_ids": shows[index::shards], "viewer_tokens": tokens[index::shards]}
        available = len(subset["show_ids"]) * (manifest["seats_per_show"] - manifest["seat_offset"])
        if scheduled_per_shard > available:
            raise ValueError("A shard has insufficient distinct seats")
        result.append(subset)
    return result


def aggregate(shard_results, rate, seconds, concurrency, exit_codes):
    result = {
        "kind": "scheduled_paid_ticket_journeys_sharded",
        "measured_at_utc": datetime.now(UTC).isoformat(),
        "rate_target_per_second": rate,
        "dispatch_seconds": seconds,
        "generator_shards": len(shard_results),
        "generator_max_in_flight": concurrency,
        "latency_summary_mode": "worst_shard_p95_not_combined_percentile",
        "shard_exit_codes": exit_codes,
    }
    for name in COUNTS:
        result[name] = sum(row.get(name, 0) for row in shard_results)
    outcomes = Counter()
    attempts = Counter()
    for row in shard_results:
        outcomes.update(row.get("outcomes", {}))
        attempts.update(row.get("physical_http_attempts", {}))
    result["outcomes"] = dict(outcomes)
    result["physical_http_attempts"] = dict(attempts)
    for name in LATENCIES:
        values = [row[name] for row in shard_results if row.get(name) is not None]
        result[name] = max(values) if values else None
    for field in ("transport_phase_p95_ms", "transport_phase_samples"):
        result[field] = {}
        for route in ("holds", "payments"):
            result[field][route] = {}
            for phase in ("pre_send_ms", "response_wait_ms", "connect_ms"):
                values = [row.get(field, {}).get(route, {}).get(phase) for row in shard_results]
                values = [value for value in values if value is not None]
                result[field][route][phase] = (
                    sum(values) if field.endswith("samples") else max(values) if values else None
                )
    result["shards"] = [
        {
            "scheduled": row.get("scheduled"),
            "dispatched": row.get("dispatched"),
            "generator_drops": row.get("generator_drops"),
            "fulfilled_by_deadline": row.get("fulfilled_by_deadline"),
            "started_at_utc": row.get("started_at_utc"),
            "dispatch_lag_p95_ms": row.get("dispatch_lag_p95_ms"),
            "transport_phase_p95_ms": row.get("transport_phase_p95_ms"),
            "outcomes": row.get("outcomes"),
            "pass": row.get("pass", False),
        }
        for row in shard_results
    ]
    result["pass"] = (
        len(shard_results) == 2
        and exit_codes == [0, 0]
        and all(row.get("pass") for row in shard_results)
        and result["scheduled"] == rate * seconds
        and result["scheduled"] == result["dispatched"] == result["fulfilled_by_deadline"]
        and result["generator_drops"] == 0
        and result["retry_attempts"] == 0
    )
    return result


async def run(args, manifest):
    if args.rate <= 0 or args.rate % 2 or args.concurrency <= 0 or args.concurrency % 2:
        raise ValueError("Two shards require even positive rate and concurrency")
    parts = split_manifest(manifest, 2, args.rate // 2 * args.seconds)
    start_at = time() + 8
    results = []
    exit_codes = []
    with tempfile.TemporaryDirectory(prefix="paid-shards-", dir=Path(__file__).parent) as private:
        os.chmod(private, 0o700)
        processes = []
        paths = []
        for index, part in enumerate(parts):
            manifest_path = Path(private) / f"manifest-{index}.json"
            output_path = Path(private) / f"result-{index}.json"
            manifest_path.write_text(json.dumps(part), encoding="utf-8")
            os.chmod(manifest_path, 0o600)
            paths.append(output_path)
            proc = await asyncio.create_subprocess_exec(
                sys.executable, str(Path(__file__).with_name("paid_ticket_load_generator.py")),
                "--manifest", str(manifest_path), "--origin", args.origin,
                "--output", str(output_path), "--rate", str(args.rate // 2),
                "--seconds", str(args.seconds),
                "--completion-deadline-seconds", str(args.completion_deadline_seconds),
                "--concurrency", str(args.concurrency // 2),
                "--http-max-connections", str(args.concurrency // 2),
                "--poll-seconds", str(args.poll_seconds),
                "--duplicates", str(args.duplicates),
                "--start-at-epoch", str(start_at),
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
            )
            processes.append(proc)
        try:
            exit_codes = list(await asyncio.gather(*(proc.wait() for proc in processes)))
        finally:
            for proc in processes:
                if proc.returncode is None:
                    proc.terminate()
            await asyncio.gather(*(proc.wait() for proc in processes))
        for output_path in paths:
            results.append(json.loads(output_path.read_text(encoding="utf-8")) if output_path.exists() else {})
    return aggregate(results, args.rate, args.seconds, args.concurrency, exit_codes)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--manifest", required=True, type=Path)
    p.add_argument("--origin", required=True)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--rate", required=True, type=int)
    p.add_argument("--seconds", required=True, type=int)
    p.add_argument("--completion-deadline-seconds", required=True, type=int)
    p.add_argument("--concurrency", required=True, type=int)
    p.add_argument("--poll-seconds", required=True, type=float)
    p.add_argument("--duplicates", required=True, type=int)
    args = p.parse_args()
    if args.output.exists() or args.seconds < 1 or args.seconds > 300:
        p.error("Fresh output and bounded duration required")
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    result = asyncio.run(run(args, manifest))
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"pass": result["pass"], "scheduled": result["scheduled"], "dispatched": result["dispatched"]}))
    if not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
