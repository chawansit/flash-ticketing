"""Portable HTTP-only load generator. Requires httpx; no application/DB/Redis imports."""

import argparse
import asyncio
import json
import math
import platform
import random
import socket
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter, process_time
from uuid import uuid4

import httpx


def percentile(values, fraction):
    return sorted(values)[math.ceil(len(values) * fraction) - 1] if values else None


async def run(args):
    manifest = json.loads(args.manifest.read_text())
    if manifest.get("schema_version") != 1 or manifest.get("environment") != "development":
        raise ValueError("Expected a development manifest")
    if datetime.fromisoformat(manifest["expires_at"]) <= datetime.now(UTC) + timedelta(
        seconds=args.seconds + 60
    ):
        raise ValueError("Regenerate credentials: they must outlive the run and drain")
    if args.origin.rstrip("/") != manifest["origin"]:
        raise ValueError("API origin must match the locally prepared manifest")
    if args.output.resolve() == args.manifest.resolve():
        raise ValueError("Result must not overwrite credentials")
    shows, tokens = manifest["show_ids"], manifest["viewer_tokens"]
    statuses, latency = defaultdict(Counter), defaultdict(list)
    counts, bytes_received, drops = Counter(), 0, 0
    transport_errors = Counter()
    validators, pending, tasks, lags = {}, set(), [], []
    run_id = str(uuid4())
    async with httpx.AsyncClient(
        base_url=args.origin,
        timeout=10,
        limits=httpx.Limits(max_connections=args.inflight, max_keepalive_connections=args.inflight),
    ) as client:
        (await client.get("/health/ready")).raise_for_status()
        initial = {}
        for show in shows:
            response = await client.get(f"/v1/events/{show}/availability")
            response.raise_for_status()
            initial[show] = response.headers["etag"]
        validators = {viewer: initial[shows[viewer % len(shows)]] for viewer in range(len(tokens))}
        randomizer = random.Random(42)
        workload = []
        for index in range(args.rate * args.seconds):
            viewer = randomizer.randrange(len(tokens))
            show = shows[viewer % len(shows)]
            write = index % 20 == 0
            seat = manifest["seat_offset"] + counts[show]
            if write:
                counts[show] += 1
                if seat >= manifest["seats_per_show"]:
                    raise ValueError("Allocated fixture seats exhausted")
            workload.append((index, viewer, show, write, seat))

        async def request(item):
            nonlocal bytes_received
            index, viewer, show, write, seat = item
            operation = "hold" if write else "read"
            begin = perf_counter()
            try:
                if write:
                    response = await client.post(
                        "/v1/holds",
                        json={"event_id": show, "seat_ids": [f"S{seat}"]},
                        headers={
                            "Authorization": "Bearer " + tokens[viewer],
                            "Idempotency-Key": f"{run_id}-{index}",
                        },
                    )
                else:
                    response = await client.get(
                        f"/v1/events/{show}/availability", headers={"If-None-Match": validators[viewer]}
                    )
                    if response.status_code == 200:
                        validators[viewer] = response.headers["etag"]
                status = str(response.status_code)
                bytes_received += len(response.content)
            except httpx.HTTPError as exc:
                status = "transport_error"
                transport_errors[type(exc).__name__] += 1
            statuses[operation][status] += 1
            latency[operation + ":" + status].append((perf_counter() - begin) * 1000)

        if datetime.fromisoformat(manifest["expires_at"]) <= datetime.now(UTC) + timedelta(
            seconds=args.seconds + 60
        ):
            raise ValueError("Credentials expired during bootstrap; regenerate manifest")
        cpu_start, start = process_time(), perf_counter()
        for index, item in enumerate(workload):
            due = start + index / args.rate
            await asyncio.sleep(max(0, due - perf_counter()))
            late = perf_counter() - due
            lags.append(late * 1000)
            if late > max(0.05, 1 / args.rate) or len(pending) >= args.inflight:
                drops += 1
                continue
            task = asyncio.create_task(request(item))
            pending.add(task)
            tasks.append(task)
            task.add_done_callback(pending.discard)
        await asyncio.gather(*tasks)
        elapsed, cpu = perf_counter() - start, process_time() - cpu_start
    reads = [value for key, values in latency.items() if key.startswith("read:") for value in values]
    holds = [value for key, values in latency.items() if key.startswith("hold:") for value in values]
    unexpected = sum(
        n
        for op, rows in statuses.items()
        for status, n in rows.items()
        if status not in ({"200", "304"} if op == "read" else {"201"})
    )
    result = {
        "utc": datetime.now(UTC).isoformat(),
        "manifest_id": manifest["id"],
        "run_id": run_id,
        "target_origin": args.origin,
        "generator_hostname": socket.gethostname(),
        "platform": platform.platform(),
        "topology_declared": args.topology,
        "offered_rps": args.rate,
        "seconds": args.seconds,
        "show_count": len(shows),
        "seats_per_show": 300,
        "viewers": len(tokens),
        "write_percent": 5,
        "generator_drops": drops,
        "transport_error_types": dict(transport_errors),
        "scheduling_lag_p95_ms": percentile(lags, 0.95),
        "generator_cpu_seconds": cpu,
        "elapsed_seconds": elapsed,
        "completed_rps": sum(sum(v.values()) for v in statuses.values()) / elapsed,
        "statuses": dict(statuses),
        "response_body_bytes": bytes_received,
        "latency_ms": {
            key: {"p50": percentile(v, 0.5), "p95": percentile(v, 0.95), "p99": percentile(v, 0.99)}
            for key, v in latency.items()
        },
        "read_p95_ms": percentile(reads, 0.95),
        "hold_p95_ms": percentile(holds, 0.95),
        "local_read_gate_pass": drops == 0
        and unexpected == 0
        and bool(reads)
        and percentile(reads, 0.95) <= 150,
        "note": "95% reads/5% holds; bootstrap excluded. Topology is operator-declared, not verified. No backend durability/queue checks here.",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    if not result["local_read_gate_pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rate", type=int, default=50)
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--inflight", type=int, default=64)
    parser.add_argument(
        "--topology", choices=["same-host", "separate-host", "unverified"], default="unverified"
    )
    args = parser.parse_args()
    if min(args.rate, args.seconds, args.inflight) < 1 or args.rate * args.seconds > 100000:
        parser.error("Use positive limits and at most 100000 requests")
    asyncio.run(run(args))
