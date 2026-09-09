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


ERROR_CODES = frozenset({
    "ADMISSION_FULL", "ADMISSION_UNAVAILABLE", "DATABASE_UNAVAILABLE", "RESOURCE_BUSY",
    "SEAT_BUSY", "SEAT_UNAVAILABLE", "SEATMAP_WARMING", "SEATMAP_UNAVAILABLE",
    "RATE_LIMITED", "UNAUTHENTICATED", "IDEMPOTENCY_MISMATCH", "SALE_CLOSED",
    "EVENT_NOT_FOUND", "SEAT_NOT_FOUND", "INVALID_REQUEST", "INVALID_SEATS",
})


def error_diagnostic(response):
    try:
        body = response.json()
    except ValueError:
        body = None
    code = body.get("code") if isinstance(body, dict) else None
    code = code if isinstance(code, str) and code in ERROR_CODES else "OTHER"
    request_id = response.headers.get("X-Request-ID")
    try:
        from uuid import UUID
        request_id = str(UUID(request_id)) if request_id else None
    except (ValueError, TypeError, AttributeError):
        request_id = None
    return code, request_id


def arrival_plan(rate, seconds, burst=False):
    phases = [(rate, seconds)] if not burst else [(rate, 60), (rate * 4, 120), (rate, 60)]
    elapsed = 0
    for phase, (phase_rate, duration) in enumerate(phases):
        for index in range(phase_rate * duration):
            yield elapsed + index / phase_rate, phase
        elapsed += duration



class TransportTrace:
    """Bounded phase-only diagnostics; never serialize exception text or trace payloads."""
    def __init__(self):
        self.started = perf_counter()
        self.events = []
        self.connect_attempted = False

    async def __call__(self, name, info):
        if name == 'connection.connect_tcp.started':
            self.connect_attempted = True
        if len(self.events) < 32:
            self.events.append({'phase': name, 'elapsed_ms': round((perf_counter()-self.started)*1000, 3)})

    def failure(self, exc):
        chain, seen = [], set()
        while exc is not None and id(exc) not in seen and len(chain) < 5:
            seen.add(id(exc))
            item = {'type': type(exc).__name__}
            number = getattr(exc, 'errno', None)
            if isinstance(number, int):
                item['errno'] = number
            chain.append(item)
            exc = exc.__cause__ or exc.__context__
        return {'connect_attempted': self.connect_attempted, 'events': list(self.events), 'exception_chain': chain}

def expiry_seconds(value):
    number = float(value)
    if not math.isfinite(number) or not 0 < number <= 60:
        raise argparse.ArgumentTypeError("Keep-alive expiry must be finite and in (0, 60] seconds")
    return number


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
    phase_statuses, phase_latency = defaultdict(lambda: defaultdict(Counter)), defaultdict(lambda: defaultdict(list))
    statuses, latency = defaultdict(Counter), defaultdict(list)
    counts, bytes_received, drops = Counter(), 0, 0
    connection_counts = defaultdict(Counter)
    expected_hot_conflicts = 0
    transport_errors = Counter()
    transport_examples = []
    drop_reasons = Counter()
    validators, pending, lags = {}, set(), []
    task_errors = Counter()
    error_codes, error_examples = Counter(), []
    run_id = str(uuid4())
    async with httpx.AsyncClient(
        base_url=args.origin,
        timeout=10,
        limits=httpx.Limits(max_connections=args.inflight, max_keepalive_connections=args.inflight,
                           keepalive_expiry=getattr(args, "keepalive_expiry", 5.0)),
    ) as client:
        (await client.get("/health/ready")).raise_for_status()
        initial = {}
        for show in shows:
            response = await client.get(f"/v1/events/{show}/availability")
            response.raise_for_status()
            initial[show] = response.headers["etag"]
        hot_show = manifest.get("hot_show_id")
        if hot_show and hot_show not in initial:
            response = await client.get(f"/v1/events/{hot_show}/availability")
            response.raise_for_status()
            initial[hot_show] = response.headers["etag"]
        validators = {}
        randomizer = random.Random(42)
        workload = []
        for index, (due, phase) in enumerate(arrival_plan(args.rate, args.seconds, args.burst)):
            viewer = randomizer.randrange(len(tokens))
            show = shows[viewer % len(shows)]
            write = index % 20 == 0
            hot = bool(getattr(args, "mixed_hot_holds", False) and index % 100 == 0)
            seat = manifest["seat_offset"] + counts[show]
            if hot:
                show = manifest["hot_hold_show_id"]
                seat = manifest.get("hot_hold_seat", 299)
                if not 0 <= seat < manifest["seats_per_show"]:
                    raise ValueError("Invalid hot seat")
            if write and not hot:
                if (getattr(args, "mixed_hot_holds", False)
                        and show == manifest["hot_hold_show_id"]
                        and seat == manifest.get("hot_hold_seat", 299)):
                    raise ValueError("Ordinary allocation overlaps the shared hot seat")
                counts[show] += 1
                if seat >= manifest["seats_per_show"]:
                    raise ValueError("Allocated fixture seats exhausted")
            if not write and hot_show and randomizer.random() < 0.9:
                show = hot_show
            workload.append((index, viewer, show, write, seat, due, phase, hot))

        async def request(item):
            nonlocal bytes_received, expected_hot_conflicts
            index, viewer, show, write, seat, _, phase, hot = item
            operation = "hot_hold" if hot else "hold" if write else "read"
            begin = perf_counter()
            trace = TransportTrace() if getattr(args, "transport_diagnostics", False) else None
            started_utc = datetime.now(UTC).isoformat() if trace else None
            extensions = {"trace": trace} if trace else {}
            try:
                if write:
                    response = await client.post(
                        "/v1/holds",
                        extensions=extensions,
                        json={"event_id": show, "seat_ids": [f"S{seat}"]},
                        headers={
                            "Authorization": "Bearer " + tokens[viewer],
                            "Idempotency-Key": f"{run_id}-{index}",
                        },
                    )
                else:
                    response = await client.get(
                        f"/v1/events/{show}/availability", extensions=extensions, headers={"If-None-Match": validators.get((viewer, show), initial[show])}
                    )
                    if response.status_code == 200:
                        validators[(viewer, show)] = response.headers["etag"]
                status = str(response.status_code)
                bytes_received += len(response.content)
                if response.status_code not in ({201} if write else {200, 304}):
                    code, request_id = error_diagnostic(response)
                    if hot and status == "409" and code in {"SEAT_BUSY", "SEAT_UNAVAILABLE"}:
                        expected_hot_conflicts += 1
                    error_codes[f"{operation}:{status}:{code}"] += 1
                    if len(error_examples) < 20:
                        error_examples.append({"operation": operation, "status": status,
                                               "code": code, "request_id": request_id})
            except httpx.HTTPError as exc:
                status = "transport_error"
                transport_errors[type(exc).__name__] += 1
                if trace and len(transport_examples) < 20:
                    transport_examples.append({"index": index, "operation": operation, "started_utc": started_utc, "elapsed_ms": round((perf_counter()-begin)*1000, 3), **trace.failure(exc)})
            if trace:
                connection_counts[operation].update(
                    event["phase"].rsplit(".", 1)[-1] for event in trace.events
                    if event["phase"].startswith("connection.connect_tcp.")
                )
            statuses[operation][status] += 1
            duration = (perf_counter() - begin) * 1000
            latency[operation + ":" + status].append(duration)
            phase_statuses[str(phase)][operation][status] += 1
            phase_latency[str(phase)][operation].append(duration)

        if datetime.fromisoformat(manifest["expires_at"]) <= datetime.now(UTC) + timedelta(
            seconds=args.seconds + 60
        ):
            raise ValueError("Credentials expired during bootstrap; regenerate manifest")

        def completed(task):
            pending.discard(task)
            if error := task.exception():
                task_errors[type(error).__name__] += 1

        if args.start_at:
            delay = (datetime.fromisoformat(args.start_at) - datetime.now(UTC)).total_seconds()
            if delay < 0:
                raise ValueError("Missed coordinated start during bootstrap")
            await asyncio.sleep(delay)
        measured_started_utc = datetime.now(UTC).isoformat()
        print(
            json.dumps(
                {
                    "phase": "measuring",
                    "rate": args.rate,
                    "seconds": args.seconds,
                    "utc": measured_started_utc,
                }
            ),
            flush=True,
        )
        cpu_start, start = process_time(), perf_counter()
        for index, item in enumerate(workload):
            due = start + item[5]
            await asyncio.sleep(max(0, due - perf_counter()))
            late = perf_counter() - due
            lags.append(late * 1000)
            if late > max(0.05, 1 / args.rate) or len(pending) >= args.inflight:
                drops += 1
                drop_reasons["late" if late > max(0.05, 1 / args.rate) else "inflight_limit"] += 1
                continue
            task = asyncio.create_task(request(item))
            pending.add(task)
            task.add_done_callback(completed)
            if index and index % (args.rate * 30) == 0:
                print(json.dumps({"phase": "progress", "scheduled": index, "drops": drops}), flush=True)
        await asyncio.gather(*pending, return_exceptions=True)
        elapsed, cpu = perf_counter() - start, process_time() - cpu_start
    reads = [value for key, values in latency.items() if key.startswith("read:") for value in values]
    holds = [value for key, values in latency.items() if key.startswith("hold:") for value in values]
    unexpected = sum(
        n
        for op, rows in statuses.items()
        for status, n in rows.items()
        if status not in ({"200", "304"} if op == "read" else {"201", "409"} if op == "hot_hold" else {"201"})
    )
    unexpected += statuses["hot_hold"].get("409", 0) - expected_hot_conflicts
    hot_failed = [v for k, values in latency.items() if k.startswith("hot_hold:") and k != "hot_hold:201" for v in values]
    result = {
        "mixed_hot_holds": getattr(args, "mixed_hot_holds", False),
        "expected_hot_conflicts": expected_hot_conflicts,
        "hot_failed_p95_ms": percentile(hot_failed, .95),
        "phases": {phase: {"statuses": dict(rows), "p95_ms": {
            op: percentile(values, 0.95) for op, values in phase_latency[phase].items()
        }} for phase, rows in phase_statuses.items()},
        "read_hot_share": 0.9 if hot_show else 0,
        "burst": args.burst,
        "utc": datetime.now(UTC).isoformat(),
        "measured_started_utc": measured_started_utc,
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
        "drop_reasons": dict(drop_reasons),
        "error_codes": dict(error_codes),
        "error_examples": error_examples,
        "keepalive_expiry_seconds": getattr(args, "keepalive_expiry", 5.0),
        "measured_tcp_connect_events": dict(connection_counts) if getattr(args, "transport_diagnostics", False) else None,
        "transport_error_types": dict(transport_errors),
        "transport_diagnostics_enabled": getattr(args, "transport_diagnostics", False),
        "transport_failure_examples": transport_examples,
        "task_error_types": dict(task_errors),
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
        and not task_errors
        and bool(reads)
        and percentile(reads, 0.95) <= 150,
        "note": "95% reads/5% holds (mixed mode: 4% distinct, 1% hot). Known hot 409 codes remain in error_codes but are expected; bootstrap excluded. Topology is operator-declared, not verified. No backend durability/queue checks here.",
    }
    result["reservation_gate_pass"] = bool(holds) and percentile(holds, 0.95) <= 300
    result["accounting_pass"] = (
        sum(sum(v.values()) for v in statuses.values()) + sum(task_errors.values()) + drops
        == len(workload)
    )
    result["workload_gate_pass"] = (
        result["local_read_gate_pass"] and result["reservation_gate_pass"] and result["accounting_pass"]
    )
    result["phase_latency_gate_pass"] = all(
        percentile(values, 0.95) <= (150 if op == "read" else 300)
        for rows in phase_latency.values() for op, values in rows.items()
    )
    result["hot_contention_gate_pass"] = not getattr(args, "mixed_hot_holds", False) or (bool(hot_failed) and percentile(hot_failed, .95) <= 200)
    result["workload_gate_pass"] &= result["phase_latency_gate_pass"] and result["hot_contention_gate_pass"]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    if not result["workload_gate_pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rate", type=int, default=50)
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--inflight", type=int, default=64)
    parser.add_argument("--burst", action="store_true", help="Continuous 60s base/120s 4x/60s recovery")
    parser.add_argument("--transport-diagnostics", action="store_true")
    parser.add_argument("--mixed-hot-holds", action="store_true")
    parser.add_argument("--keepalive-expiry", type=expiry_seconds, default=5.0)
    parser.add_argument("--start-at", help="Optional coordinated UTC ISO start time")
    parser.add_argument(
        "--topology", choices=["same-host", "separate-host", "unverified"], default="unverified"
    )
    args = parser.parse_args()
    if args.burst:
        args.seconds = 240
    if min(args.rate, args.seconds, args.inflight) < 1 or args.rate * (600 if args.burst else args.seconds) > 2000000:
        parser.error("Use positive limits and at most 2000000 requests")
    asyncio.run(run(args))
