"""Open-loop, bounded paid-ticket journey generator for development fixtures."""

import argparse
import asyncio
import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import httpx
from checkout_journey_probe import journey, percentile


def endpoint(path):
    if path == "/v1/holds":
        return "holds"
    if path.startswith("/v1/reservation-commands/"):
        return "reservation_commands"
    if path.endswith("/payments"):
        return "payments"
    if path.startswith("/v1/orders/"):
        return "orders"
    return "other"


def validate(args, manifest):
    if manifest.get("schema_version") != 1 or manifest.get("environment") != "development":
        raise ValueError("A development-only private manifest is required")
    if args.origin.rstrip("/") != manifest.get("origin"):
        raise ValueError("Origin does not match private manifest")
    if not 1 <= args.rate <= 500 or not 1 <= args.seconds <= 3600:
        raise ValueError("Rate or duration outside bounded limits")
    scheduled = args.rate * args.seconds
    if scheduled > 300000:
        raise ValueError("Maximum 300000 scheduled journeys")
    if not 1 <= args.concurrency <= 1000 or not 1 <= args.duplicates <= 10:
        raise ValueError("Concurrency or callback count outside bounded limits")
    if not 1 <= args.timeout_seconds <= 110 or not 0.05 <= args.poll_seconds <= 2:
        raise ValueError("Timeout or poll interval outside bounded limits")
    if args.completion_deadline_seconds < args.seconds:
        raise ValueError("Completion deadline precedes end of dispatch")
    if args.output.exists():
        raise ValueError("Use a fresh output path")
    shows = manifest.get("show_ids")
    tokens = manifest.get("viewer_tokens")
    if not shows or not tokens:
        raise ValueError("Manifest requires shows and viewer credentials")
    if manifest["seat_offset"] < 0 or (
        manifest["seat_offset"] + (scheduled - 1) // len(shows) >= manifest["seats_per_show"]
    ):
        raise ValueError("Fixture has insufficient distinct seats")
    if datetime.fromisoformat(manifest["expires_at"]) <= datetime.now(UTC) + timedelta(
        seconds=args.completion_deadline_seconds + 60
    ):
        raise ValueError("Manifest expires before the measurement deadline")
    return scheduled


async def scheduled_journeys(args, manifest, journey_fn=journey):
    scheduled = validate(args, manifest)
    outcomes = Counter()
    attempts = Counter()
    latencies = {key: [] for key in (
        "hold_http_ms", "command_durable_wait_ms", "durable_ms",
        "payment_http_ms", "ticket_wait_ms", "payment_to_ticket_ms", "hold_to_ticket_ms",
    )}
    unique_orders = set()
    unique_tickets = set()
    active = set()
    dispatch_lags = []
    dropped = dispatched = fulfilled_by_deadline = 0
    run_id = uuid4().hex

    async def on_request(request):
        attempts[endpoint(request.url.path)] += 1

    async with httpx.AsyncClient(
        base_url=args.origin,
        timeout=10,
        limits=httpx.Limits(max_connections=args.concurrency),
        event_hooks={"request": [on_request]},
    ) as client:
        (await client.get("/health/ready")).raise_for_status()
        attempts.clear()
        started = perf_counter()
        completion_deadline = started + args.completion_deadline_seconds

        async def one(index):
            nonlocal fulfilled_by_deadline
            try:
                row = await journey_fn(
                    client,
                    manifest,
                    index,
                    run_id,
                    args.timeout_seconds,
                    args.poll_seconds,
                    args.duplicates,
                )
            except Exception:  # noqa: BLE001 - account for every dispatched journey
                row = {"outcome": "generator_exception"}
            outcomes[row["outcome"]] += 1
            if row["outcome"] == "fulfilled":
                unique_orders.add(row["order_id"])
                unique_tickets.add(row["ticket_id"])
                if perf_counter() <= completion_deadline:
                    fulfilled_by_deadline += 1
                for key, values in latencies.items():
                    values.append(row[key])

        for index in range(scheduled):
            due = started + index / args.rate
            await asyncio.sleep(max(0.0, due - perf_counter()))
            dispatch_lags.append(max(0.0, (perf_counter() - due) * 1000))
            if len(active) >= args.concurrency:
                dropped += 1
                continue
            task = asyncio.create_task(one(index))
            active.add(task)
            task.add_done_callback(active.discard)
            dispatched += 1
        if active:
            await asyncio.gather(*active)
        finished = perf_counter()

    fulfilled = outcomes["fulfilled"]
    result = {
        "measured_at_utc": datetime.now(UTC).isoformat(),
        "kind": "scheduled_paid_ticket_journeys",
        "rate_target_per_second": args.rate,
        "dispatch_seconds": args.seconds,
        "completion_deadline_seconds": args.completion_deadline_seconds,
        "scheduled": scheduled,
        "dispatched": dispatched,
        "generator_drops": dropped,
        "completed": sum(outcomes.values()),
        "fulfilled": fulfilled,
        "fulfilled_by_deadline": fulfilled_by_deadline,
        "distinct_orders": len(unique_orders),
        "distinct_tickets": len(unique_tickets),
        "outcomes": dict(outcomes),
        "physical_http_attempts": dict(attempts),
        "retry_attempts": 0,
        "dispatch_lag_p95_ms": percentile(dispatch_lags, 0.95),
        "dispatch_lag_max_ms": max(dispatch_lags, default=0),
        "hold_http_p95_ms": percentile(latencies["hold_http_ms"], 0.95),
        "command_durable_wait_p95_ms": percentile(latencies["command_durable_wait_ms"], 0.95),
        "durable_p95_ms": percentile(latencies["durable_ms"], 0.95),
        "payment_http_p95_ms": percentile(latencies["payment_http_ms"], 0.95),
        "ticket_wait_p95_ms": percentile(latencies["ticket_wait_ms"], 0.95),
        "payment_to_ticket_p95_ms": percentile(latencies["payment_to_ticket_ms"], 0.95),
        "hold_to_ticket_p95_ms": percentile(latencies["hold_to_ticket_ms"], 0.95),
        "elapsed_seconds": finished - started,
    }
    result["pass"] = (
        scheduled
        == dispatched
        == result["completed"]
        == fulfilled
        == fulfilled_by_deadline
        == len(unique_orders)
        == len(unique_tickets)
        and dropped == 0
        and result["dispatch_lag_p95_ms"] <= 100
        and result["dispatch_lag_max_ms"] <= 500
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rate", required=True, type=int)
    parser.add_argument("--seconds", required=True, type=int)
    parser.add_argument("--completion-deadline-seconds", required=True, type=int)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--duplicates", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument("--poll-seconds", type=float, default=0.2)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    result = asyncio.run(scheduled_journeys(args, manifest))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result), flush=True)
    if not result["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
