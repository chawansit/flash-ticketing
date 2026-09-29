"""Development-only Redis-first hold-to-ticket smoke probe using a private load manifest.

This validates the HTTP contract for a bounded number of journeys. It is not a
capacity benchmark or a database/queue durability audit.
"""

import argparse
import asyncio
import json
import math
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import httpx


def percentile(values, fraction):
    return sorted(values)[math.ceil(len(values) * fraction) - 1] if values else None


async def journey(client, manifest, index, run_id, timeout_seconds, poll_seconds, duplicates):
    started = perf_counter()
    show_ids = manifest["show_ids"]
    show = show_ids[index % len(show_ids)]
    seat = manifest["seat_offset"] + index // len(show_ids)
    actor = manifest["viewer_tokens"][index % len(manifest["viewer_tokens"])]
    headers = {"Authorization": "Bearer " + actor}
    deadline = started + timeout_seconds
    try:
        hold = await client.post(
            "/v1/holds",
            json={"event_id": show, "seat_ids": [f"S{seat}"]},
            headers={**headers, "Idempotency-Key": f"{run_id}-{index}-hold"},
        )
        hold_http_ms = (perf_counter() - started) * 1000
        if hold.status_code != 202:
            return {"outcome": f"hold_http_{hold.status_code}"}
        response = hold.json()
        command_id, order_id = response["command_id"], response["order_id"]
        command_url = f"/v1/reservation-commands/{show}/{command_id}"
        while perf_counter() < deadline:
            command = await client.get(command_url, headers=headers)
            if command.status_code != 200:
                return {"outcome": f"command_http_{command.status_code}"}
            state = command.json()["persistence_status"]
            if state == "DURABLE":
                durable_ms = (perf_counter() - started) * 1000
                command_durable_wait_ms = durable_ms - hold_http_ms
                break
            if state == "FAILED":
                return {"outcome": "command_failed"}
            if state != "PENDING":
                return {"outcome": "command_unknown_state"}
            await asyncio.sleep(poll_seconds)
        else:
            return {"outcome": "durability_timeout"}

        payment_started = perf_counter()
        payment = await client.post(
            f"/v1/orders/{order_id}/payments",
            json={"outcome": "SUCCEEDED", "delay_seconds": 0, "duplicates": duplicates},
            headers={**headers, "Idempotency-Key": f"{run_id}-{index}-payment"},
        )
        payment_http_ms = (perf_counter() - payment_started) * 1000
        if payment.status_code != 202:
            return {"outcome": f"payment_http_{payment.status_code}", "durable_ms": durable_ms}
        while perf_counter() < deadline:
            order = await client.get(f"/v1/orders/{order_id}", headers=headers)
            if order.status_code != 200:
                return {"outcome": f"order_http_{order.status_code}", "durable_ms": durable_ms}
            body = order.json()
            if body["status"] == "FULFILLED":
                ticket_wait_ms = (perf_counter() - payment_started) * 1000 - payment_http_ms
                tickets = body.get("tickets", [])
                if len(tickets) != 1:
                    return {"outcome": "ticket_count_mismatch", "durable_ms": durable_ms}
                return {
                    "outcome": "fulfilled",
                    "durable_ms": durable_ms,
                    "hold_http_ms": hold_http_ms,
                    "command_durable_wait_ms": command_durable_wait_ms,
                    "payment_http_ms": payment_http_ms,
                    "ticket_wait_ms": ticket_wait_ms,
                    "payment_to_ticket_ms": (perf_counter() - payment_started) * 1000,
                    "hold_to_ticket_ms": (perf_counter() - started) * 1000,
                    "order_id": order_id,
                    "ticket_id": str(tickets[0]["id"]),
                }
            if body["status"] not in {"PENDING", "PAID"}:
                return {"outcome": "order_terminal_without_ticket", "durable_ms": durable_ms}
            await asyncio.sleep(poll_seconds)
        return {"outcome": "ticket_timeout", "durable_ms": durable_ms}
    except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
        return {"outcome": f"probe_{type(exc).__name__}"}


async def run(args):
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1 or manifest.get("environment") != "development":
        raise ValueError("Expected a development-only private manifest")
    if args.origin.rstrip("/") != manifest.get("origin"):
        raise ValueError("Origin does not match the private manifest")
    worst_case_seconds = math.ceil(args.journeys / args.concurrency) * args.timeout_seconds
    if datetime.fromisoformat(manifest["expires_at"]) <= datetime.now(UTC) + timedelta(
        seconds=worst_case_seconds + 60
    ):
        raise ValueError("Manifest credentials expire before probe completion")
    shows, tokens = manifest["show_ids"], manifest["viewer_tokens"]
    if not shows or not tokens:
        raise ValueError("Manifest needs shows and viewer credentials")
    if (manifest["seat_offset"] < 0 or
            manifest["seat_offset"] + (args.journeys - 1) // len(shows) >= manifest["seats_per_show"]):
        raise ValueError("Not enough distinct fixture seats for the probe")
    if args.output.resolve() == args.manifest.resolve() or args.output.exists():
        raise ValueError("Use a fresh output path separate from the private manifest")

    semaphore = asyncio.Semaphore(args.concurrency)
    run_id = uuid4().hex

    async with httpx.AsyncClient(
        base_url=args.origin,
        timeout=10,
        limits=httpx.Limits(max_connections=args.concurrency),
    ) as client:
        (await client.get("/health/ready")).raise_for_status()

        async def bounded(index):
            async with semaphore:
                return await journey(
                    client, manifest, index, run_id, args.timeout_seconds,
                    args.poll_seconds, args.duplicates,
                )

        rows = await asyncio.gather(*(bounded(index) for index in range(args.journeys)))

    outcomes = Counter(row["outcome"] for row in rows)
    fulfilled = [row for row in rows if row["outcome"] == "fulfilled"]
    distinct_orders = len({row["order_id"] for row in fulfilled})
    distinct_tickets = len({row["ticket_id"] for row in fulfilled})
    result = {
        "measured_at_utc": datetime.now(UTC).isoformat(),
        "kind": "redis_first_checkout_smoke",
        "journeys": args.journeys,
        "concurrency": args.concurrency,
        "duplicates_per_payment": args.duplicates,
        "outcomes": dict(outcomes),
        "distinct_fulfilled_orders": distinct_orders,
        "distinct_tickets": distinct_tickets,
        "durable_p95_ms": percentile([row["durable_ms"] for row in fulfilled], 0.95),
        "payment_to_ticket_p95_ms": percentile(
            [row["payment_to_ticket_ms"] for row in fulfilled], 0.95
        ),
        "hold_to_ticket_p95_ms": percentile(
            [row["hold_to_ticket_ms"] for row in fulfilled], 0.95
        ),
        "pass": len(fulfilled) == distinct_orders == distinct_tickets == args.journeys,
        "note": "HTTP smoke only; no DB/Kafka audit or arrival-rate claim.",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result))
    if not result["pass"]:
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--journeys", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument("--poll-seconds", type=float, default=0.2)
    parser.add_argument("--duplicates", type=int, default=3)
    args = parser.parse_args()
    if (not 1 <= args.journeys <= 1000 or not 1 <= args.concurrency <= 100
            or not 1 <= args.timeout_seconds <= 110 or not 0.05 <= args.poll_seconds <= 2
            or not 1 <= args.duplicates <= 10):
        parser.error("Use bounded positive smoke-probe limits")
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
