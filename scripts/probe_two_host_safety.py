"""One isolated100-way two-host hold and payment replay safety probe; not load sizing."""

import argparse
import asyncio
import ipaddress
import json
import os
import re
from collections import Counter
from pathlib import Path
from time import monotonic
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import httpx
import jwt
import psycopg


def validate_origins(origins):
    if len(origins) != 2:
        raise ValueError("Two API origins required")
    addresses = []
    for origin in origins:
        url = urlsplit(origin)
        address = ipaddress.ip_address(url.hostname or "")
        if (
            url.scheme != "http"
            or not any(
                address in ipaddress.ip_network(n) for n in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
            )
            or url.port not in range(8101, 8105)
            or url.username
            or url.password
            or url.path not in {"", "/"}
            or url.query
            or url.fragment
        ):
            raise ValueError("Verified private published API origin required")
        addresses.append(address)
    if len(set(addresses)) != 2:
        raise ValueError("Distinct API hosts required")


def wave_view(responses):
    accepted = [r for r in responses if r["status"] in {201, 202}]
    statuses = dict(Counter(str(r["status"]) for r in responses))
    if (
        len(responses) != 100
        or len(accepted) != 1
        or any(r["status"] not in {201, 202, 409} for r in responses)
    ):
        raise ValueError("100-way wave did not yield exactly one accepted hold")
    if Counter(r["host_index"] for r in responses) != Counter({0: 50, 1: 50}):
        raise ValueError("Wave did not exercise both hosts")
    winner = accepted[0]
    for field in ("command_id", "hold_id", "order_id"):
        UUID(winner["body"][field])
    if any(r["body"].get("code") != "SEAT_UNAVAILABLE" for r in responses if r["status"] == 409):
        raise ValueError("Unexpected conflict response")
    return winner, statuses


def probe_actors(run_id):
    if not isinstance(run_id, str) or not re.fullmatch(r"adr0147-[0-9a-f]{32}", run_id):
        raise ValueError("Fresh probe actor namespace required")
    return [f"{run_id}-{i}" for i in range(100)]


def audit_one_owner(event_id, body, run_id):
    actors = probe_actors(run_id)
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=False) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        conn.execute("SET LOCAL statement_timeout = '3s'")
        records = conn.execute(
            """SELECT count(*),count(DISTINCT h.id),count(DISTINCT o.id),
            count(*) FILTER (WHERE h.id=%s::uuid AND h.event_id=%s::uuid
              AND o.hold_id=h.id AND o.actor=h.actor AND r.actor=h.actor)
            FROM idempotency_records r LEFT JOIN holds h ON h.id=(r.response->>'hold_id')::uuid
            LEFT JOIN orders o ON o.id=(r.response->>'order_id')::uuid
            WHERE r.operation='hold' AND r.key LIKE %s AND r.actor=ANY(%s::text[])""",
            (body["hold_id"], event_id, run_id + "-%", actors),
        ).fetchone()
        seat = conn.execute(
            "SELECT hold_id FROM event_seats WHERE event_id=%s AND seat_id='S0'", (event_id,)
        ).fetchone()
        active = conn.execute(
            """SELECT count(DISTINCT h.id),count(DISTINCT o.id)
            FROM order_items i JOIN orders o ON o.id=i.order_id JOIN holds h ON h.id=o.hold_id
            WHERE i.event_id=%s AND i.seat_id='S0' AND h.status='ACTIVE' AND h.expires_at>clock_timestamp()""",
            (event_id,),
        ).fetchone()
    if records != (1, 1, 1, 1) or active != (1, 1) or not seat or str(seat[0]) != body["hold_id"]:
        raise ValueError("Exactly one durable seat owner not proven")
    return True


async def probe(event_id, origins, tokens, run_id, *, client=None, evidence=None):
    validate_origins(origins)
    UUID(event_id)
    if len(tokens) != 100 or len(set(tokens)) != 100:
        raise ValueError("100 distinct actors required")

    def record(phase, **values):
        if evidence is not None:
            evidence(phase, values)

    owned = client is None
    if owned:
        client = httpx.AsyncClient(
            timeout=10,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=100),
            trust_env=False,
        )
    try:
        ready = asyncio.Event()

        async def reserve(index):
            await ready.wait()
            response = await client.post(
                origins[index % 2] + "/v1/holds",
                json={"event_id": event_id, "seat_ids": ["S0"]},
                headers={"Authorization": "Bearer " + tokens[index], "Idempotency-Key": f"{run_id}-{index}"},
            )
            return {
                "index": index,
                "host_index": index % 2,
                "status": response.status_code,
                "body": response.json(),
            }

        record("hold-wave")
        tasks = [asyncio.create_task(reserve(i)) for i in range(100)]
        ready.set()
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        responses = [r for r in outcomes if not isinstance(r, BaseException)]
        failures = [type(r).__name__ for r in outcomes if isinstance(r, BaseException)]
        record("hold-wave-validation", wave_responses=responses, transport_errors=failures)
        if failures:
            raise ValueError("Hold wave transport failure; no retry")
        winner, statuses = wave_view(responses)
        headers = {
            "Authorization": "Bearer " + tokens[winner["index"]],
            "Idempotency-Key": f"{run_id}-{winner['index']}",
        }
        other = 1 - winner["host_index"]
        record("cross-host-hold-replay")
        replay = await client.post(
            origins[other] + "/v1/holds", json={"event_id": event_id, "seat_ids": ["S0"]}, headers=headers
        )
        if replay.status_code not in {200, 201, 202} or any(
            replay.json().get(k) != winner["body"][k] for k in ("command_id", "hold_id", "order_id")
        ):
            raise ValueError("Cross-host hold replay changed identity")
        body = winner["body"]
        record("durable-owner")
        deadline = monotonic() + 30
        while monotonic() < deadline:
            state = await client.get(
                origins[other] + f"/v1/reservation-commands/{event_id}/{body['command_id']}", headers=headers
            )
            if state.status_code != 200:
                raise ValueError("Unexpected command response")
            if state.json()["persistence_status"] == "DURABLE":
                break
            if state.json()["persistence_status"] != "PENDING":
                raise ValueError("Reservation persistence failed")
            await asyncio.sleep(0.2)
        else:
            raise TimeoutError("Cross-host reservation did not become durable")
        audit_one_owner(event_id, body, run_id)
        payment_headers = {**headers, "Idempotency-Key": run_id + "-payment"}
        record("payment-replay")
        payments = []
        for origin in origins:
            response = await client.post(
                origin + f"/v1/orders/{body['order_id']}/payments",
                json={"outcome": "SUCCEEDED", "delay_seconds": 0, "duplicates": 3},
                headers=payment_headers,
            )
            if response.status_code != 202:
                raise ValueError("Payment replay HTTP failed")
            payments.append(response.json())
        if payments[0] != payments[1] or not payments[0].get("payment_id"):
            raise ValueError("Cross-host payment replay created another payment")
        record("customer-authorization")
        unauthorized = await client.get(
            origins[other] + f"/v1/orders/{body['order_id']}",
            headers={"Authorization": "Bearer " + tokens[(winner["index"] + 1) % 100]},
        )
        if unauthorized.status_code not in {403, 404}:
            raise ValueError("Other actor accessed order")
        record("ticket-confirmation")
        deadline = monotonic() + 45
        while monotonic() < deadline:
            order = await client.get(origins[other] + f"/v1/orders/{body['order_id']}", headers=headers)
            if order.status_code != 200:
                raise ValueError("Unexpected order response")
            if order.json()["status"] == "FULFILLED" and len(order.json().get("tickets", [])) == 1:
                break
            await asyncio.sleep(0.5)
        else:
            raise TimeoutError("Safety ticket did not become fulfilled")
        return {
            "run_id": run_id,
            "event_id": event_id,
            "wave_requests": 100,
            "requests_per_host": [50, 50],
            "wave_statuses": statuses,
            "accepted_holds": 1,
            "one_durable_owner_before_payment": True,
            "cross_host_hold_replay": True,
            "cross_host_payment_replay": True,
            "other_actor_denied": True,
            "customer_ticket_confirmed": True,
            "hold_id": body["hold_id"],
            "order_id": body["order_id"],
            "payment_id": payments[0]["payment_id"],
            "scope": "Safety probe only; database ownership, duplicate callbacks, post-TTL and global drain require separate audits.",
        }
    finally:
        if owned:
            await client.aclose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-id", required=True)
    parser.add_argument("--origin", action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if os.getenv("ENVIRONMENT") != "development" or args.output.exists():
        parser.error("Development and fresh owned output required")
    identifier = "adr0147-" + uuid4().hex
    import time

    tokens = [
        jwt.encode(
            {
                "sub": actor,
                "aud": "ticketing",
                "iss": "ticketing",
                "exp": int(time.time()) + 600,
            },
            os.environ["JWT_SECRET"],
            algorithm="HS256",
        )
        for actor in probe_actors(identifier)
    ]
    with args.output.open("x") as out:
        os.chmod(args.output, 0o600)
        state = {"run_id": identifier, "event_id": args.event_id, "pass": False, "phase": "initializing"}

        def save(phase, values):
            state.update(values)
            state["phase"] = phase
            out.seek(0)
            json.dump(state, out)
            out.truncate()
            out.flush()

        save("initializing", {})
        try:
            result = asyncio.run(probe(args.event_id, args.origin, tokens, identifier, evidence=save))
        except BaseException as exc:
            save(state["phase"], {"failure_type": type(exc).__name__})
            raise
        save("complete", {**result, "pass": True})
    print(json.dumps({k: v for k, v in result.items() if not k.endswith("_id")}))


if __name__ == "__main__":
    main()
