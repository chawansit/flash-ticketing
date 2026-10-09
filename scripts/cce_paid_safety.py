"""ADR0228 four-pod safety protocol; not a capacity or durability qualification."""

import asyncio
from collections import Counter
from time import monotonic
from uuid import UUID

import httpx
from cce_api_adapter import endpoints
from probe_two_host_safety import audit_one_owner, probe_actors


def origins_for(receipts):
    return ["http://" + address + ":8000" for address in endpoints(receipts)]


def wave_view(responses):
    if (
        len(responses) != 100
        or {r["index"] for r in responses} != set(range(100))
        or Counter(r["pod_index"] for r in responses) != Counter({i: 25 for i in range(4)})
    ):
        raise ValueError("100 requests must exercise all four pods equally")
    accepted = [r for r in responses if r["status"] in {201, 202}]
    if len(accepted) != 1 or any(r["status"] not in {201, 202, 409} for r in responses):
        raise ValueError("Exactly one atomic hold must succeed")
    if any(r["body"].get("code") != "SEAT_UNAVAILABLE" for r in responses if r["status"] == 409):
        raise ValueError("Unexpected hold conflict; never retry the wave")
    winner = accepted[0]
    for key in ("command_id", "hold_id", "order_id"):
        UUID(winner["body"][key])
    return winner, dict(Counter(str(r["status"]) for r in responses))


async def probe(
    event_id, receipts, tokens, run_id, *, client=None, evidence=None, owner_audit=audit_one_owner
):
    origins = origins_for(receipts)
    UUID(event_id)
    probe_actors(run_id)
    if len(tokens) != 100 or len(set(tokens)) != 100:
        raise ValueError("100 distinct authorized actors required")

    def record(phase, **values):
        if evidence is not None:
            evidence(phase, values)

    owned = client is None
    if owned:
        client = httpx.AsyncClient(
            timeout=10,
            trust_env=False,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=100),
        )
    try:
        ready = asyncio.Event()

        async def hold(index):
            await ready.wait()
            response = await client.post(
                origins[index % 4] + "/v1/holds",
                json={"event_id": event_id, "seat_ids": ["S0"]},
                headers={"Authorization": "Bearer " + tokens[index], "Idempotency-Key": f"{run_id}-{index}"},
            )
            return {
                "index": index,
                "pod_index": index % 4,
                "status": response.status_code,
                "body": response.json(),
            }

        record("hold-wave")
        tasks = [asyncio.create_task(hold(index)) for index in range(100)]
        ready.set()
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)
        responses = [r for r in outcomes if not isinstance(r, BaseException)]
        failures = [type(r).__name__ for r in outcomes if isinstance(r, BaseException)]
        record("hold-wave-validation", responses=responses, transport_errors=failures)
        if failures:
            raise ValueError("Hold wave transport failure; no retry")
        winner, statuses = wave_view(responses)
        body = winner["body"]
        headers = {
            "Authorization": "Bearer " + tokens[winner["index"]],
            "Idempotency-Key": f"{run_id}-{winner['index']}",
        }
        record("all-pod-hold-replay")
        for origin in origins:
            response = await client.post(
                origin + "/v1/holds", json={"event_id": event_id, "seat_ids": ["S0"]}, headers=headers
            )
            if response.status_code not in {200, 201, 202} or any(
                response.json().get(k) != body[k] for k in ("command_id", "hold_id", "order_id")
            ):
                raise ValueError("Cross-pod hold replay changed identity")
        record("durable-owner")
        deadline = monotonic() + 30
        index = 0
        while monotonic() < deadline:
            state = await client.get(
                origins[index % 4] + f"/v1/reservation-commands/{event_id}/{body['command_id']}",
                headers=headers,
            )
            index += 1
            if state.status_code != 200:
                raise ValueError("Unexpected reservation response")
            if state.json().get("persistence_status") == "DURABLE":
                break
            if state.json().get("persistence_status") != "PENDING":
                raise ValueError("Reservation persistence failed")
            await asyncio.sleep(0.2)
        else:
            raise TimeoutError("Safety hold did not become durable")
        owner_audit(event_id, body, run_id)
        record("all-pod-payment-replay")
        payment_headers = {**headers, "Idempotency-Key": run_id + "-payment"}
        payments = []
        for origin in origins:
            response = await client.post(
                origin + f"/v1/orders/{body['order_id']}/payments",
                json={"outcome": "SUCCEEDED", "delay_seconds": 0, "duplicates": 3},
                headers=payment_headers,
            )
            if response.status_code != 202:
                raise ValueError("Payment replay failed")
            payments.append(response.json())
        if not payments[0].get("payment_id") or any(p != payments[0] for p in payments):
            raise ValueError("Cross-pod payment replay created another payment")
        record("all-pod-customer-authorization")
        for origin in origins:
            response = await client.get(
                origin + f"/v1/orders/{body['order_id']}",
                headers={"Authorization": "Bearer " + tokens[(winner["index"] + 1) % 100]},
            )
            if response.status_code not in {403, 404}:
                raise ValueError("Other actor accessed order")
        record("customer-ticket-confirmation")
        deadline = monotonic() + 45
        index = 0
        while monotonic() < deadline:
            response = await client.get(
                origins[index % 4] + f"/v1/orders/{body['order_id']}", headers=headers
            )
            index += 1
            if response.status_code != 200:
                raise ValueError("Unexpected order response")
            if response.json().get("status") == "FULFILLED" and len(response.json().get("tickets", [])) == 1:
                break
            await asyncio.sleep(0.5)
        else:
            raise TimeoutError("Safety ticket was not confirmed")
        return {
            "run_id": run_id,
            "event_id": event_id,
            "requests_per_pod": [25] * 4,
            "wave_requests": 100,
            "accepted_holds": 1,
            "wave_statuses": statuses,
            "one_durable_owner_before_payment": True,
            "all_pod_hold_replay": True,
            "all_pod_payment_replay": True,
            "other_actor_denied_on_all_pods": True,
            "customer_ticket_confirmed": True,
            "hold_id": body["hold_id"],
            "order_id": body["order_id"],
            "payment_id": payments[0]["payment_id"],
            "scope": "Safety only; separate post-TTL, duplicate callback, financial and global queue audits remain mandatory.",
        }
    finally:
        if owned:
            await client.aclose()
