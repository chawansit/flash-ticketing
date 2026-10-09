"""Offline four-pod protocol tests; no real booking or payment service."""

import asyncio
import copy
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_paid_safety as safety

BODY = {key: str(uuid4()) for key in ("command_id", "hold_id", "order_id")}
RUN = "adr0147-" + "e" * 32
EVENT = str(uuid4())
TOKENS = ["token-" + str(i) for i in range(100)]


def receipts():
    return [
        {"pod_name": f"api-{i}", "pod_uid": f"uid-{i}", "private_ipv4": f"10.2.240.{i + 1}"} for i in range(4)
    ]


class Client:
    def __init__(self, fault=None):
        self.fault = fault
        self.calls = []
        self.wave = []

    async def post(self, url, *, json, headers):
        self.calls.append(("post", url, json, headers))
        if url.endswith("/v1/holds"):
            index = int(headers["Idempotency-Key"].rsplit("-", 1)[1])
            if len(self.wave) < 100:
                self.wave.append(index)
                if self.fault == "transport" and index == 5:
                    raise TimeoutError("connection lost")
                winner = index == 7 or (self.fault == "double" and index == 8)
                return response(202 if winner else 409, BODY if winner else {"code": "SEAT_UNAVAILABLE"})
            body = {**BODY, "hold_id": str(uuid4())} if self.fault == "hold_replay" else BODY
            return response(200, body)
        payment = {"payment_id": "same-payment"}
        if self.fault == "payment_replay" and "10.2.240.4" in url:
            payment = {"payment_id": "different-payment"}
        return response(503 if self.fault == "payment_error" else 202, payment)

    async def get(self, url, *, headers):
        self.calls.append(("get", url, None, headers))
        if "/reservation-commands/" in url:
            return response(
                200, {"persistence_status": "FAILED" if self.fault == "durability" else "DURABLE"}
            )
        if headers["Authorization"] != "Bearer " + TOKENS[7]:
            return response(200 if self.fault == "authorization" else 403, {})
        return response(200, {"status": "FULFILLED", "tickets": [{"id": "same-ticket"}]})


def response(status, body):
    return SimpleNamespace(status_code=status, json=lambda: copy.deepcopy(body))


def test_four_pods_one_winner_all_pod_replays_and_authorization():
    client = Client()
    audits = []
    evidence = []
    result = asyncio.run(
        safety.probe(
            EVENT,
            receipts(),
            TOKENS,
            RUN,
            client=client,
            owner_audit=lambda *values: audits.append(values),
            evidence=lambda *values: evidence.append(values),
        )
    )
    assert result["requests_per_pod"] == [25] * 4 and result["accepted_holds"] == 1
    assert result["wave_statuses"] == {"409": 99, "202": 1}
    assert result["all_pod_payment_replay"] and result["other_actor_denied_on_all_pods"]
    assert len(audits) == 1 and audits[0] == (EVENT, BODY, RUN)
    assert len([c for c in client.calls if c[0] == "post" and c[1].endswith("/payments")]) == 4
    assert all(c[2]["duplicates"] == 3 for c in client.calls if c[0] == "post" and c[1].endswith("/payments"))


@pytest.mark.parametrize(
    "fault",
    ["transport", "double", "hold_replay", "durability", "payment_replay", "payment_error", "authorization"],
)
def test_protocol_failure_never_retries_or_claims_safety(fault):
    client = Client(fault)
    evidence = []
    with pytest.raises(ValueError):
        asyncio.run(
            safety.probe(
                EVENT,
                receipts(),
                TOKENS,
                RUN,
                client=client,
                owner_audit=lambda *values: None,
                evidence=lambda *values: evidence.append(values),
            )
        )
    assert len(client.wave) == 100
    assert any(values[0] == "hold-wave-validation" for values in evidence)
    if fault in {"transport", "double", "hold_replay", "durability"}:
        assert not any(c[1].endswith("/payments") for c in client.calls)


@pytest.mark.parametrize("fault", ["missing", "distribution", "duplicate_index", "conflict", "double"])
def test_wave_requires_exactly_one_success_and_full_four_pod_coverage(fault):
    rows = [
        {
            "index": i,
            "pod_index": i % 4,
            "status": 202 if i == 7 else 409,
            "body": BODY if i == 7 else {"code": "SEAT_UNAVAILABLE"},
        }
        for i in range(100)
    ]
    if fault == "missing":
        rows.pop()
    elif fault == "distribution":
        rows[0]["pod_index"] = 1
    elif fault == "duplicate_index":
        rows[0]["index"] = 1
    elif fault == "conflict":
        rows[0]["body"] = {"code": "OVERLOADED"}
    elif fault == "double":
        rows[0].update(status=202, body=BODY)
    with pytest.raises(ValueError):
        safety.wave_view(rows)


def test_invalid_actor_or_endpoints_fail_before_any_request():
    client = Client()
    with pytest.raises(ValueError):
        asyncio.run(safety.probe(EVENT, receipts()[:3], TOKENS, RUN, client=client))
    with pytest.raises(ValueError):
        asyncio.run(safety.probe(EVENT, receipts(), TOKENS, "old-run", client=client))
    with pytest.raises(ValueError):
        asyncio.run(safety.probe(EVENT, receipts(), ["same"] * 100, RUN, client=client))
    assert client.calls == []
