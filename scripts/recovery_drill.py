"""Host-only local Kafka outage drill. Always restarts Kafka, even if an assertion fails."""

import json
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import httpx
import jwt

from ticketing.config import Settings


def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=Path(__file__).resolve().parents[1], check=True)


def main():
    token = jwt.encode(
        {
            "sub": "recovery-" + uuid4().hex,
            "iss": "ticketing",
            "aud": "ticketing",
            "exp": datetime.now(UTC) + timedelta(minutes=10),
        },
        Settings().jwt_secret,
        algorithm="HS256",
    )
    with httpx.Client(
        base_url="http://localhost:8000", headers={"Authorization": "Bearer " + token}, timeout=5
    ) as client:
        events = client.get("/v1/events").json()
        event_id = events[0]["id"]
        available = [
            s
            for s in client.get(f"/v1/events/{event_id}/seats").json()["seats"]
            if s["status"] == "AVAILABLE"
        ]
        response = client.post(
            "/v1/holds",
            headers={"Idempotency-Key": str(uuid4())},
            json={"event_id": event_id, "seat_ids": [available[-1]["seat_id"]]},
        )
        response.raise_for_status()
        order_id = response.json()["order_id"]
        try:
            compose("stop", "kafka")
            response = client.post(
                f"/v1/orders/{order_id}/payments",
                headers={"Idempotency-Key": str(uuid4())},
                json={"delay_seconds": 0, "duplicates": 3},
            )
            response.raise_for_status()
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                order = client.get(f"/v1/orders/{order_id}").json()
                if order["status"] == "PAID":
                    break
                time.sleep(0.3)
            assert order["status"] == "PAID", order
            assert order["tickets"] == [], order
            print(json.dumps({"kafka_stopped": True, "order_status": "PAID", "tickets": 0}), flush=True)
        finally:
            compose("start", "kafka")
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            order = client.get(f"/v1/orders/{order_id}").json()
            if order["status"] == "FULFILLED":
                assert len(order["tickets"]) == 1, order
                print(
                    json.dumps({"kafka_recovered": True, "order_status": "FULFILLED", "tickets": 1}),
                    flush=True,
                )
                return
            time.sleep(0.5)
        raise AssertionError(f"Fulfillment did not recover: {order}")


if __name__ == "__main__":
    main()
