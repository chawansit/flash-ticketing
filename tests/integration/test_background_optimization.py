from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier, Event
from unittest.mock import Mock
from urllib.error import URLError
from uuid import uuid4

import pytest

from ticketing import workers
from ticketing.config import Settings

pytestmark = pytest.mark.integration


def changed(event_id):
    return {
        "event_id": str(uuid4()),
        "schema_version": 1,
        "event_type": "SeatsChanged",
        "payload": {"event_id": str(event_id)},
    }


def due(db):
    with db.transaction() as conn:
        conn.execute(
            "UPDATE seat_refresh_requests SET next_attempt_at=clock_timestamp()-interval '1 second',"
            "lease_until=clock_timestamp()-interval '1 second'"
        )


def refresh_row(db):
    with db.transaction() as conn:
        return conn.execute("SELECT * FROM seat_refresh_requests").fetchone()


def test_coalesces_changes_and_deduplicates_intent(system):
    svc, db, event_id = system
    svc.reserve("one", event_id, ["A"], "hold")
    envelopes = [changed(event_id) for _ in range(20)]
    for envelope in envelopes:
        workers.consume_event(db, None, envelope)
    workers.consume_event(db, None, envelopes[0])
    row = refresh_row(db)
    assert row["generation"] == 20
    cache = Mock()
    assert workers.refresh_one(db, cache)
    assert cache.put.call_count == 1
    assert cache.put.call_args.args[1] == 1
    assert refresh_row(db)["completed_generation"] == 20
    assert not workers.refresh_one(db, cache)


def test_intent_and_inbox_rollback_together(system, monkeypatch):
    _, db, event_id = system
    original = workers.request_refresh

    def fail(conn, event):
        original(conn, event)
        raise RuntimeError("failure before commit")

    monkeypatch.setattr(workers, "request_refresh", fail)
    with pytest.raises(RuntimeError):
        workers.consume_event(db, None, changed(event_id))
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM seat_refresh_requests").fetchone()["n"] == 0


def test_change_during_refresh_is_not_lost(system, monkeypatch):
    svc, db, event_id = system
    svc.reserve("one", event_id, ["A"], "first")
    workers.consume_event(db, None, changed(event_id))
    original = workers.snapshot
    cache = Mock()
    original_requested_at = refresh_row(db)["requested_at"]

    def overlap(database, target, event):
        original(database, target, event)
        svc.reserve("two", event_id, ["B"], "second")
        workers.consume_event(db, None, changed(event_id))

    with monkeypatch.context() as patch:
        patch.setattr(workers, "snapshot", overlap)
        workers.refresh_one(db, cache)
    row = refresh_row(db)
    assert (row["generation"], row["completed_generation"]) == (2, 1)
    assert row["requested_at"] > original_requested_at
    due(db)
    workers.refresh_one(db, cache)
    assert cache.put.call_count == 2
    assert cache.put.call_args.args[1] == 2
    assert refresh_row(db)["completed_generation"] == 2


@pytest.mark.parametrize("after_write", [False, True])
def test_refresh_failure_keeps_dirty_work(system, monkeypatch, after_write):
    _, db, event_id = system
    workers.consume_event(db, None, changed(event_id))
    cache = Mock()
    original = workers.snapshot

    def fail(database, target, event):
        if after_write:
            original(database, target, event)
        raise RuntimeError("refresh interrupted")

    with monkeypatch.context() as patch:
        patch.setattr(workers, "snapshot", fail)
        with pytest.raises(RuntimeError):
            workers.refresh_one(db, cache)
    assert refresh_row(db)["completed_generation"] == 0
    assert not workers.refresh_one(db, cache)
    due(db)
    assert workers.refresh_one(db, cache)
    assert refresh_row(db)["completed_generation"] == 1


def test_refresh_batch_acknowledges_success_and_keeps_failed_work_leased(system):
    _, db, event_id = system
    failed_event = uuid4()
    with db.transaction() as conn:
        conn.execute(
            """INSERT INTO events VALUES (%s,'Second','THB',clock_timestamp()-interval '1 day',
            clock_timestamp()+interval '1 day')""",
            (failed_event,),
        )
        conn.execute(
            "INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,'A',100)",
            (failed_event,),
        )
        workers.request_refresh(conn, event_id)
        workers.request_refresh(conn, failed_event)

    cache = Mock()

    def put_many(updates):
        return [
            RuntimeError("cache unavailable") if target_event == str(failed_event) else True
            for target_event, _version, _data in updates
        ]

    cache.put_many.side_effect = put_many
    with pytest.raises(RuntimeError, match="cache unavailable"):
        workers.refresh_batch(db, cache, limit=10)

    with db.transaction() as conn:
        rows = conn.execute(
            """SELECT event_id,generation,completed_generation,lease_token
            FROM seat_refresh_requests ORDER BY event_id"""
        ).fetchall()
    state = {row["event_id"]: row for row in rows}
    assert state[event_id]["completed_generation"] == state[event_id]["generation"] == 1
    assert state[event_id]["lease_token"] is None
    assert state[failed_event]["completed_generation"] == 0
    assert state[failed_event]["generation"] == 1
    assert state[failed_event]["lease_token"] is not None
    assert cache.put_many.call_count == 1


def test_stale_projector_cannot_ack_new_lease(system, monkeypatch):
    _, db, event_id = system
    workers.consume_event(db, None, changed(event_id))

    def steal_lease(*_):
        with db.transaction() as conn:
            conn.execute("UPDATE seat_refresh_requests SET lease_token=%s", (uuid4(),))

    with monkeypatch.context() as patch:
        patch.setattr(workers, "snapshot", steal_lease)
        assert workers.refresh_one(db, Mock())
    assert refresh_row(db)["completed_generation"] == 0
    due(db)
    assert workers.refresh_one(db, Mock())
    assert refresh_row(db)["completed_generation"] == 1


@pytest.mark.parametrize("failure", ["send", "ack"])
def test_publisher_preserves_partial_acknowledgements(system, failure):
    svc, db, event_id = system
    for seat in ["A", "B", "C"]:
        svc.reserve(seat, event_id, [seat], seat)
    producer = Mock()
    good = Mock()
    bad = Mock()
    bad.get.side_effect = TimeoutError("ack unknown")
    producer.send.side_effect = (
        [good, TimeoutError("send failed")] if failure == "send" else [good, good, bad]
    )
    with pytest.raises(TimeoutError):
        workers.publish_batch(db, producer, 3)
    with db.transaction() as conn:
        rows = conn.execute("SELECT * FROM outbox_events").fetchall()
        assert sum(r["published_at"] is not None for r in rows) == (1 if failure == "send" else 2)
        remaining = {str(r["id"]) for r in rows if r["published_at"] is None}
        conn.execute("UPDATE outbox_events SET lease_until=clock_timestamp()-interval '1 second'")
    recovered = Mock()
    assert workers.publish_batch(db, recovered, 3)
    assert {call.kwargs["value"]["event_id"] for call in recovered.send.call_args_list} == remaining
    with db.transaction() as conn:
        assert (
            conn.execute("SELECT count(*) AS n FROM outbox_events WHERE published_at IS NULL").fetchone()["n"]
            == 0
        )


def test_stale_publisher_cannot_ack_new_lease(system):
    svc, db, event_id = system
    svc.reserve("one", event_id, ["A"], "one")
    producer = Mock()

    def steal_lease(**_):
        with db.transaction() as conn:
            conn.execute("UPDATE outbox_events SET lease_token=%s", (uuid4(),))

    producer.send.return_value.get.side_effect = steal_lease
    workers.publish_batch(db, producer)
    with db.transaction() as conn:
        assert conn.execute("SELECT published_at FROM outbox_events").fetchone()["published_at"] is None


class Response:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def read(self):
        return b"{}"


def payment(svc, event_id, seat):
    hold = svc.reserve(seat, event_id, [seat], seat)
    return svc.initiate_payment(seat, hold["order_id"], seat, "SUCCEEDED", 0, 3)


def test_simulator_excludes_a_second_dispatch_for_leased_payment(system, monkeypatch):
    svc, db, event_id = system
    payment(svc, event_id, "A")
    entered, release = Event(), Event()

    def send(*_, **__):
        entered.set()
        assert release.wait(5)
        return Response()

    monkeypatch.setattr(workers.urllib.request, "urlopen", send)
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(workers.simulate_one, db, Settings())
        try:
            assert entered.wait(5)
            assert workers.simulate_one(db, Settings()) is False
        finally:
            release.set()
        assert first.result(timeout=5)
    with db.transaction() as conn:
        assert conn.execute("SELECT deliveries FROM payment_attempts").fetchone()["deliveries"] == 1


def test_simulator_batch_dispatches_distinct_payments_concurrently(system, monkeypatch):
    svc, db, event_id = system
    payment(svc, event_id, "A")
    payment(svc, event_id, "B")
    barrier = Barrier(2)

    def send(*_, **__):
        barrier.wait(timeout=5)
        return Response()

    monkeypatch.setattr(workers.urllib.request, "urlopen", send)
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert workers.simulate_batch(db, replace(Settings(), simulator_concurrency=2), pool)
    with db.transaction() as conn:
        assert [
            r["deliveries"] for r in conn.execute("SELECT deliveries FROM payment_attempts").fetchall()
        ] == [1, 1]


def test_simulator_failed_http_retries_after_lease(system, monkeypatch):
    svc, db, event_id = system
    payment(svc, event_id, "A")
    monkeypatch.setattr(workers.urllib.request, "urlopen", Mock(side_effect=URLError("offline")))
    with pytest.raises(URLError):
        workers.simulate_one(db, Settings())
    assert not workers.simulate_one(db, Settings())
    with db.transaction() as conn:
        row = conn.execute("SELECT deliveries FROM payment_attempts").fetchone()
        assert row["deliveries"] == 0
        conn.execute("UPDATE payment_attempts SET lease_until=clock_timestamp()-interval '1 second'")
    monkeypatch.setattr(workers.urllib.request, "urlopen", Mock(return_value=Response()))
    assert workers.simulate_one(db, Settings())


def test_consumer_batch_coalesces_refreshes_and_replay_is_idempotent(system):
    _, db, event_id = system
    first = changed(event_id)
    first["payload"]["seats"] = ["A", "B"]
    second = changed(event_id)
    second["payload"]["seats"] = ["B", "C"]

    workers.consume_events(db, None, [first, second])

    with db.transaction() as conn:
        row = conn.execute("SELECT * FROM seat_refresh_requests").fetchone()
        inbox_count = conn.execute("SELECT count(*) AS n FROM consumer_inbox").fetchone()["n"]
    assert row["generation"] == 1
    assert set(row["seat_ids"]) == {"A", "B", "C"}
    assert inbox_count == 2

    workers.consume_events(db, None, [first, second])

    with db.transaction() as conn:
        replayed = conn.execute("SELECT * FROM seat_refresh_requests").fetchone()
        replayed_inbox_count = conn.execute(
            "SELECT count(*) AS n FROM consumer_inbox"
        ).fetchone()["n"]
    assert replayed["generation"] == 1
    assert set(replayed["seat_ids"]) == {"A", "B", "C"}
    assert replayed_inbox_count == 2


def test_consumer_batch_full_refresh_supersedes_partial_seats(system):
    _, db, event_id = system
    partial = changed(event_id)
    partial["payload"]["seats"] = ["A"]
    full = changed(event_id)

    assert workers.consume_refresh_batch(db, [partial, full]) == 2

    row = refresh_row(db)
    assert row["generation"] == 1
    assert row["seat_ids"] is None


def test_consumer_batch_rejects_non_refresh_without_writing_inbox(system):
    _, db, event_id = system
    envelope = changed(event_id)
    envelope["event_type"] = "TicketsIssued"

    with pytest.raises(ValueError, match="Only SeatsChanged"):
        workers.consume_refresh_batch(db, [envelope])

    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox").fetchone()["n"] == 0


def test_consumer_batch_uses_only_first_payload_for_duplicate_event_id(system):
    _, db, event_id = system
    first = changed(event_id)
    first["payload"]["seats"] = ["A"]
    conflicting_duplicate = {
        **first,
        "payload": {"event_id": str(event_id), "seats": ["B"]},
    }

    assert workers.consume_refresh_batch(db, [first, conflicting_duplicate]) == 1

    row = refresh_row(db)
    assert row["generation"] == 1
    assert row["seat_ids"] == ["A"]

def test_changed_snapshot_batch_reads_and_patches_multiple_events(system):
    _, db, first_event = system
    second_event = uuid4()
    with db.transaction() as conn:
        conn.execute(
            """INSERT INTO events VALUES (%s,'Second','THB',clock_timestamp()-interval '1 day',
            clock_timestamp()+interval '1 day')""",
            (second_event,),
        )
        conn.execute(
            "INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,'A',100)",
            (second_event,),
        )

    cache = Mock()
    cache.patch_many.return_value = [True, True]
    requests = [
        {"event_id": first_event, "seat_ids": ["A", "B"]},
        {"event_id": second_event, "seat_ids": ["A"]},
    ]

    completed, errors = workers.changed_snapshot_batch(db, cache, requests)

    assert completed == {first_event, second_event}
    assert errors == []
    updates = list(cache.patch_many.call_args.args[0])
    assert [event_id for event_id, _seats in updates] == [
        str(first_event),
        str(second_event),
    ]
    assert [[seat["seat_id"] for seat in seats] for _event_id, seats in updates] == [
        ["A", "B"],
        ["A"],
    ]


def test_full_snapshot_batch_reads_and_pipelines_multiple_events(system):
    _, db, first_event = system
    second_event = uuid4()
    with db.transaction() as conn:
        conn.execute(
            """INSERT INTO events VALUES (%s,'Second','THB',clock_timestamp()-interval '1 day',
            clock_timestamp()+interval '1 day')""",
            (second_event,),
        )
        conn.execute(
            "INSERT INTO event_seats(event_id,seat_id,price) VALUES (%s,'A',100)",
            (second_event,),
        )

    cache = Mock()
    cache.put_many.return_value = [True, True]

    completed, errors = workers.full_snapshot_batch(
        db, cache, [str(first_event), str(second_event)]
    )

    assert completed == {first_event, second_event}
    assert errors == []
    updates = cache.put_many.call_args.args[0]
    assert [event_id for event_id, _version, _data in updates] == [
        str(first_event),
        str(second_event),
    ]
    assert [[seat["seat_id"] for seat in data["seats"]] for _, _, data in updates] == [
        ["A", "B", "C"],
        ["A"],
    ]


def test_pooled_callback_failure_preserves_lease_then_duplicate_delivery_is_idempotent(system):
    import hashlib
    import hmac
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from urllib.error import HTTPError

    from ticketing.infrastructure.payment_transport import CallbackTransport

    svc, db, event_id = system
    payment(svc, event_id, "A")
    settings = Settings()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            timestamp = self.headers["X-Payment-Timestamp"]
            expected = hmac.new(settings.webhook_secret.encode(), timestamp.encode() + b"." + raw,
                                hashlib.sha256).hexdigest()
            assert hmac.compare_digest(self.headers["X-Payment-Signature"], expected)
            payload = json.loads(raw)
            self.server.callback_ids.append(payload["callback_id"])
            if self.server.accept:
                svc.callback(payload)
            self.send_response(200 if self.server.accept else 503)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.accept = False
    server.callback_ids = []
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    transport = CallbackTransport(f"http://127.0.0.1:{server.server_port}/callback", 2)
    try:
        with pytest.raises(HTTPError):
            workers.simulate_one(db, settings, transport)
        assert len(server.callback_ids) == 1  # No immediate resend.
        assert not workers.simulate_one(db, settings, transport)
        with db.transaction() as conn:
            row = conn.execute("SELECT deliveries,lease_until FROM payment_attempts").fetchone()
            assert row["deliveries"] == 0 and row["lease_until"] is not None
            conn.execute("UPDATE payment_attempts SET lease_until=clock_timestamp()-interval '1 second'")
        server.accept = True
        for _ in range(3):
            assert workers.simulate_one(db, settings, transport)
        assert not workers.simulate_one(db, settings, transport)
        assert len(set(server.callback_ids)) == 1
        with db.transaction() as conn:
            assert conn.execute("SELECT deliveries FROM payment_attempts").fetchone()["deliveries"] == 3
            assert conn.execute("SELECT count(*) AS n FROM payment_callbacks").fetchone()["n"] == 1
            assert conn.execute("SELECT count(*) AS n FROM payment_attempts WHERE status='SUCCEEDED'").fetchone()["n"] == 1
            assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 1
            assert conn.execute("SELECT count(*) AS n FROM outbox_events WHERE event_type='OrderPaid'").fetchone()["n"] == 1
    finally:
        transport.close()
        server.shutdown()
        server.server_close()
        thread.join(3)


@pytest.mark.parametrize("full", [False, True])
def test_interleaved_refresh_coalesces_without_changing_business_order(system, full):
    svc, db, event_id = system
    hold = svc.reserve("buyer", event_id, ["A"], "hold")
    attempt = svc.initiate_payment("buyer", hold["order_id"], "pay", "SUCCEEDED", 0, 1)
    svc.callback({"callback_id": str(uuid4()), "payment_id": attempt["payment_id"],
        "order_id": hold["order_id"], "amount": hold["total"], "currency": hold["currency"], "outcome": "SUCCEEDED"})
    paid = {"event_id": str(uuid4()), "schema_version": 1, "event_type": "OrderPaid", "payload": {"order_id": hold["order_id"]}}
    issued = {**paid, "event_id": str(uuid4()), "event_type": "TicketsIssued"}
    first, second = changed(event_id), changed(event_id)
    first["payload"]["seats"] = ["A"]
    if not full:
        second["payload"]["seats"] = ["B"]
    projector = Mock()
    batch = [first, paid, second, issued]
    workers.consume_events(db, None, batch, order_status_projector=projector)
    assert [c.kwargs["event_type"] for c in projector.refresh.call_args_list] == ["OrderPaid", "TicketsIssued"]
    assert svc.get_order("buyer", hold["order_id"])["status"] == "FULFILLED"
    row = refresh_row(db)
    assert row["generation"] == 1
    assert row["seat_ids"] is None if full else set(row["seat_ids"]) == {"A", "B"}
    workers.consume_events(db, None, batch, order_status_projector=projector)
    assert refresh_row(db)["generation"] == 1
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox").fetchone()["n"] == 4
        assert conn.execute("SELECT count(*) AS n FROM outbox_events WHERE event_type='TicketsIssued'").fetchone()["n"] == 1


def test_deferred_refresh_failure_replays_after_committed_ticket_without_duplicates(system, monkeypatch):
    svc, db, event_id = system
    hold = svc.reserve("buyer", event_id, ["A"], "hold")
    attempt = svc.initiate_payment("buyer", hold["order_id"], "pay", "SUCCEEDED", 0, 1)
    svc.callback({"callback_id": str(uuid4()), "payment_id": attempt["payment_id"],
        "order_id": hold["order_id"], "amount": hold["total"], "currency": hold["currency"], "outcome": "SUCCEEDED"})
    paid = {"event_id": str(uuid4()), "schema_version": 1, "event_type": "OrderPaid", "payload": {"order_id": hold["order_id"]}}
    first, second = changed(event_id), changed(event_id)
    first["payload"]["seats"] = ["A"]
    second["payload"]["seats"] = ["B"]
    batch = [first, paid, second]
    original = workers.request_refresh

    def fail(conn, target, seats):
        original(conn, target, seats)
        raise RuntimeError("crash before deferred refresh commit")

    with monkeypatch.context() as patch:
        patch.setattr(workers, "request_refresh", fail)
        with pytest.raises(RuntimeError, match="deferred refresh commit"):
            workers.consume_events(db, None, batch)
    assert svc.get_order("buyer", hold["order_id"])["status"] == "FULFILLED"
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM seat_refresh_requests").fetchone()["n"] == 0
    workers.consume_events(db, None, batch)
    workers.consume_events(db, None, batch)
    assert refresh_row(db)["generation"] == 1
    assert set(refresh_row(db)["seat_ids"]) == {"A", "B"}
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox").fetchone()["n"] == 3
        assert conn.execute("SELECT count(*) AS n FROM outbox_events WHERE event_type='TicketsIssued'").fetchone()["n"] == 1


def test_overlapping_refresh_batches_with_opposite_show_order_make_progress(system):
    _, db, first = system
    second = uuid4()
    with db.transaction() as conn:
        conn.execute("INSERT INTO events VALUES (%s,'Second','THB',clock_timestamp()-interval '1 day',clock_timestamp()+interval '1 day')", (second,))
    barrier = Barrier(2)

    def process(show_ids):
        completed = 0
        for _ in range(20):
            batch = [changed(show) for show in show_ids]
            barrier.wait(timeout=10)
            workers.consume_refresh_batch(db, batch)
            completed += 1
        return completed

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(process, ids) for ids in ((first, second), (second, first))]
        assert [f.result(timeout=30) for f in futures] == [20, 20]
    with db.transaction() as conn:
        rows = conn.execute("SELECT generation FROM seat_refresh_requests ORDER BY event_id").fetchall()
        assert [r["generation"] for r in rows] == [40, 40]
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox").fetchone()["n"] == 80
