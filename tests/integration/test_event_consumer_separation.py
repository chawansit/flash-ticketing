"""ADR0265: real PostgreSQL locking, commit replay, and split/mixed compatibility."""
import json
from uuid import uuid4

import pytest
from psycopg.errors import LockNotAvailable

from ticketing import workers

pytestmark = pytest.mark.integration


def changed(show, seats):
    return {"schema_version": 1, "event_id": str(uuid4()), "event_type": "SeatsChanged",
            "payload": {"event_id": str(show), "seats": seats}}


def paid(system, seat="A", buyer="buyer"):
    svc, _db, show = system
    hold = svc.reserve(buyer, show, [seat], str(uuid4()))
    attempt = svc.initiate_payment(buyer, hold["order_id"], str(uuid4()), "SUCCEEDED", 0, 3)
    callback = {"callback_id": str(uuid4()), "payment_id": attempt["payment_id"],
                "order_id": hold["order_id"], "amount": hold["total"],
                "currency": hold["currency"], "outcome": "SUCCEEDED"}
    assert svc.callback(callback)["status"] == "book"
    assert svc.callback(callback)["status"] == "duplicate"
    callback["callback_id"] = str(uuid4())
    assert svc.callback(callback)["status"] == "duplicate"
    envelope = {"schema_version": 1, "event_id": str(uuid4()), "event_type": "OrderPaid",
                "payload": {"order_id": hold["order_id"]}}
    return hold, envelope


def assert_ticket(svc, db, hold, buyer="buyer"):
    assert svc.get_order(buyer, hold["order_id"])["status"] == "FULFILLED"
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM tickets").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM bookings").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM payment_attempts WHERE status='SUCCEEDED'").fetchone()["n"] == 1
        assert conn.execute("SELECT count(*) AS n FROM outbox_events WHERE event_type='TicketsIssued'").fetchone()["n"] == 1


def test_refresh_row_lock_does_not_delay_later_paid_event(system):
    svc, db, show = system
    hold, payment = paid(system)
    first, second = changed(show, ["A"]), changed(show, ["B"])
    workers.consume_lane_events(db, None, [first], lane="projection")
    with db.transaction() as blocker:
        blocker.execute("SELECT event_id FROM seat_refresh_requests WHERE event_id=%s FOR UPDATE", (show,))
        with pytest.raises(LockNotAvailable):
            workers.consume_lane_events(db, None, [second, payment], lane="projection")
        # New business work commits even while the refresh row remains locked.
        workers.consume_lane_events(db, None, [second, payment], lane="fulfillment")
        assert_ticket(svc, db, hold)
        with db.transaction() as conn:
            assert conn.execute("SELECT count(*) AS n FROM consumer_inbox WHERE event_id=%s", (second["event_id"],)).fetchone()["n"] == 0
    workers.consume_lane_events(db, None, [second, payment], lane="projection")
    # Lost offset acknowledgement / restart replays both lanes.
    workers.consume_lane_events(db, None, [first, second, payment], lane="fulfillment")
    workers.consume_lane_events(db, None, [first, second, payment], lane="projection")
    assert_ticket(svc, db, hold)
    with db.transaction() as conn:
        row = conn.execute("SELECT generation,seat_ids FROM seat_refresh_requests").fetchone()
        assert row["generation"] == 2 and set(row["seat_ids"]) == {"A", "B"}
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox").fetchone()["n"] == 3


@pytest.mark.parametrize("initial_mode", ["mixed", "split"])
def test_rollout_and_rollback_replay_preserve_durable_effects(system, initial_mode):
    svc, db, show = system
    hold, payment = paid(system)
    batch = [changed(show, ["A"]), payment, changed(show, ["B"])]
    modes = [initial_mode, "mixed" if initial_mode == "split" else "split", initial_mode]
    for mode in modes:
        for lane in (["mixed"] if mode == "mixed" else ["projection", "fulfillment"]):
            workers.consume_lane_events(db, None, batch, lane=lane)
    assert_ticket(svc, db, hold)
    with db.transaction() as conn:
        row = conn.execute("SELECT generation,seat_ids FROM seat_refresh_requests").fetchone()
        assert row["generation"] == 1 and set(row["seat_ids"]) == {"A", "B"}
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox").fetchone()["n"] == 3


def test_refresh_commit_lost_response_replays_without_new_generation(system):
    _, db, show = system
    batch = [changed(show, ["A"]), changed(show, ["B"])]
    workers.consume_lane_events(db, None, batch, lane="projection")
    workers.consume_lane_events(db, None, batch, lane="projection")
    with db.transaction() as conn:
        assert conn.execute("SELECT generation FROM seat_refresh_requests").fetchone()["generation"] == 1
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox").fetchone()["n"] == 2


def test_projection_poison_fallback_cannot_consume_payment(system, monkeypatch):
    svc, db, _ = system
    hold, payment = paid(system)
    poison = {"schema_version": 999, "event_id": str(uuid4()), "event_type": "SeatsChanged", "payload": {}}
    from types import SimpleNamespace
    messages = [SimpleNamespace(value=json.dumps(e).encode()) for e in [poison, payment]]
    monkeypatch.setattr(workers.time, "sleep", lambda _: None)
    workers.consume_kafka_messages(db, None, messages, lane="projection")
    assert svc.get_order("buyer", hold["order_id"])["status"] == "PAID"
    with db.transaction() as conn:
        assert conn.execute("SELECT count(*) AS n FROM consumer_inbox").fetchone()["n"] == 0
        assert conn.execute("SELECT count(*) AS n FROM dead_letters").fetchone()["n"] == 1
    workers.consume_lane_events(db, None, [payment], lane="fulfillment")
    assert_ticket(svc, db, hold)


def test_real_kafka_groups_progress_independently_and_replay(system, monkeypatch):
    import os
    import time
    from dataclasses import replace

    from kafka import KafkaProducer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic

    from ticketing.config import Settings

    bootstrap = os.getenv("TEST_KAFKA_BOOTSTRAP")
    if not bootstrap:
        pytest.skip("TEST_KAFKA_BOOTSTRAP not configured")
    suffix = uuid4().hex
    topic = "test-event-lanes-" + suffix
    monkeypatch.setattr(workers, "EVENT_TOPIC", topic)
    monkeypatch.setattr(workers, "FULFILLMENT_GROUP", "test-fulfillment-" + suffix)
    monkeypatch.setattr(workers, "PROJECTION_GROUP", "test-projection-" + suffix)
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    settings = replace(Settings(), kafka_bootstrap=bootstrap, event_consumer_separation=True)
    producer = fulfillment = projection = None
    svc, db, show = system
    hold, payment = paid(system)
    seed, change = changed(show, ["A"]), changed(show, ["B"])
    workers.consume_lane_events(db, None, [seed], lane="projection")
    try:
        fulfillment = workers.create_event_consumer(settings, "consumer")
        projection = workers.create_event_consumer(settings, "projection-consumer")
        for consumer in (fulfillment, projection):
            deadline = time.monotonic() + 20
            while not consumer.assignment() and time.monotonic() < deadline:
                assert not consumer.poll(timeout_ms=200)
            assert consumer.assignment()
        producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all",
                                 value_serializer=lambda value: json.dumps(value).encode())
        for value in (change, payment):
            producer.send(topic, value=value, key=b"same-order", partition=0).get(timeout=10)
        messages = []
        deadline = time.monotonic() + 10
        while len(messages) < 2 and time.monotonic() < deadline:
            for batch in projection.poll(timeout_ms=200).values():
                messages.extend(batch)
        assert len(messages) == 2
        tp = TopicPartition(topic, 0)
        with db.transaction() as blocker:
            blocker.execute("SELECT event_id FROM seat_refresh_requests WHERE event_id=%s FOR UPDATE", (show,))
            with pytest.raises(LockNotAvailable):
                workers.consume_lane_events(db, None, [json.loads(m.value) for m in messages], lane="projection")
            deadline = time.monotonic() + 2
            while (fulfillment.committed(tp) or 0) < 2 and time.monotonic() < deadline:
                workers.consume_iteration(fulfillment, db, None, 100, lane="fulfillment")
            assert fulfillment.committed(tp) == 2
            assert projection.committed(tp) is None
            assert_ticket(svc, db, hold)
        workers.consume_kafka_messages(db, None, messages, lane="projection")
        projection.commit()
        assert projection.committed(tp) == 2
        projection.close()
        projection = workers.create_event_consumer(settings, "projection-consumer")
        # Redeliver the same envelopes at new offsets after restarting projection.
        for value in (change, payment):
            producer.send(topic, value=value, key=b"same-order", partition=0).get(timeout=10)
        for consumer, lane in ((fulfillment, "fulfillment"), (projection, "projection")):
            deadline = time.monotonic() + 20
            while (consumer.committed(tp) or 0) < 4 and time.monotonic() < deadline:
                workers.consume_iteration(consumer, db, None, 100, lane=lane)
            assert consumer.committed(tp) == 4
        assert_ticket(svc, db, hold)
        with db.transaction() as conn:
            assert conn.execute("SELECT generation FROM seat_refresh_requests").fetchone()["generation"] == 2
            assert conn.execute("SELECT count(*) AS n FROM consumer_inbox").fetchone()["n"] == 3
    finally:
        for consumer in (fulfillment, projection):
            if consumer:
                consumer.close()
        if producer:
            producer.close()
        admin.delete_topics([topic])
        admin.close()
