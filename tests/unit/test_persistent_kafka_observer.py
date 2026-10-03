"""Lag monitoring must never mutate delivery state or fabricate zero lag."""

import io
import json
from types import SimpleNamespace as NS

import pytest
from kafka import TopicPartition
from kafka.errors import KafkaTimeoutError

from scripts import kafka_lag_observe as module

TOPIC = "ticketing.events"


def group(members=None, state="Stable"):
    members = members or [("member-a", [0]), ("member-b", [1])]
    return NS(error_code=0, state=state, protocol_type="consumer", members=[
        NS(member_id=name, member_assignment=NS(assignment=[(TOPIC, parts)]))
        for name, parts in members
    ])


class Admin:
    def __init__(self, descriptions=None, offsets=None):
        self.descriptions = descriptions or [group(), group()]
        self.offsets = offsets or {TopicPartition(TOPIC, 0): NS(offset=10),
                                   TopicPartition(TOPIC, 1): NS(offset=20)}
        self.closed = False

    def describe_consumer_groups(self, groups):
        assert groups == ["test-group"]
        return [self.descriptions.pop(0)] if len(self.descriptions) > 1 else self.descriptions

    def list_consumer_group_offsets(self, group_id, partitions):
        assert group_id == "test-group"
        assert set(partitions) == {TopicPartition(TOPIC, 0), TopicPartition(TOPIC, 1)}
        return self.offsets

    def close(self):
        self.closed = True


class EndReader:
    def __init__(self):
        self.closed = False
        self.ends = {TopicPartition(TOPIC, 0): 14, TopicPartition(TOPIC, 1): 21}

    def partitions_for_topic(self, topic):
        assert topic == TOPIC
        return {0, 1}

    def end_offsets(self, partitions):
        assert set(partitions) == set(self.ends)
        return self.ends

    def close(self, autocommit, timeout_ms):
        assert autocommit is False and timeout_ms == 1000
        self.closed = True


def test_lag_and_ownership_match_cli_without_fetch_join_or_commit():
    admin, reader = Admin(), EndReader()
    observer = module.ReadOnlyKafkaObserver(admin, reader, TOPIC)
    row = observer.sample("test-group")
    cli = module.parse_describe(
        "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID\n"
        "g ticketing.events 0 10 14 4 member-a\n"
        "g ticketing.events 1 20 21 1 member-b\n"
    )
    assert row == cli
    assert "member-a" not in json.dumps(row)
    # Fakes deliberately have no subscribe/assign/poll/commit/seek methods.
    observer.close()
    assert admin.closed and reader.closed


@pytest.mark.parametrize("descriptions", [
    [group(state="PreparingRebalance")],
    [group(), group([("member-c", [0]), ("member-b", [1])])],
    [group([("member-a", [0, 1]), ("member-b", [1])])],
    [group([("member-a", [0])])],
])
def test_transitional_duplicate_or_missing_ownership_fails(descriptions):
    observer = module.ReadOnlyKafkaObserver(Admin(descriptions), EndReader(), TOPIC)
    with pytest.raises(ValueError):
        observer.sample("test-group")


@pytest.mark.parametrize("offsets", [
    {TopicPartition(TOPIC, 0): NS(offset=10)},
    {TopicPartition(TOPIC, 0): NS(offset=-1), TopicPartition(TOPIC, 1): NS(offset=20)},
    {TopicPartition(TOPIC, 0): NS(offset=15), TopicPartition(TOPIC, 1): NS(offset=20)},
])
def test_missing_uncommitted_or_truncated_offsets_never_become_zero_lag(offsets):
    observer = module.ReadOnlyKafkaObserver(Admin(offsets=offsets), EndReader(), TOPIC)
    with pytest.raises(ValueError):
        observer.sample("test-group")


def test_client_configuration_disables_group_membership_and_auto_commit(monkeypatch):
    import kafka
    import kafka.admin

    calls = []
    admin, reader = Admin(), EndReader()

    def create_admin(**kwargs):
        calls.append(("admin", kwargs))
        return admin

    def create_reader(*args, **kwargs):
        assert args == ()
        assert kwargs["group_id"] is None
        assert kwargs["enable_auto_commit"] is False
        assert kwargs["allow_auto_create_topics"] is False
        assert kwargs["request_timeout_ms"] == 5000
        calls.append(("reader", kwargs))
        return reader

    monkeypatch.setattr(kafka.admin, "KafkaAdminClient", create_admin)
    monkeypatch.setattr(kafka, "KafkaConsumer", create_reader)
    observer = module.python_observer("test-broker", TOPIC)
    observer.close()
    assert len(calls) == 2 and admin.closed and reader.closed


def test_reader_startup_failure_closes_admin(monkeypatch):
    import kafka
    import kafka.admin

    admin = Admin()
    monkeypatch.setattr(kafka.admin, "KafkaAdminClient", lambda **kwargs: admin)

    def fail(**kwargs):
        raise KafkaTimeoutError("private broker details")

    monkeypatch.setattr(kafka, "KafkaConsumer", fail)
    with pytest.raises(KafkaTimeoutError):
        module.python_observer("test-broker", TOPIC)
    assert admin.closed


def test_sample_errors_remain_visible_after_reconnect_and_clients_close(monkeypatch):
    clock = [0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(module.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    handlers = {}
    monkeypatch.setattr(module.signal, "signal", lambda sig, fn: handlers.setdefault(sig, fn))
    clients = []

    class Observer:
        def __init__(self, fail=False):
            self.fail = fail
            self.closed = False
            clients.append(self)

        def sample(self, _group):
            clock[0] += 0.2
            if self.fail:
                raise KafkaTimeoutError("private broker details")
            return {"total_lag": 0}

        def close(self):
            self.closed = True

    monkeypatch.setattr(module, "python_observer",
                        lambda bootstrap, topic: Observer(fail=not clients))
    args = NS(backend="python", container="unused", bootstrap_server="test",
              group="test-group", topic=TOPIC, seconds=3, interval=1)
    output = io.StringIO()
    module.observe(args, output)
    rows = [json.loads(line) for line in output.getvalue().splitlines()]
    assert rows[0]["error_type"] == "KafkaTimeoutError"
    assert rows[-1]["total_lag"] == 0
    assert "private broker details" not in output.getvalue()
    assert len(clients) == 2 and all(client.closed for client in clients)


def test_sigterm_closes_persistent_clients_without_starting_another_sample(monkeypatch):
    handlers = {}
    monkeypatch.setattr(module.signal, "signal", lambda sig, fn: handlers.setdefault(sig, fn))
    closed = []

    class Observer:
        def sample(self, _group):
            handlers[module.signal.SIGTERM](module.signal.SIGTERM, None)
            return {"total_lag": 0}

        def close(self):
            closed.append(True)

    monkeypatch.setattr(module, "python_observer", lambda *args: Observer())
    args = NS(backend="python", container="unused", bootstrap_server="test",
              group="test-group", topic=TOPIC, seconds=10, interval=2)
    output = io.StringIO()
    module.observe(args, output)
    assert len(output.getvalue().splitlines()) == 1 and closed == [True]
