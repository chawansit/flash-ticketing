import sys
from collections import defaultdict, deque
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import qualify_two_host_deployment as driver
import run_two_host_paid_comparison as runner


@pytest.fixture
def transport(monkeypatch):
    clients, errors, channels = [], defaultdict(deque), []

    class SSHException(Exception):
        pass

    class AuthenticationException(SSHException):
        pass

    class BadHostKeyException(SSHException):
        pass

    class ChannelException(SSHException):
        pass

    class Client:
        def __init__(self):
            self.closed, self.connects, self.commands = False, [], []
            clients.append(self)

        def load_host_keys(self, path):
            assert path == "pinned-hosts"

        def set_missing_host_key_policy(self, policy):
            assert policy == "reject"

        def connect(self, host, **options):
            assert options["username"] == "root"
            assert options["password"] == "synthetic-secret"
            assert not options["look_for_keys"] and not options["allow_agent"]
            assert options["timeout"] == 12
            self.connects.append((host, options))
            if errors[host]:
                raise errors[host].popleft()

        def get_transport(self):
            return self

        def is_active(self):
            return not self.closed

        def open_channel(self, kind, destination, origin, **options):
            assert kind == "direct-tcpip" and destination == ("10.0.0.2", 22)
            assert origin == ("127.0.0.1", 0) and options["timeout"] == 12
            channel = SimpleNamespace(closed=False)
            channel.close = lambda: setattr(channel, "closed", True)
            channels.append(channel)
            return channel

        def set_keepalive(self, value):
            assert value == 30

        def close(self):
            self.closed = True

        def exec_command(self, code, **options):
            self.commands.append(code)
            raise ConnectionResetError("synthetic reset")

    module = SimpleNamespace(SSHClient=Client, RejectPolicy=lambda: "reject",
                             SSHException=SSHException, AuthenticationException=AuthenticationException,
                             BadHostKeyException=BadHostKeyException, ChannelException=ChannelException)
    monkeypatch.setitem(sys.modules, "paramiko", module)
    return SimpleNamespace(clients=clients, errors=errors, channels=channels, module=module)


def config(fallback=False):
    return {"known_hosts": "pinned-hosts", "secondary_ssh_private_fallback": fallback,
            **{role: {"host": role, "private_ipv4": f"10.0.0.{i}"}
               for i, role in enumerate(("primary", "secondary", "generator"), 1)}}


def test_call_never_replays_and_explicit_reconnect_is_bounded(transport, tmp_path):
    session = driver.Session(config(), tmp_path, "synthetic-secret")
    first = session.clients["primary"]
    with pytest.raises(ConnectionResetError):
        session.call("primary", "print('{}')")
    assert len(first.commands) == 1
    assert len(transport.clients) == 3
    session.reconnect("primary")
    assert first.closed and len(transport.clients) == 4
    session.reconnect("primary")
    with pytest.raises(RuntimeError, match="budget exhausted"):
        session.reconnect("primary")
    assert len(transport.clients) == 5
    assert session.state["ssh_reconnect_attempts"] == {"primary": 2}
    assert "synthetic-secret" not in (tmp_path / "state.json").read_text()
    session.close()
    assert session._password is None and all(c.closed for c in transport.clients)
    with pytest.raises(ValueError):
        session.reconnect("primary")


def test_opt_in_private_hop_keeps_public_host_key_identity_and_invalidates_on_parent_reconnect(transport, tmp_path):
    transport.errors["secondary"].append(TimeoutError())
    session = driver.Session(config(True), tmp_path, "synthetic-secret")
    child = session.clients["secondary"]
    assert session.state["ssh_routes"]["secondary"] == "pinned_private_hop"
    assert child.connects[0][0] == "secondary"
    assert child.connects[0][1]["sock"] is transport.channels[0]
    session.reconnect("primary")
    assert child.closed
    session.reconnect("secondary")
    assert session.state["ssh_routes"]["secondary"] == "direct"
    session.close()


@pytest.mark.parametrize("kind", ["timeout", "auth", "key", "channel"])
def test_no_fallback_by_default_and_never_on_auth_or_key_rejection(transport, tmp_path, kind):
    exception = {"timeout": TimeoutError, "auth": transport.module.AuthenticationException,
                 "key": transport.module.BadHostKeyException, "channel": transport.module.ChannelException}[kind]
    transport.errors["secondary"].append(exception())
    session = driver.Session.__new__(driver.Session)
    with pytest.raises(exception):
        session.__init__(config(kind != "timeout"), tmp_path, "synthetic-secret")
    assert not transport.channels
    assert all(client.closed for client in transport.clients)
    assert session._password is None


def test_invalid_private_address_cannot_be_used_as_hop(transport, tmp_path):
    transport.errors["secondary"].append(TimeoutError())
    settings = config(True)
    settings["secondary"]["private_ipv4"] = "8.8.8.8"
    with pytest.raises(ValueError):
        driver.Session(settings, tmp_path, "synthetic-secret")
    assert not transport.channels and all(c.closed for c in transport.clients)


def test_failed_hop_closes_channel_and_partial_clients(transport, tmp_path):
    transport.errors["secondary"].extend([TimeoutError(), transport.module.AuthenticationException()])
    with pytest.raises(transport.module.AuthenticationException):
        driver.Session(config(True), tmp_path, "synthetic-secret")
    assert transport.channels[0].closed
    assert all(c.closed for c in transport.clients)


def test_invalid_fallback_option_fails_before_connect(transport, tmp_path):
    settings = config()
    settings["secondary_ssh_private_fallback"] = "true"
    with pytest.raises(ValueError):
        driver.Session(settings, tmp_path, "synthetic-secret")
    assert not transport.clients


@pytest.mark.parametrize("failure", [EOFError(), ConnectionResetError(), TimeoutError()])
@pytest.mark.parametrize("role", ["secondary", "container"])
def test_owned_stop_reconnects_once_without_launch_replay(monkeypatch, role, failure):
    calls, reconnects = [], []
    job = {"owned": True}
    monkeypatch.setattr(runner, "process_program", lambda value, stop: "identity-guarded-stop" if value is job and stop else None)

    def invoke(*args):
        calls.append(args)
        if len(calls) == 1:
            raise failure
        return {"running": False}

    session = SimpleNamespace(api=invoke, call=invoke, reconnect=reconnects.append)
    assert runner.Stages(False, {}).stop(session, role, "cid", job) == {"running": False}
    assert len(calls) == 2 and calls[0] == calls[1]
    assert reconnects == ["primary" if role == "container" else role]


@pytest.mark.parametrize("failure", [RuntimeError("ownership mismatch"), ValueError("wrong PID"), OSError("local error")])
def test_ownership_and_nontransport_errors_are_never_retried(monkeypatch, failure):
    calls, reconnects = [], []
    monkeypatch.setattr(runner, "process_program", lambda *_args, **_kwargs: "guarded")

    def invoke(*args):
        calls.append(args)
        raise failure

    session = SimpleNamespace(call=invoke, reconnect=reconnects.append)
    with pytest.raises(type(failure)):
        runner.Stages(False, {}).stop(session, "secondary", None, {})
    assert len(calls) == 1 and not reconnects


def test_second_stop_transport_failure_propagates_without_more_attempts(monkeypatch):
    calls, reconnects = [], []
    monkeypatch.setattr(runner, "process_program", lambda *_args, **_kwargs: "guarded")

    def invoke(*args):
        calls.append(args)
        raise ConnectionResetError()

    with pytest.raises(ConnectionResetError):
        runner.Stages(False, {}).stop(SimpleNamespace(call=invoke, reconnect=reconnects.append), "secondary", None, {})
    assert len(calls) == 2 and reconnects == ["secondary"]


def test_paramiko_authorization_errors_are_not_transport_failures(transport):
    assert driver.transport_failure(transport.module.SSHException())
    assert not any(driver.transport_failure(kind()) for kind in (
        transport.module.AuthenticationException, transport.module.BadHostKeyException, transport.module.ChannelException))


def test_failed_reconnect_consumes_budget_without_replaying_rpc(transport, tmp_path):
    session = driver.Session(config(), tmp_path, "synthetic-secret")
    transport.errors["primary"].append(TimeoutError())
    with pytest.raises(TimeoutError):
        session.reconnect("primary")
    assert session.state["ssh_reconnect_attempts"] == {"primary": 1}
    assert not any(client.commands for client in transport.clients)
    session.close()


def test_close_failure_cannot_skip_other_transports_or_secret_clearance(transport, tmp_path):
    session = driver.Session(config(), tmp_path, "synthetic-secret")

    def broken():
        raise OSError("synthetic close failure")

    session.clients["primary"].close = broken
    session.close()
    assert session._password is None and not session.clients and session._closed
    assert session.state["ssh_close_errors"] == ["OSError"]
    assert transport.clients[1].closed and transport.clients[2].closed
