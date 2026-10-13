import configparser
import hashlib
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"scripts"))
import cce_backend_partition as b


@pytest.fixture
def original(monkeypatch):
    raw = b"""[databases]
ticketing = host=postgres port=5432 auth_user=ticketing
[pgbouncer]
pool_mode = transaction
default_pool_size = 24
reserve_pool_size = 0
server_tls_sslmode = verify-full
auth_type = scram-sha-256
admin_users = postgres
unix_socket_dir =
"""
    monkeypatch.setattr(b,"ORIGINAL_CONFIG",hashlib.sha256(raw).hexdigest())
    return raw


def test_partition_preserves_security_and_original_config(original):
    before=configparser.ConfigParser();before.read_string(original.decode())
    for phase in ("prepare","active"):
        after=configparser.ConfigParser();after.read_string(b.render(original,phase=phase).decode())
        assert set(after["databases"])==set(b.ALIASES)
        for key,value in before["pgbouncer"].items():
            assert after["pgbouncer"][key]==value
        assert after["pgbouncer"]["max_user_connections"]=="24"
        for alias,limit in b.ALIASES.items():
            expected=24 if phase=="prepare" and alias=="ticketing" else limit
            assert "pool_size="+str(expected)+" " in after["databases"][alias]
            assert "max_db_connections="+str(expected)+" " in after["databases"][alias]
            assert "dbname=ticketing " in after["databases"][alias]
    assert sum(b.ALIASES.values())==24


def test_unknown_phase_and_changed_original_are_rejected(original):
    with pytest.raises(ValueError):b.render(original,phase="unbounded")
    with pytest.raises(ValueError):b.render(original+b"\n")


def test_route_preserves_authority_options_and_credentials():
    url="postgresql://ticketing:encoded%23pass@pgbouncer:5432/ticketing?sslmode=prefer&application_name=api"
    assert b.route(url,"ticketing_foreground")==url.replace("/ticketing?","/ticketing_foreground?")
    for invalid in (url.replace("ticketing:","other:"),url.replace("/ticketing?","/other?")):
        with pytest.raises(ValueError):b.route(invalid,"ticketing_foreground")
    with pytest.raises(ValueError):b.route(url,"ticketing_foregound")


def connection(pools,stats):
    return SimpleNamespace(execute=lambda sql:SimpleNamespace(fetchall=lambda:pools if sql=="SHOW POOLS" else stats))


def sample():
    pools=[{"database":alias,"user":"ticketing","sv_active":cap,"cl_waiting":1,
            "maxwait":0,"maxwait_us":1000} for alias,cap in b.ALIASES.items()]
    stats=[{"database":alias,"total_xact_count":5} for alias in b.ALIASES]
    return pools,stats


def test_all_aliases_are_counted_at_full_budget():
    pools,stats=sample()
    view=b.view(connection(pools,stats),"ticketing",("sv_active","cl_waiting"),("total_xact_count",))
    assert view["physical_backends"]==24 and view["pools"]["cl_waiting"]==3
    assert view["stats"]["total_xact_count"]==15 and set(view["aliases"])==set(b.ALIASES)


@pytest.mark.parametrize("fault",["missing_pool","missing_stats","unknown_pool","wrong_user","over_budget","negative","nan"])
def test_incomplete_or_excessive_budget_never_passes(fault):
    pools,stats=sample()
    if fault=="missing_pool":pools.pop()
    elif fault=="missing_stats":stats.pop()
    elif fault=="unknown_pool":pools.append({"database":"other","user":"ticketing"})
    elif fault=="wrong_user":pools[0]["user"]="other"
    elif fault=="over_budget":pools[0]["sv_active"]+=1
    elif fault=="negative":pools[0]["sv_active"]=-1
    else:pools[0]["sv_active"]=float("nan")
    with pytest.raises(ValueError):b.view(connection(pools,stats),"ticketing",("sv_active",),("total_xact_count",))


def isolation_goal():
    import cce_recovery_headroom as h
    import work_envelope as p
    from test_cce_writer_batch import goal as parent
    value={k:v for k,v in parent().items() if k not in h.BATCH_KEYS}
    return {**value,"backend_partition_decision":"ADR0317",
            "backend_partition_contract_sha256":p.digest(b.contract())}


def test_partition_requires_exact_scope_and_cannot_combine_batch_change():
    value=isolation_goal()
    assert b.selected(value)
    assert not b.selected(None)
    for mutation in ({"backend_partition_contract_sha256":"0"*64},
                     {"backend_partition_decision":"ADR0318"},
                     {"writer_batch_decision":"ADR0316","writer_batch_failure_sha256":b.BASELINE,"writer_batch_size":8}):
        with pytest.raises(ValueError):b.selected({**value,**mutation})


@pytest.mark.parametrize("role",["consumer","projection-consumer","reservation-writer","publisher","maintenance","reconciler","simulator"])
def test_worker_alias_only_preserves_all_other_settings(monkeypatch,role):
    import cce_committed_status_profile as committed
    import cce_native_workers as workers
    import cce_recovery_headroom as h
    import cce_transaction_profile as transaction
    goal=isolation_goal()
    old={k:v for k,v in goal.items() if k not in h.PARTITION_KEYS}
    old["baseline_sha256"]=h.SLOT_SHA256
    values={"DATABASE_URL":"postgresql://ticketing:local-test@pgbouncer:5432/ticketing?application_name=worker",
            "ORDER_STATUS_COMMITTED_SNAPSHOT":"0","RESERVATION_WRITER_BATCH_SIZE":"4"}
    if role=="simulator":values["API_URL"]="http://load-balancer:8000"
    monkeypatch.setattr(committed,"selected",lambda x:True)
    monkeypatch.setattr(transaction,"active",lambda:h.runtime_view(old))
    before=workers.environment(role,values,"10.1.2.3","ADR0315")
    monkeypatch.setattr(transaction,"active",lambda:h.runtime_view(goal))
    after=workers.environment(role,values,"10.1.2.3","ADR0315")
    assert {k for k in before if before[k]!=after[k]}=={"DATABASE_URL"}
    assert after["DATABASE_URL"]==before["DATABASE_URL"].replace("/ticketing?","/ticketing_background?")


@pytest.mark.parametrize("namespace_removed",[False,True])
def test_pooler_rollback_precedes_original_worker_restart(monkeypatch,namespace_removed):
    import cce_worker_transition as w
    from cce_worker_transition import WorkerTransition
    events=[]
    monkeypatch.setattr(w,"change_program",lambda *a:"restart-owned")
    monkeypatch.setattr(w.Transition,"restore",lambda self:events.append("API_restore") or True)
    transition=WorkerTransition.__new__(WorkerTransition)
    transition.session=SimpleNamespace(begin_cleanup=lambda:None,
        call=lambda *a:events.append("worker_restart") or {"owned_worker_transition_verified":True,"running":True})
    transition.deployment=SimpleNamespace(attempted=True,cleanup=lambda:events.append("namespace_delete") or {"namespace_removed":namespace_removed})
    transition.backend_partition=SimpleNamespace(record={"attempted":True},restore=lambda:events.append("pooler_restore"))
    transition.worker_stop_attempted=True;transition.worker_runtime=SimpleNamespace(attempted=True)
    transition.worker_rows=[];transition.decision="ADR0315";transition.normal_decision="ADR0298"
    transition.record={};transition.checkpoint=lambda:None
    if namespace_removed:
        assert transition.restore()
        assert events==["namespace_delete","pooler_restore","worker_restart","API_restore"]
    else:
        with pytest.raises(ValueError):transition.restore()
        assert events==["namespace_delete"]


def test_pause_attempt_is_durable_before_lost_signal_response():
    from cce_backend_partition_runtime import Partition
    actor=Partition.__new__(Partition)
    actor.record={"paused":False}
    checkpoints=[]
    actor.checkpoint=lambda:checkpoints.append(dict(actor.record))
    def lost_signal(value):
        assert value=="USR1"
        raise TimeoutError("signal response lost")
    actor.signal=lost_signal
    with pytest.raises(TimeoutError):actor.pause()
    assert checkpoints==[{"paused":True}]


def test_configuration_write_attempt_survives_lost_response():
    import hashlib

    from cce_backend_partition_runtime import Partition
    actor=Partition.__new__(Partition)
    actor.run="adr0151-123456789abc"
    actor.info={"uid":70,"gid":70,"mode":0o600}
    actor.record={}
    checkpoints=[]
    actor.check=lambda:None
    actor.checkpoint=lambda:checkpoints.append(dict(actor.record))
    actor.prefix=lambda:""
    def lost_write(*args):
        assert actor.record["write_attempted_sha256"]==hashlib.sha256(b"local-test").hexdigest()
        assert "created=0; trap" in args[1]
        raise TimeoutError("write acknowledgement lost")
    actor.session=SimpleNamespace(call=lost_write)
    with pytest.raises(TimeoutError):actor.write(b"local-test")
    assert checkpoints==[{"write_attempted_sha256":hashlib.sha256(b"local-test").hexdigest()}]


def test_admin_dsn_timeout_is_overridden_once(monkeypatch):
    import contextlib
    import io
    import json

    import psycopg
    from cce_backend_partition_runtime import ADMIN
    monkeypatch.setenv("DATABASE_URL","postgresql://ticketing:local-test@pgbouncer:5432/ticketing?connect_timeout=2")
    captured={}
    class Connection:
        def __enter__(self):return self
        def __exit__(self,*args):return False
        def execute(self,sql):
            assert sql=="SHOW SERVERS"
            return SimpleNamespace(fetchall=list)
    def connect(**params):
        captured.update(params)
        return Connection()
    monkeypatch.setattr(psycopg,"connect",connect)
    output=io.StringIO()
    with contextlib.redirect_stdout(output):exec(ADMIN,{"names":["SERVERS"]})  # noqa: S102 - execute the trusted diagnostic literal in its regression test
    assert captured["connect_timeout"]==5 and captured["dbname"]=="pgbouncer"
    assert captured["autocommit"] is True and captured["user"]=="ticketing"
    assert json.loads(output.getvalue())=={"SERVERS":[]}
