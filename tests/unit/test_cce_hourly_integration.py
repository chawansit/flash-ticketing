"""Offline hourly bounds, real generated SQL execution and fault gates; no cloud."""

import copy
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_hourly_window as window
import cce_paid_stage as stage
import observe_cce_paid_pipeline as observer
import run_cce_hourly_qualification as entry
import work_envelope as policy
from cce_paid_profiles import HOURLY, SHORT, for_guard
from run_two_host_paid_comparison import job_program


def test_profiles_are_exact_and_separate():
    assert SHORT.duration == 300 and SHORT.expected == 25200 and SHORT.experiment_limit == 3600
    assert HOURLY.duration == 3600 and HOURLY.expected == 302400 and HOURLY.experiment_limit == 5400
    for profile in (SHORT, HOURLY):
        assert for_guard(SimpleNamespace(key=profile.ledger_prefix + "__" + "a" * 12)) == profile
    with pytest.raises(ValueError):
        for_guard(SimpleNamespace(key="bounded_cce_dependency_probe__" + "a" * 12))


def test_generated_arguments_do_not_relax_short_profile():
    directory = "/root/repo/tmp/adr0151-" + "a" * 12 + "-cce-candidate"
    for profile in (SHORT, HOURLY):
        args = stage.generator_arguments(directory, "http://10.1.137.69:8000", 100, profile=profile)
        assert args[args.index("--seconds") + 1] == str(profile.duration)
        assert args[args.index("--completion-deadline-seconds") + 1] == str(profile.completion_deadline)
        assert "--retry" not in args
        compile(job_program(args, directory, hourly=profile == HOURLY), "job", "exec")
        assert ("timeout=3780" if profile == HOURLY else "timeout=600") in job_program(args, directory, hourly=profile == HOURLY)


def test_hourly_plan_requires_full_passing_short_control(monkeypatch):
    import cce_transaction_profile
    monkeypatch.setattr(cce_transaction_profile, "active", lambda: None)
    value = copy.deepcopy(policy.read(policy.ROOT / entry.BASELINE))
    assert entry.plan()["expected_terminal_tickets"] == 302400
    value["measurement_gates"]["full_queue_drain"] = False
    original = policy.read
    monkeypatch.setattr(policy, "read", lambda path: value if path == policy.ROOT / entry.BASELINE else original(path))
    with pytest.raises(ValueError, match="passing short"):
        entry.plan()


@pytest.mark.parametrize("tickets,inner,orders,payments,joined,skew,expected", [
    (302100,301800,302100,302100,302100,0,True),
    (299999,299900,299999,299999,299999,0,False),
    (300001,299999,300001,300001,300001,0,False),
    (302100,301800,302099,302100,302100,0,False),
    (302100,301800,302100,302101,302101,0,False),
    (302401,302100,302401,302401,302401,0,False),
    (302100,301800,302100,302100,302100,3,False),
])
def test_execute_generated_window_audit(monkeypatch, capsys, tickets, inner, orders, payments, joined, skew, expected):
    import json
    start, now = 100000, 104000
    calls = []
    row = (tickets, orders, payments, joined, inner, datetime.fromtimestamp(now + skew, UTC), None, None)
    class Conn:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def transaction(self): return self
        def execute(self, query, args=None):
            calls.append((query, args))
            return SimpleNamespace(fetchone=lambda: row)
    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(connect=lambda *a, **kw: Conn()))
    monkeypatch.setenv("DATABASE_URL", "test-only")
    monkeypatch.setattr("time.time", lambda: now)
    events = [str(uuid4()) for _ in range(1008)]
    exec(compile(window.program(events, start), "window", "exec"), {})  # noqa: S102 - Execute only repository-generated audit code against a fake connection.
    result = json.loads(capsys.readouterr().out)
    assert result["pass"] is expected
    assert calls[0][0] == "SET TRANSACTION READ ONLY"
    assert "t.issued_at<to_timestamp(%s+3600)" in calls[-1][0]
    assert calls[-1][1] == (start, start, events, start, start)


def test_hourly_sampling_keeps_global_counts_fresh():
    clock, scans, global_reads = [0], [], []
    def original(conn, shows):
        scans.append(clock[0])
        return {"issued_tickets": len(scans), "unpublished_outbox": 10, "db_lock_waiters": 11}
    module = SimpleNamespace(sample=original)
    observer.install_hourly_sampling(module, now=lambda: clock[0])
    conn = SimpleNamespace(execute=lambda query: global_reads.append(query) or SimpleNamespace(fetchone=lambda: (2, 3)))
    assert module.sample(conn, ["owned"])["issued_tickets"] == 1
    clock[0] = 9
    sample = module.sample(conn, ["owned"])
    assert sample["issued_tickets"] == 1 and sample["unpublished_outbox"] == 2
    assert sample["db_lock_waiters"] == 3 and sample["cohort_sample_age_seconds"] == 9
    clock[0] = 10
    assert module.sample(conn, ["owned"])["issued_tickets"] == 2
    assert scans == [0, 10] and len(global_reads) == 1


def test_registration_does_not_authorize_hourly_without_explicit_exception():
    data = copy.deepcopy(policy.envelope())
    data["time"]["hourly_qualification_exception"]["experiment_seconds_limit"] = 3600
    with pytest.raises(ValueError, match="90-minute"):
        policy.validate_hourly_allowance(data)


from test_work_envelope import area  # noqa: F401 - Shared isolated ledger fixture.


def hourly_goal(data):
    previous = data["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"]
    return {**previous, "decision": "ADR0232", "profile": HOURLY.name,
            "offered_seconds": 3600, "maximum_experiment_seconds": 5400,
            "ledger_start_index": 0, "acquisition_budget": 20,
            "baseline_sha256": entry.plan()["baseline_sha256"]}


def test_fresh_hourly_reservation_preserves_old_scopes_and_checks_sources(area, monkeypatch):  # noqa: F811 - pytest injects the imported fixture.
    data = policy.read(policy.ENVELOPE)
    data["qualified_profiles"] = list(policy.PROFILES)
    data["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = hourly_goal(data)
    policy.write(policy.ENVELOPE, data)
    policy.validate_hourly_allowance(policy.envelope())
    binding = {"configuration_sha256": data["existing_resource_configuration_sha256"],
               "cce_paid_entry_sources": entry.identity(), "cce_paid_core_sources": entry.core.paid.identity(),
               "baseline_sha256": entry.plan()["baseline_sha256"]}
    for key in ("cce_manifest_sha256", "diagnostic_target_sha256", "saved_api_service_sha256",
                "image_proof_sha256", "cce_resource_source_sha256", "cce_transition_source_sha256"):
        binding[key] = "a" * 64
    old = copy.deepcopy(policy.read(policy.STATE))
    reservation = policy.reserve(binding, entry.plan(), profile=HOURLY.name)
    assert reservation["reserved_seconds"] == 5400
    assert all(policy.read(policy.STATE)[key] == value for key, value in old.items())
    guard = policy.ActionGuard(reservation["ledger"], binding)
    guard.check(4500)
    monkeypatch.setattr(entry, "identity", lambda: {"changed": "b" * 64})
    with pytest.raises(ValueError, match="drift"):
        guard.check()


def test_successful_short_result_cannot_qualify_hourly_without_window_gate(monkeypatch):
    monkeypatch.setattr(entry.core, "outcome", lambda *a, **kw: (True, True, True))
    assert entry.outcome({"native": {"measurement_gates": {}}}, {}, {}) == (True, True, False)


def test_hourly_pods_have_full_bounded_lifetime():
    from test_cce_api_adapter import service
    objects = entry.core.native.objects("adr0151-" + "a" * 12, service(), "10.1.137.69", "user", "test-only", profile=HOURLY)
    assert [obj["spec"]["activeDeadlineSeconds"] for obj in objects[3:]] == [5400] * 4


@pytest.mark.parametrize("shows,age,passes", [(1008,170,True),(1008,600,True),(1008,601,False),(84,170,False)])
def test_hourly_manifest_preparation_age_does_not_relax_short_freshness(shows, age, passes):
    from datetime import timedelta

    from fixture_identity_evidence import fixture_identity
    now = datetime.now(UTC)
    created = now - timedelta(seconds=age)
    fixture = {"schema_version":1,"environment":"development","fixture_layout":"distributed",
               "fixture_id":str(uuid4()),"shows":shows,"seats_per_show":300,
               "show_ids":[str(uuid4()) for _ in range(shows)],"created_at":created.isoformat(),
               "sale_ends":(created + timedelta(hours=2 if shows == 1008 else 1)).isoformat()}
    if passes:
        assert fixture_identity(fixture,shows,now=now)["shows"] == shows
    else:
        with pytest.raises(ValueError,match="Fresh open"):
            fixture_identity(fixture,shows,now=now)


def test_hourly_prefetch_is_bounded_and_other_profiles_keep_original_reads():
    from run_two_host_paid_comparison import fetch
    calls=[]
    class File:
        def __enter__(self):return self
        def __exit__(self,*a):pass
        def stat(self):return SimpleNamespace(st_size=3)
        def prefetch(self,**kw):calls.append(kw)
        def read(self,*a):return b"abc"
    transport=SimpleNamespace(open=lambda *a:File(),close=lambda:None)
    for profile in (SHORT,HOURLY):
        session=SimpleNamespace(action_guard=SimpleNamespace(key=profile.ledger_prefix+"__"+"a"*12),
                                clients={"primary":SimpleNamespace(open_sftp=lambda:transport)})
        assert fetch(session,"primary","owned") == b"abc"
    assert calls == [{"file_size":3,"max_concurrent_requests":16}]
