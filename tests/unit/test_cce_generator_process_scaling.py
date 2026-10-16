import asyncio
import copy
import hashlib
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_customer_outcome_slo as slo
import cce_generator_process_scaling as scaling
import cce_recovery_headroom as headroom
import cce_throughput_push as push
import work_envelope as policy
from test_cce_fulfillment_batch import goal as parent_goal
from test_cce_recovery_headroom import parent, reads
from test_customer_outcome_slo import customer


def goal():
    value = parent_goal()
    value.update(
        throughput_push_decision="ADR0329",
        offered_journeys_per_second=160,
        generator_process_decision="ADR0333",
    )
    value["generator_process_contract_sha256"] = policy.digest(scaling.contract())
    value["throughput_push_contract_sha256"] = policy.digest(push.contract(value))
    value["headroom_contract_sha256"] = policy.digest(headroom.contract(value))
    return value


def workload():
    bundle, coord = parent()
    bundle, coord = reads(bundle, coord)
    return headroom.qualify_workload(bundle, coord, goal())


def four_customer():
    value = customer(expected=48000)
    shard = {
        "scheduled": 12000,
        "dispatched": 12000,
        "generator_drops": 0,
        "fulfilled_by_deadline": 12000,
        "outcomes": {"fulfilled": 12000},
    }
    value.update(
        shards=[copy.deepcopy(shard) for _ in range(4)], generator_shards=4, shard_exit_codes=[0, 0, 0, 0]
    )
    return value


def test_exact_scope_preserves_budgets_and_runtime_view():
    actual = goal()
    assert scaling.selected(actual) and scaling.selected(headroom.runtime_view(actual))
    assert headroom.selected(actual)
    assert not scaling.selected(None) and not scaling.selected({})
    binding = {"cce_generator_process_contract": scaling.contract()}
    assert scaling.validate_binding(actual, binding)
    with pytest.raises(ValueError):
        scaling.validate_binding(actual, {})
    assert scaling.contract()["total_http_clients"] == 16
    assert scaling.contract()["total_concurrency"] == 500


@pytest.mark.parametrize(
    "field,value",
    [
        ("generator_concurrency", 1000),
        ("offered_journeys_per_second", 200),
        ("offered_seconds", 600),
        ("maximum_paid_stages", 2),
        ("generator_process_contract_sha256", "0" * 64),
        ("generator_process_decision", "ADR0332"),
    ],
)
def test_cannot_expand_or_partially_select(field, value):
    with pytest.raises(ValueError):
        scaling.selected({**goal(), field: value})


def test_only_coordinator_changes_and_four_raw_results_are_required(tmp_path):
    bundle, coord = workload()
    changed, current = scaling.qualify_workload(bundle, coord)
    name = "scripts/paid_ticket_sharded_generator.py"
    assert {k for k in bundle if bundle[k] != changed[k]} == {name}
    assert hashlib.sha256(changed[name]).hexdigest().encode() in current
    module = ModuleType("four_shards")
    exec(compile(changed[name], name, "exec"), module.__dict__)
    rows = []
    for _ in range(4):
        row = customer(expected=12000)
        row.update(pass_=True)
        rows.append(row)
    assert module.aggregate(rows, 160, 300, 500, [0, 0, 0, 0], recovery_max_attempts=3)["pass"]
    assert not module.aggregate(rows[:3], 160, 300, 500, [0, 0, 0], recovery_max_attempts=3)["pass"]
    parts = module.split_manifest(
        {
            "schema_version": 1,
            "environment": "development",
            "show_ids": [str(i) for i in range(160)],
            "viewer_tokens": [str(i) for i in range(48000)],
            "seats_per_show": 300,
            "seat_offset": 0,
        },
        4,
        12000,
    )
    assert [len(p["show_ids"]) for p in parts] == [40] * 4
    assert len(set(s for p in parts for s in p["show_ids"])) == 160
    args = SimpleNamespace(
        rate=200,
        seconds=300,
        concurrency=500,
        http_client_count=8,
        poll_seconds=1,
        duplicates=1,
        recovery_max_attempts=3,
    )
    with pytest.raises(ValueError, match="Exact ADR0333"):
        asyncio.run(module.run(args, {"fixture_layout": "distributed"}))


def test_leaf_or_coordinator_drift_rejected():
    bundle, coord = workload()
    with pytest.raises(ValueError):
        scaling.qualify_workload({**bundle, "scripts/paid_ticket_load_generator.py": b"drift"}, coord)
    with pytest.raises(ValueError):
        scaling.qualify_workload(bundle, b"drift")


def test_four_process_accounting_requires_explicit_expected_count():
    value = four_customer()
    with pytest.raises(ValueError):
        slo.evaluate(value, 48000)
    assert slo.evaluate(value, 48000, expected_shards=4)["pass"]
    assert slo.evaluate(customer(expected=48000), 48000)["pass"]
    value["shard_exit_codes"] = [0, 0, 0]
    assert not slo.evaluate(value, 48000, expected_shards=4)["pass"]
    value["shard_exit_codes"] = [0, 0, 0, 2]
    assert not slo.evaluate(value, 48000, expected_shards=4)["pass"]
    value["shard_exit_codes"] = [False, 0, 0, 0]
    assert not slo.evaluate(value, 48000, expected_shards=4)["pass"]


def test_drop_and_latency_gates_unchanged_with_four_processes():
    for key, value in (
        ("generator_drops", 1),
        ("dispatch_lag_p95_ms", 101),
        ("dispatch_lag_max_ms", 501),
        ("distinct_tickets", 47999),
    ):
        assert not slo.evaluate({**four_customer(), key: value}, 48000, expected_shards=4)["pass"]


@pytest.mark.parametrize("slots", [500, 800])
@pytest.mark.parametrize("case", ["clean", "drops", "missing", "checkpoint", "bad_checkpoint"])
def test_actual_four_child_run_retains_complete_failed_measurements(tmp_path, monkeypatch, case, slots):
    import json

    bundle, coord = workload()
    g = occupancy_goal() if slots == 800 else None
    changed, _ = scaling.qualify_workload(bundle, coord, g)
    name = "scripts/paid_ticket_sharded_generator.py"
    module = ModuleType("four_child_run")
    module.__file__ = str(tmp_path / "paid_ticket_sharded_generator.py")
    exec(compile(changed[name], name, "exec"), module.__dict__)
    calls = []

    async def launch(*command, **_kwargs):
        def argument(name):
            return command[command.index(name) + 1]
        output = Path(argument("--output"))
        index = len(calls)
        calls.append(command)
        row = customer(expected=12000)
        row.update(measurement_complete=True, accounting_complete=True,
                   rate_target_per_second=40, dispatch_seconds=300)
        if case == "drops":
            for key in ("dispatched", "completed", "fulfilled", "fulfilled_by_deadline",
                        "distinct_orders", "distinct_tickets"):
                row[key] = 10000
            row.update(generator_drops=2000, outcomes={"fulfilled": 10000})
            row["pass"] = False
        if index == 3 and case in {"checkpoint", "bad_checkpoint"}:
            row.update(kind="paid_journey_checkpoint", measurement_complete=False,
                       accounting_complete=False, rate_target_per_second=80 if case == "bad_checkpoint" else 40)
            output = output.with_suffix(output.suffix + ".progress.json")
        if not (case == "missing" and index == 3):
            output.write_text(json.dumps(row), encoding="utf-8")
        code = 0 if case == "clean" else 1
        async def wait():
            return code
        return SimpleNamespace(returncode=code, wait=wait)

    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", launch)
    args = SimpleNamespace(rate=160, seconds=300, concurrency=slots, http_client_count=8,
                           poll_seconds=1, duplicates=1, recovery_max_attempts=3,
                           origin="http://synthetic.invalid", completion_deadline_seconds=420)
    manifest = {"schema_version": 1, "environment": "development", "fixture_layout": "distributed",
                "show_ids": [str(i) for i in range(160)], "viewer_tokens": [str(i) for i in range(48000)],
                "seats_per_show": 300, "seat_offset": 0}
    result = asyncio.run(module.run(args, manifest))
    assert len(calls) == 4
    assert all(command[command.index("--concurrency") + 1] == str(slots // 4) for command in calls)
    assert all(command[command.index("--http-max-connections") + 1] == "125" for command in calls)
    assert all(command[command.index("--http-client-count") + 1] == "4" for command in calls)
    assert all(command[command.index("--rate") + 1] == "40" for command in calls)
    assert result["pass"] is (case == "clean")
    if case in {"clean", "drops"}:
        assert result["measurement_complete"] and result["accounting_complete"]
        assert result["dispatched"] == (48000 if case == "clean" else 40000)
        assert result["generator_drops"] == (0 if case == "clean" else 8000)
        assert result["final_customer_failures"] == 0
        assert "retained_lower_bound_counts" not in result
    else:
        assert result["measurement_complete"] is False
        assert result["dispatched"] is None
        assert result["shard_evidence_states"][-1] == {
            "missing": "missing", "checkpoint": "checkpoint", "bad_checkpoint": "invalid"}[case]



def occupancy_goal():
    value = goal()
    value.update(generator_process_decision="ADR0335", generator_concurrency=800)
    value["generator_process_contract_sha256"] = policy.digest(scaling.contract(value))
    value["throughput_push_contract_sha256"] = policy.digest(push.contract(value))
    value["headroom_contract_sha256"] = policy.digest(headroom.contract(value))
    return value


def test_occupancy_metadata_launch_and_leaf_change_agree(monkeypatch):
    import cce_api_adapter as api
    import cce_paid_stage as stage
    import cce_transaction_profile as transaction
    from cce_paid_profiles import RATE_PUSH_160

    g = occupancy_goal()
    assert scaling.selected(g) and scaling.selected(headroom.runtime_view(g))
    assert headroom.selected(g)
    assert scaling.contract(g)["total_concurrency"] == 800
    assert scaling.contract(g)["total_http_connections"] == 500
    assert scaling.contract(g)["total_http_clients"] == 16
    assert scaling.contract()["total_concurrency"] == 500
    monkeypatch.setattr(transaction, "active", lambda: headroom.runtime_view(g))
    workload_metadata = api.contract()["workload"]
    assert workload_metadata["concurrency"] == 800
    assert workload_metadata["http_max_connections"] == 500
    args = stage.generator_arguments("/root/load/tmp/adr0151-0123456789ab-cce-candidate",
                                     __import__("cce_nginx_ingress").origin(transaction.active()),
                                     1234567890, profile=RATE_PUSH_160, recovery_max_attempts=3)
    assert args[args.index("--concurrency") + 1] == "800"
    bundle, coord = workload()
    candidate, _ = scaling.qualify_workload(bundle, coord, g)
    assert {k for k in bundle if bundle[k] != candidate[k]} == {
        "scripts/paid_ticket_sharded_generator.py", "scripts/paid_ticket_load_generator.py"}
    leaf = "scripts/paid_ticket_load_generator.py"
    before = b"if not args.concurrency <= args.http_max_connections <= 4000:"
    after = b"if (args.concurrency, args.http_max_connections, client_count) != (200, 125, 4):"
    assert candidate[leaf] == bundle[leaf].replace(before, after)


@pytest.mark.parametrize("fault,value", [("generator_concurrency", 500), ("generator_concurrency", 801),
    ("offered_journeys_per_second", 200), ("maximum_paid_stages", 2),
    ("maximum_experiment_seconds", 7200), ("generator_process_contract_sha256", "0" * 64)])
def test_occupancy_cannot_expand_or_partially_select(fault, value):
    with pytest.raises(ValueError):
        scaling.selected({**occupancy_goal(), fault: value})


@pytest.mark.parametrize("active,http,clients", [(200,125,4), (200,200,4), (201,125,4), (200,125,8)])
def test_real_leaf_validation_decouples_only_exact_resource_pair(tmp_path, active, http, clients):
    import ast
    from datetime import UTC, datetime, timedelta

    from paid_fixture_layout import validate_child_allocation

    bundle, coord = workload()
    candidate, _ = scaling.qualify_workload(bundle, coord, occupancy_goal())
    tree = ast.parse(candidate["scripts/paid_ticket_load_generator.py"])
    node = next(v for v in tree.body if isinstance(v, ast.FunctionDef) and v.name == "validate")
    namespace = {"datetime": datetime, "UTC": UTC, "timedelta": timedelta,
                 "validate_child_allocation": validate_child_allocation}
    exec(compile(ast.Module(body=[node], type_ignores=[]), "actual_leaf_validate", "exec"), namespace)
    args = SimpleNamespace(origin="http://synthetic.invalid", rate=40, seconds=300,
                           recovery_max_attempts=3, concurrency=active, http_max_connections=http,
                           http_client_count=clients, duplicates=1, timeout_seconds=30,
                           poll_seconds=1, completion_deadline_seconds=420, output=tmp_path/"fresh.json")
    manifest = {"schema_version":1,"environment":"development","origin":args.origin,
                "show_ids":[str(i) for i in range(40)],"viewer_tokens":["synthetic"],
                "seat_offset":0,"seats_per_show":300,
                "expires_at":(datetime.now(UTC)+timedelta(hours=1)).isoformat()}
    if (active,http,clients) == (200,125,4):
        assert namespace["validate"](args, manifest) == 12000
    else:
        with pytest.raises(ValueError):
            namespace["validate"](args, manifest)



@pytest.mark.parametrize("fault", [None,"active","reference","recovery","status","budget","paid_count"])
def test_occupancy_reference_requires_sealed_recovery_and_same_application(monkeypatch, fault):
    prior = {"application_image":"pinned", "database_connections":24}
    report = {"binding_sha256":policy.digest(prior), "pass":False,"restoration_complete":True,
              "integrity_verified":False,"transport_credentials_cleared":True}
    expected = policy.digest(report)
    recovery = {"original_result_sha256":expected,"pass":True,"gates":{"all_financial_predicates":True},
                "raw_shard_accounting":{"dispatched":40095,"generator_drops":7905},
                "fresh_financial":{"pass":True,"hold_deadlines_elapsed":True}}
    entry = {"ledger":scaling.OCCUPANCY_REFERENCE_LEDGER,"status":"FAILED_RESTORED",
             "result_sha256":expected,"paid_runs_started":1,"recovery":{"sha256":policy.digest(recovery)}}
    scope = {"binding":prior,"active_run":None,"paid_runs_started":1}
    actual = copy.deepcopy(prior)
    if fault == "active":scope["active_run"]="unresolved"
    elif fault == "reference":report["pass"]=True
    elif fault == "recovery":recovery["pass"]=False
    elif fault == "status":entry["status"]="RECOVERY_REQUIRED"
    elif fault == "budget":actual["database_connections"]=25
    elif fault == "paid_count":scope["paid_runs_started"]=2
    original_read = policy.read
    def read(path):
        if Path(path) == policy.STATE:return {scaling.OCCUPANCY_REFERENCE_LEDGER:scope}
        if str(path).endswith("cce-comparison.private.json"):return report
        if str(path).endswith("four-generator-160-recovery-2026-10-17.json"):return recovery
        return original_read(path)
    monkeypatch.setattr(policy,"read",read)
    monkeypatch.setattr(policy,"envelope",lambda:{})
    monkeypatch.setattr(policy,"journal",lambda _: {"experiments":[entry]})
    monkeypatch.setattr(scaling,"OCCUPANCY_REFERENCE_SHA256",expected)
    if fault:
        with pytest.raises(ValueError):scaling.validate_occupancy_reference(actual)
    else:
        assert scaling.validate_occupancy_reference(actual)
