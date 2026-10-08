"""Index correction preserves SQL results, historical gates and frozen images."""
import copy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import apply_orders_event_index as index
import orders_event_index_probe_contract as policy
import run_orders_event_index_probe as runner
import work_envelope as envelope
from test_diagnostic_runner_connection import TARGET
from test_interleaved_refresh_probe import probe_inventory
from test_work_envelope import area  # noqa: F401


class Connection:
    def __init__(self, row=None, *, autocommit=True):
        self.row, self.autocommit, self.calls = row, autocommit, []

    def execute(self, statement):
        self.calls.append(statement)
        if statement == index.INDEX_SQL:
            self.row = (True, True, "btree", False, "event_id", None, 1, True, 123)
        return SimpleNamespace(fetchone=lambda: self.row)


def test_build_and_verification_do_not_recreate_or_drop_the_index():
    conn = Connection()
    result = index.apply(conn)
    assert result["pass"] and result["created"]
    assert index.apply(conn, verify_only=True)["pass"]
    assert conn.calls.count(index.INDEX_SQL) == 1
    assert not any("DROP" in sql for sql in conn.calls)


@pytest.mark.parametrize("position,value", [(0, False), (1, False), (2, "hash"), (3, True),
                                            (4, "status"), (5, "status='PAID'"), (6, 2), (7, False)])
def test_incompatible_existing_index_blocks_ddl(position, value):
    row = [True, True, "btree", False, "event_id", None, 1, True, 123]
    row[position] = value
    conn = Connection(tuple(row))
    with pytest.raises(RuntimeError): index.apply(conn)
    assert index.INDEX_SQL not in conn.calls


def test_missing_verification_only_and_transactional_connections_are_rejected():
    conn = Connection()
    assert index.apply(conn, verify_only=True)["pass"] is False
    assert index.INDEX_SQL not in conn.calls
    with pytest.raises(ValueError): index.apply(Connection(autocommit=False))


def test_profile_keeps_all_images_and_registers_only_the_exact_index_factor(area):  # noqa: F811
    engine = runner.create_runner()
    engine.configure_diagnostic_target(TARGET)
    data = policy.plan()
    c = engine.StatusRefreshContract(data["artifact_receipt"], "candidate", data["expected_runtime_source_sha256"])
    from observe_two_host_pipeline import verify_admission_factor_evidence
    inventory = probe_inventory()
    inventory["status_refresh_contract"] = c.inventory_marker()
    c.verify_inventory(inventory)
    verify_admission_factor_evidence(inventory)
    engine.RefreshStages(False, {}, c, "adr0151-" + "a" * 12)
    assert c.images == data["artifact_receipt"]["images"]
    assert {"scripts/apply_orders_event_index.py", policy.MIGRATION} <= set(engine.identity())
    assert not c.index_verified()
    c.index_before = index.apply(Connection())
    c.index_after = {**copy.deepcopy(c.index_before), "created": False, "verification_only": True}
    assert c.index_verified()
    c.index_after["index"]["oid"] += 1
    assert not c.index_verified()
    binding, _old, _state = area
    binding.update(diagnostic_target_sha256="f" * 64)
    entry = envelope.reserve(binding, data, profile="orders_event_index_probe")
    assert entry["status"] == "ACTIVE" and entry["profile"] == "orders_event_index_probe"
    with pytest.raises(ValueError):
        changed = copy.deepcopy(data)
        changed["database_index"]["columns"] = ["event_id", "status"]
        envelope.reserve(binding, changed, profile="orders_event_index_probe")


def test_remote_programs_compile_and_bind_the_exact_concurrent_statement():
    for verify in (True, False):
        program = policy.index_program(verify_only=verify)
        compile(program, "bound-index", "exec")
        assert "assert namespace['INDEX_SQL']==" in program
        assert "ON public.orders(event_id)" in program


@pytest.mark.parametrize("drift", ["missing", "wrong_table", "wrong_key", "invalid", "oid_bool"])
def test_index_proof_cannot_accept_pass_flags_without_catalog_properties(drift):
    c = object.__new__(policy.OrdersEventIndexProbeContract)
    c.index_before = index.apply(Connection())
    c.index_after = {**copy.deepcopy(c.index_before), "created": False, "verification_only": True}
    if drift == "missing": c.index_before["index"] = c.index_after["index"] = None
    elif drift == "wrong_table": c.index_before["index"]["expected_table"] = c.index_after["index"]["expected_table"] = False
    elif drift == "wrong_key": c.index_before["index"]["key"] = c.index_after["index"]["key"] = "status"
    elif drift == "invalid": c.index_before["index"]["valid"] = c.index_after["index"]["valid"] = False
    else: c.index_before["index"]["oid"] = c.index_after["index"]["oid"] = True
    assert not c.index_verified()


def test_missing_index_proof_fails_the_additional_gate(monkeypatch):
    fake = SimpleNamespace(identity=dict, stage_gates=lambda *args: {
        "failed_gates": [], "all_required_gates_pass": True})
    monkeypatch.setattr(runner, "parent_runner", lambda **kwargs: fake)
    engine = runner.create_runner()
    c = SimpleNamespace(index_verified=lambda: False)
    result = engine.stage_gates({}, {}, {}, c)
    assert not result["all_required_gates_pass"]
    assert "orders_event_index_verified" in result["failed_gates"]
