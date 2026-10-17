"""Explicit rate, lineage and integrity boundaries for the adopted recovery workload."""
import copy
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_recovery_headroom as h
import work_envelope as p
from cce_paid_profiles import HEADROOM, HOURLY, SHORT, TICKET_TARGET, for_guard
from customer_gateway_read_bundle import qualify as reads
from test_gateway_read_bundle import parent


def goal():
    return {"decision": "ADR0277", "extension_decision": "ADR0277",
            "profile": "cce_ticket_target", "headroom_decision": "ADR0312",
            "headroom_contract_sha256": p.digest(h.contract()), "baseline_sha256": h.BASELINE_SHA256,
            "offered_journeys_per_second": 102, "offered_seconds": 300,
            "maximum_experiment_seconds": 3600, "maximum_paid_stages": 1,
            "generator_concurrency": 500, "acquisition_budget": 20,
            "maximum_pods": 18, "pod_vcpu": 1, "pod_memory_gib": 2,
            "payment_slot_arm": "candidate", "gateway_read_recovery_decision": "ADR0311"}


def test_profile_selection_does_not_mutate_historical_bounds(monkeypatch):
    monkeypatch.setattr(p, "envelope", lambda: {"spending": {"temporary_cce_pilot_exception":
                                               {"goal_bounded_authorization": goal()}}})
    assert (SHORT.expected, HOURLY.expected, TICKET_TARGET.expected) == (25200, 302400, 50400)
    assert for_guard(SimpleNamespace(key="bounded_cce_ticket_target__000000000000", binding={})) == TICKET_TARGET
    guard = SimpleNamespace(key="bounded_cce_ticket_target__000000000000",
                            binding={"cce_headroom_contract": h.contract()})
    assert for_guard(guard) == HEADROOM and HEADROOM.expected == 30600
    guard.binding["cce_headroom_contract"]["offered_rate"] = 104
    with pytest.raises(ValueError):
        for_guard(guard)


@pytest.mark.parametrize("field,value", [("offered_journeys_per_second", 104),
    ("maximum_paid_stages", 2), ("generator_concurrency", 1000), ("acquisition_budget", 24),
    ("maximum_pods", 20), ("baseline_sha256", "0" * 64), ("headroom_contract_sha256", "0" * 64)])
def test_partial_or_expanded_authorization_is_rejected(field, value):
    assert not h.selected(None) and not h.selected({})
    with pytest.raises(ValueError):
        h.selected({**goal(), field: value})


def test_application_view_preserves_actual_rate_and_cannot_authorize_a_different_goal(monkeypatch):
    actual = goal()
    monkeypatch.setattr(p, "envelope", lambda: {"spending": {"temporary_cce_pilot_exception":
                                                {"goal_bounded_authorization": actual}}})
    view = h.runtime_view(actual)
    assert h.selected(view) and actual["offered_journeys_per_second"] == 102
    assert view["offered_journeys_per_second"] == 84
    view["maximum_pods"] = 20
    with pytest.raises(ValueError):
        h.selected(view)


def test_sharded_ceiling_extension_changes_only_ceiling_and_coordinator_hash():
    bundle, coord = parent()
    bundle, coord = reads(bundle, coord)
    before = copy.deepcopy(bundle)
    changed, current_coord = h.qualify_workload(bundle, coord)
    name = "scripts/paid_ticket_sharded_generator.py"
    assert bundle == before and len(changed) == 80
    assert {k for k in bundle if changed[k] != bundle[k]} == {name}
    assert changed[name].replace(b"args.rate <= 102", b"args.rate <= 100") == bundle[name]
    assert current_coord.replace(h.sha(changed[name]).encode(), h.sha(bundle[name]).encode()) == coord
    assert changed["scripts/customer_recovery_client.py"] == bundle["scripts/customer_recovery_client.py"]
    with pytest.raises(ValueError):
        h.qualify_workload(changed, current_coord)


def test_issuance_target_uses_owned_cohort_and_actual_window():
    events = [str(uuid4()) for _ in range(102)]
    program = h.issuance_program(events, 1000.0)
    assert "30000" in program and "30600" in program
    assert "50000" not in program and "302400" not in program
    for invalid in (events[:-1], [events[0]] * 102):
        with pytest.raises(ValueError):
            h.issuance_program(invalid, 1000.0)


@pytest.fixture
def reference(monkeypatch):
    report = {"pass": True, "restoration_complete": True, "integrity_verified": True,
        "transport_credentials_cleared": True, "native": {"measurement_gates": {"financial": True}},
        "paid_stage": {"customer": {"scheduled": 25200, "dispatched": 25200,
            "fulfilled": 25200, "generator_drops": 0},
            "customer_outcome_gate": {"pass": True},
            "financial": {"pass": True, "hold_deadlines_elapsed": True},
            "global_queues": {"pass": True}, "short_issuance_window": {"unique_paid_issued_tickets": 25099}}}
    binding = {"runtime": "same"}
    report["binding_sha256"] = p.digest(binding)
    result = p.digest(report)
    monkeypatch.setattr(h, "BASELINE_SHA256", result)
    state = {"binding": binding, "active_run": None, "paid_runs_started": 1,
             "cce_paid_result_sha256": result}
    entry = {"ledger": h.BASELINE_LEDGER, "status": "PASSED_RESTORED", "result_sha256": result}
    monkeypatch.setattr(p, "read", lambda path: {h.BASELINE_LEDGER: state} if path == p.STATE else report)
    monkeypatch.setattr(p, "envelope", dict)
    monkeypatch.setattr(p, "journal", lambda e: {"experiments": [entry]})
    return report, entry, state


def test_exact_passed_prerequisite_is_required(reference):
    assert h.require_short(goal())[1] is reference[0]


@pytest.mark.parametrize("kind", ["tamper", "failed", "active", "state_hash"])
def test_prerequisite_failures_block_headroom(reference, kind):
    report, entry, state = reference
    if kind == "tamper":
        report["paid_stage"]["customer"]["generator_drops"] = 1
    elif kind == "failed":
        entry["status"] = "FAILED_RESTORED"
    elif kind == "active":
        state["active_run"] = "busy"
    else:
        state["cce_paid_result_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        h.require_short(goal())


def test_slot_correction_requires_exact_failed_case_and_keeps_total_budget():
    correction = {**goal(), "payment_slot_arm": "control",
       "headroom_slot_correction_decision": "ADR0314",
       "headroom_slot_failure_sha256": h.FAILURE_SHA256,
       "baseline_sha256": h.FAILURE_SHA256}
    assert h.selected(correction) and h.correction_selected(correction)
    assert h.contract()["per_api_total"] == 4
    for field, value in (("payment_slot_arm", "candidate"),
                         ("headroom_slot_failure_sha256", "0" * 64),
                         ("headroom_slot_correction_decision", "ADR9999")):
        with pytest.raises(ValueError):
            h.selected({**correction, field: value})


def test_writer_scale_is_native_only_and_keeps_original_specs():
    from cce_worker_identity import specification
    original = specification("ADR0298")
    scaled = specification("ADR0315")
    assert original["counts"]["reservation-writer"] == 3
    assert scaled["counts"]["reservation-writer"] == 4
    assert sum(original["counts"].values()) == 14
    assert sum(scaled["counts"].values()) == 15
    assert scaled["manifest"] == original["manifest"]
    assert scaled["resources"] == original["resources"]
    assert specification("ADR0298") == original
    correction = {**goal(), "payment_slot_arm": "control", "maximum_pods": 19,
        "headroom_slot_correction_decision": "ADR0314",
        "headroom_slot_failure_sha256": h.FAILURE_SHA256,
        "reservation_writer_scale_decision": "ADR0315",
        "reservation_writer_failure_sha256": h.SLOT_SHA256,
        "baseline_sha256": h.SLOT_SHA256}
    assert h.writer_scale_selected(correction)
    assert h.selected(h.runtime_view(correction))
    for key, value in (("maximum_pods", 20), ("reservation_writer_failure_sha256", "0" * 64)):
        with pytest.raises(ValueError):
            h.selected({**correction, key: value})


@pytest.mark.parametrize("removed", [True, False])
def test_scaled_namespace_must_be_absent_before_restoring_only14original_workers(monkeypatch, removed):
    from cce_ecs_transition import Transition
    from cce_worker_transition import WorkerTransition
    sequence = []
    restored_programs = []
    worker = WorkerTransition.__new__(WorkerTransition)
    worker.decision, worker.normal_decision = "ADR0315", "ADR0298"
    worker.worker_stop_attempted = True
    worker.worker_runtime = SimpleNamespace(attempted=True)
    worker.deployment = SimpleNamespace(cleanup=lambda: sequence.append("remove-native15") or {"namespace_removed": removed})
    def call(host, program, seconds):
        restored_programs.append(program)
        sequence.append("restore-normal14")
        return {"owned_worker_transition_verified": True, "running": True}
    worker.session = SimpleNamespace(begin_cleanup=lambda: None, call=call)
    worker.record = {}
    worker.checkpoint = lambda: None
    worker.worker_rows = [{"Id": format(i, "064x"), "Image": "same", "Config": {},
                          "HostConfig": {}, "Mounts": []} for i in range(1, 15)]
    monkeypatch.setattr(Transition, "restore", lambda s: sequence.append("restore-api") or True)
    if removed:
        assert worker.restore()
        assert sequence == ["remove-native15", "restore-normal14", "restore-api"]
        assert "size=14" in restored_programs[0]
    else:
        with pytest.raises(ValueError, match="overlapping"):
            worker.restore()
        assert sequence == ["remove-native15"] and restored_programs == []


@pytest.mark.parametrize("scaled,index,accepted", [(False, 2, True), (False, 3, False),
                                                 (True, 3, True), (True, 4, False)])
def test_transport_allows_only_declared_writer_indices(monkeypatch, scaled, index, accepted):
    import cce_api_adapter as api
    import cce_nginx_ingress as ingress
    import cce_transaction_profile as transaction
    monkeypatch.setattr(transaction, "active", lambda: {"extension_decision": "ADR0271"})
    monkeypatch.setattr(ingress, "allowed_request", lambda *a: False)
    monkeypatch.setattr(h, "writer_scale_selected", lambda g: scaled)
    transport = api.KubernetesTransport.__new__(api.KubernetesTransport)
    transport.material = {"test": "fake"}
    transport.session = SimpleNamespace(cleanup_mode=False,
        call=lambda *a, **kw: {"value": {"bounded": True}})
    path = "/api/v1/namespaces/flash-cce-000000000000/pods/reservation-writer-" + str(index)
    if accepted:
        assert transport.request("GET", path) == {"bounded": True}
    else:
        with pytest.raises(ValueError, match="Bounded owned"):
            transport.request("GET", path)


def test_inventory_renewal_uses_declared15native_and14original_workers(monkeypatch):
    from datetime import UTC, datetime, timedelta

    import cce_inventory_renewal as renewal
    import cce_worker_identity as identity
    monkeypatch.setattr(identity, "validate_bundle", lambda b: None)
    now = datetime.now(UTC)
    original = {"captured_at": (now - timedelta(seconds=100)).isoformat()}
    api = {"run": "same", "receipts": [{}] * 4}
    workers = {"run": "same", "decision": "ADR0315", "receipts": [{}] * 15}
    binding = {"cce_inventory_renewal_contract": renewal.contract(),
               "cce_writer_scale_contract": h.writer_scale_contract()}
    updated, evidence = renewal.renew(original, run="same", binding=binding,
       native_before=api, workers_before=workers, observe_native=lambda: api,
       observe_workers=lambda: workers, verify_background=lambda: None, now=now)
    assert updated["captured_at"] == now.isoformat()
    assert evidence["native_background_workers"] == 15
    assert evidence["normal_ecs_background_workers"] == 14
    workers["receipts"].pop()
    with pytest.raises(ValueError, match="declared worker"):
        renewal.renew(original, run="same", binding=binding,
           native_before=api, workers_before=workers, observe_native=lambda: api,
           observe_workers=lambda: workers, verify_background=lambda: None, now=now)
