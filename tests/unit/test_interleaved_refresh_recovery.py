"""The second exact failure cannot weaken or rewrite either failed experiment."""
import copy
from dataclasses import FrozenInstanceError

import pytest
from test_dispatched_cohort_recovery import cohort, recovery, write  # noqa: F401

KEY = "bounded_interleaved_refresh_probe__380556ea4230"


@pytest.fixture(params=[KEY, "bounded_orders_event_index_probe__c9a9ead76625"])
def timing_cohort(cohort, request):  # noqa: F811 - imported pytest fixture
    root, old_entry, old_state, old_report, path, old_fresh = cohort
    case = recovery.CASES[request.param]
    entry = {**copy.deepcopy(old_entry), "ledger": case.key, "profile": case.profile,
             "reports": [{"pass": True}, {"run": case.run, "pass": False}]}
    state = {case.key: copy.deepcopy(old_state[recovery.KEY])}
    report = {**copy.deepcopy(old_report), "run": case.run, "experiment_decision": case.decision}

    def change(value):
        if isinstance(value, dict):
            return {k: change(v) for k, v in value.items()}
        if isinstance(value, list):
            return [change(v) for v in value]
        if type(value) is int:
            return {24331: case.expected, 869: case.drops}.get(value, value)
        return value

    import json
    old = root / "tmp" / recovery.RUN / recovery.ARM
    owned = root / "tmp" / case.run
    arm = owned / case.arm
    write(owned / "comparison-summary.json", report)
    for name in ("state.json", "candidate/stage.private.json", "candidate/fixture-identity.json"):
        write(arm / name, change(json.loads((old / name).read_text())))
    fresh = change(copy.deepcopy(old_fresh))
    fresh.update(ledger=case.key, original_result_sha256=recovery.digest(report))
    if case.decision == "ADR0224":
        saved = json.loads((arm / "state.json").read_text())
        index = {"verification_only": True, "pass": True, "index": {"oid": 53133, "valid": True,
                 "ready": True, "unique": False, "method": "btree", "key": "event_id",
                 "predicate": None, "columns": 1, "expected_table": True, "pass": True}}
        saved.update(orders_event_index_before=index, orders_event_index_after=index)
        write(arm / "state.json", saved)
        fresh["orders_event_index"] = index
    write(path, fresh)
    return root, entry, state, report, path, fresh


def test_second_case_preserves_failure_and_exact_dispatched_volume(timing_cohort):
    root, entry, state, report, path, _fresh = timing_cohort
    before = copy.deepcopy((entry, report))
    receipt = recovery.verify(root, entry, state, path)
    assert (entry, report) == before
    assert receipt["dispatched_paid_tickets"] == recovery.CASES[entry["ledger"]].expected
    assert receipt["undispatched"] == recovery.CASES[entry["ledger"]].drops
    assert not receipt["original_experiment_pass"] and not receipt["capacity_qualified"]
    assert all(receipt["gates"].values())


@pytest.mark.parametrize("drift", ["unknown", "profile", "decision", "volume", "relationship", "queue"])
def test_second_case_drift_is_rejected(timing_cohort, drift):
    root, entry, state, report, path, fresh = timing_cohort
    if drift == "unknown": entry["ledger"] = "bounded_interleaved_refresh_probe__000000000000"
    elif drift == "profile": entry["profile"] = "shared_callback_rate_probe"
    elif drift == "decision":
        report["experiment_decision"] = "ADR0219"
        write(root / "tmp" / recovery.CASES[entry["ledger"]].run / "comparison-summary.json", report)
    elif drift == "volume": fresh["fixture_financial_audit"]["counts"]["tickets"] -= 1
    elif drift == "relationship": fresh["fixture_financial_audit"]["relationships"]["inventory_relationship_errors"] = 1
    else: fresh["queue_counts"]["pending_callback_deliveries"] = 1
    write(path, fresh)
    with pytest.raises(ValueError): recovery.verify(root, entry, state, path)


def test_case_registry_is_immutable():
    with pytest.raises(TypeError): recovery.CASES["unknown"] = recovery.CASES[KEY]
    with pytest.raises(FrozenInstanceError): recovery.CASES[KEY].expected = 1


def test_index_recovery_requires_fresh_index_identity(timing_cohort):
    root, entry, state, _report, path, fresh = timing_cohort
    if recovery.CASES[entry["ledger"]].decision != "ADR0224":
        return
    fresh["orders_event_index"]["index"]["oid"] += 1
    write(path, fresh)
    with pytest.raises(ValueError, match="Fresh identical persistent index"):
        recovery.verify(root, entry, state, path)
