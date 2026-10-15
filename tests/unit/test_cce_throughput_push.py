"""ADR0327 fixed-budget rate escalation; offline only, no network or dispatch."""
import copy
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import cce_fulfillment_batch_profile as fulfillment
import cce_recovery_headroom as h
import cce_throughput_push as push
import work_envelope as policy
from cce_paid_profiles import RATE_PUSH, for_guard
from test_cce_fulfillment_batch import goal as original_goal
from test_cce_recovery_headroom import parent, reads


def goal():
    value = original_goal()
    value.update(throughput_push_decision="ADR0327", throughput_push_contract_sha256=policy.digest(push.contract()),
                 offered_journeys_per_second=120)
    value["headroom_contract_sha256"] = policy.digest(h.contract(value))
    return value


def test_original_profile_and_exact_new_rate_coexist():
    assert push.bounds(original_goal()) == (102, 102, 30600, 30000)
    actual = goal()
    assert h.selected(actual) and h.selected(h.runtime_view(actual))
    assert push.bounds(actual) == (120, 120, 36000, 35400)
    assert fulfillment.selected(actual)


@pytest.mark.parametrize("field,value", [("offered_journeys_per_second", 122),
    ("offered_seconds", 600), ("generator_concurrency", 1000), ("maximum_pods", 20),
    ("acquisition_budget", 24), ("maximum_paid_stages", 2),
    ("throughput_push_contract_sha256", "0" * 64), ("throughput_push_decision", "ADR0326")])
def test_expanded_or_partial_scope_cannot_select(field, value):
    with pytest.raises(ValueError):
        h.selected({**goal(), field: value})
    unmarked = goal()
    unmarked.pop("throughput_push_decision")
    with pytest.raises(ValueError):
        h.selected(unmarked)


def test_real_reference_is_the_completed_upgraded_rds_run():
    path, report = push.require_reference()
    assert path == push.REFERENCE_PATH and policy.digest(report) == push.REFERENCE_SHA256


def test_guard_requires_both_rate_and_source_bound_contracts(monkeypatch):
    actual = goal()
    monkeypatch.setattr(policy, "envelope", lambda: {"spending": {"temporary_cce_pilot_exception":
                          {"goal_bounded_authorization": actual}}})
    binding = {"cce_headroom_contract": h.contract(actual), "cce_throughput_push_contract": push.contract()}
    guard = SimpleNamespace(key="bounded_cce_ticket_target__0123456789ab", binding=binding)
    assert for_guard(guard) == RATE_PUSH
    assert RATE_PUSH.expected == 36000 and RATE_PUSH.shows == 120
    binding.pop("cce_throughput_push_contract")
    with pytest.raises(ValueError):
        for_guard(guard)


def test_sealed_customer_behavior_is_byte_identical_except_rate_ceiling():
    bundle, coordinator = parent()
    bundle, coordinator = reads(bundle, coordinator)
    before = copy.deepcopy(bundle)
    after, current = h.qualify_workload(bundle, coordinator, goal())
    name = "scripts/paid_ticket_sharded_generator.py"
    assert after[name].replace(b"args.rate <= 120", b"args.rate <= 100") == before[name]
    assert current.replace(h.sha(after[name]).encode(), h.sha(before[name]).encode()) == coordinator
    assert {key for key in before if after[key] != before[key]} == {name}


def test_financial_window_requires_exact120_show_identity():
    events = [str(uuid4()) for _ in range(120)]
    source = h.issuance_program(events, 1000.0, goal())
    assert "35400" in source and "36000" in source
    assert "30600" not in source and "302400" not in source
    for invalid in (events[:-1], [events[0]] * 120):
        with pytest.raises(ValueError):
            h.issuance_program(invalid, 1000.0, goal())


def test_same_baseline_settings_and_source_closure_are_enforced():
    actual = goal()
    original = policy.read(policy.STATE)[push.REFERENCE_LEDGER]["binding"]
    binding = copy.deepcopy(original)
    binding.update(cce_throughput_push_contract=push.contract(), baseline_sha256=push.REFERENCE_SHA256)
    for name in ("cce_paid_entry_sources", "cce_paid_core_sources"):
        binding[name]["scripts/cce_throughput_push.py"] = "new-source-hash"
    assert push.validate_binding(actual, binding)
    binding["cce_acquisition_budget"] += 1
    with pytest.raises(ValueError):
        push.validate_binding(actual, binding)


@pytest.mark.parametrize("rate,decision,expected,target", [(120,"ADR0327",36000,35400),(140,"ADR0328",42000,41400)])
def test_complete_exact_rate_profile(rate, decision, expected, target, monkeypatch):
    from cce_bounded_financial_audit import program as audit_program
    from cce_paid_inputs import fixture_program
    from cce_paid_profiles import HIGHER_RATE_PUSH
    value = original_goal()
    value.update(throughput_push_decision=decision, offered_journeys_per_second=rate)
    value["throughput_push_contract_sha256"] = policy.digest(push.contract(value))
    value["headroom_contract_sha256"] = policy.digest(h.contract(value))
    assert h.selected(value) and fulfillment.selected(value)
    assert push.bounds(value) == (rate, rate, expected, target)
    path, report = push.require_reference(value)
    assert path == push.reference(value)[0] and policy.digest(report) == push.reference(value)[2]
    envelope = policy.envelope()
    envelope["spending"]["temporary_cce_pilot_exception"]["goal_bounded_authorization"] = value
    monkeypatch.setattr(policy, "envelope", lambda: envelope)
    binding = {"cce_headroom_contract": h.contract(value), "cce_throughput_push_contract": push.contract(value)}
    profile = HIGHER_RATE_PUSH if rate == 140 else RATE_PUSH
    assert for_guard(SimpleNamespace(key="bounded_cce_ticket_target__0123456789ab", binding=binding)) == profile
    events = [str(uuid4()) for _ in range(rate)]
    compile(audit_program(events, expected), "terminal-audit", "exec")
    compile(fixture_program("/tmp/adr0328-fixture", rate, "0" * 64), "fixture", "exec")
    source = h.issuance_program(events, 1000.0, value)
    assert str(expected) in source and str(target) in source
    bundle, coordinator = parent()
    bundle, coordinator = reads(bundle, coordinator)
    after, _ = h.qualify_workload(bundle, coordinator, value)
    assert f"args.rate <= {rate}".encode() in after["scripts/paid_ticket_sharded_generator.py"]
    original = policy.read(policy.STATE)[push.reference(value)[1]]["binding"]
    candidate = copy.deepcopy(original)
    candidate.update(cce_throughput_push_contract=push.contract(value), baseline_sha256=push.reference(value)[2])
    for name in ("cce_paid_entry_sources", "cce_paid_core_sources"):
        candidate[name]["scripts/cce_throughput_push.py"] = "new-source-hash"
    assert push.validate_binding(value, candidate)
    candidate["cce_acquisition_budget"] += 1
    with pytest.raises(ValueError):
        push.validate_binding(value, candidate)


def test_140_requires_its_own_contract_and_cannot_expand_rate():
    value = goal()
    value.update(throughput_push_decision="ADR0328", offered_journeys_per_second=140)
    with pytest.raises(ValueError):
        h.selected(value)
    value["throughput_push_contract_sha256"] = policy.digest(push.contract(value))
    value["headroom_contract_sha256"] = policy.digest(h.contract(value))
    assert h.selected(value)
    with pytest.raises(ValueError):
        h.selected({**value,"offered_journeys_per_second":150})
