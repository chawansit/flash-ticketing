"""ADR0224 one additive index factor through the existing protected paid runner."""
import orders_event_index_probe_contract as policy
from run_interleaved_refresh_probe import create_runner as parent_runner

LEDGER = "bounded_orders_event_index_probe"
AUTHORIZATION = "adr0224-orders-event-index-84-probe-2026-10-08"


def create_runner():
    engine = parent_runner(policy_module=policy, ledger=LEDGER, authorization=AUTHORIZATION,
        decision="ADR0224", profile_name="orders_event_index_probe",
        runner_filename="run_orders_event_index_probe.py",
        extra_identity=("run_interleaved_refresh_probe.py", "interleaved_refresh_probe_contract.py",
                        "apply_orders_event_index.py"))
    original_identity, original_gates = engine.identity, engine.stage_gates

    def identity():
        return {**original_identity(), policy.MIGRATION: engine.comparison.source_sha256(
            (policy.ROOT / policy.MIGRATION).read_bytes())}

    def stage_gates(record, inventory, restored, contract):
        result = original_gates(record, inventory, restored, contract)
        proof = contract.index_verified()
        result["database_index"] = {"orders_event_index_verified": proof,
                                    "persistent_schema_correction": True}
        if not proof:
            result["failed_gates"].append("orders_event_index_verified")
            result["all_required_gates_pass"] = False
        return result

    engine.identity, engine.stage_gates = identity, stage_gates
    return engine
