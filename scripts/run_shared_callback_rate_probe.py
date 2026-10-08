"""ADR0219: one fresh protected 84/s stage, measured shared callback 1+3 configuration."""
import shared_callback_rate_probe_contract as policy
from run_callback_routing_comparison import create_runner as routing_runner

LEDGER = "bounded_shared_callback_rate_probe"
AUTHORIZATION = "adr0219-shared-callback-84-rate-probe-2026-10-08"


def create_runner():
    return routing_runner(policy_module=policy, ledger=LEDGER, authorization=AUTHORIZATION,
                          decision="ADR0219", profile_name="shared_callback_rate_probe",
                          runner_filename="run_shared_callback_rate_probe.py", arms=("candidate",),
                          extra_identity=("run_callback_routing_comparison.py", "callback_routing_contract.py",
                                          "shared_callback_placement_contract.py"))
