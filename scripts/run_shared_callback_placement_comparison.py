"""ADR0217: proven protected runner, shared callbacks, placement-only factor."""
import shared_callback_placement_contract as policy
from run_callback_routing_comparison import create_runner as routing_runner

LEDGER = "bounded_shared_callback_placement"
AUTHORIZATION = "adr0217-shared-callback-placement-pair-2026-10-08"


def create_runner():
    return routing_runner(policy_module=policy, ledger=LEDGER, authorization=AUTHORIZATION,
                          decision="ADR0217", profile_name="shared_callback_placement",
                          runner_filename="run_shared_callback_placement_comparison.py",
                          extra_identity=("run_callback_routing_comparison.py", "callback_routing_contract.py"))
