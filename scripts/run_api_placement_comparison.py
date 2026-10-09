"""ADR0174 matched placement pair; standing envelope owns cloud authorization."""
import api_placement_contract as policy
from run_async_confirmation_comparison import create_runner as shared_runner

LEDGER = "bounded_api_placement_rebalance"
AUTHORIZATION = "adr0174-api-placement-pair-2026-10-06"


def create_runner():
    return shared_runner(policy_module=policy, ledger=LEDGER, authorization=AUTHORIZATION,
                         decision="ADR0174", profile_name="api_placement",
                         artifact_directory="partial-timeout-reclamation", patch_name="adr0163.patch",
                         arms=("control", "candidate"), runner_filename="run_api_placement_comparison.py")


if __name__ == "__main__":
    create_runner().main()
