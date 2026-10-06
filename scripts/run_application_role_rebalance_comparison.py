"""ADR0177 separate scoped placement pair; default local, fresh envelope only."""
import application_role_rebalance_contract as policy
from run_async_confirmation_comparison import create_runner as shared_runner

LEDGER = "bounded_application_role_rebalance"
AUTHORIZATION = "adr0177-application-role-rebalance-2026-10-06"


def create_runner():
    return shared_runner(policy_module=policy, ledger=LEDGER, authorization=AUTHORIZATION,
                         decision="ADR0177", profile_name="application_role_rebalance",
                         artifact_directory="partial-timeout-reclamation", patch_name="adr0163.patch",
                         arms=("control", "candidate"), runner_filename="run_application_role_rebalance_comparison.py")


if __name__ == "__main__":
    create_runner().main()
