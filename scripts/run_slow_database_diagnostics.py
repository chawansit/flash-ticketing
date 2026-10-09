"""ADR0171 bounded single control; local preparation does not access cloud."""
import slow_database_contract as policy
from run_async_confirmation_comparison import create_runner as shared_runner
from run_async_confirmation_comparison import create_stager as shared_stager

LEDGER = "bounded_slow_database_diagnostics"
AUTHORIZATION = "adr0171-slow-database-control-2026-10-06"
OPTIONS = {"policy_module":policy,"ledger":LEDGER,"authorization":AUTHORIZATION,"decision":"ADR0171",
           "profile_name":"slow_database","artifact_directory":"partial-timeout-reclamation",
           "patch_name":"adr0163.patch","arms":("control",),"runner_filename":"run_slow_database_diagnostics.py"}


def create_runner():
    return shared_runner(**OPTIONS)


def create_stager():
    return shared_stager(**OPTIONS)


if __name__ == "__main__":create_runner().main()
