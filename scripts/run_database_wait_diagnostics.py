"""ADR0173 bounded single control; local preparation does not access cloud."""
import database_wait_contract as policy
from run_async_confirmation_comparison import create_runner as shared_runner
from run_async_confirmation_comparison import create_stager as shared_stager

LEDGER = "bounded_database_wait_diagnostics"
AUTHORIZATION = "adr0173-database-wait-control-2026-10-06"
OPTIONS = {"policy_module":policy,"ledger":LEDGER,"authorization":AUTHORIZATION,"decision":"ADR0173",
           "profile_name":"database_wait","artifact_directory":"partial-timeout-reclamation",
           "patch_name":"adr0163.patch","arms":("control",),"runner_filename":"run_database_wait_diagnostics.py"}


def create_runner():
    return shared_runner(**OPTIONS)


def create_stager():
    return shared_stager(**OPTIONS)


if __name__ == "__main__":create_runner().main()
