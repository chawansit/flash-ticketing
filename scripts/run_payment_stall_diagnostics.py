"""ADR0170 bounded single control; local preparation does not access cloud."""
import payment_stall_contract as policy
from run_async_confirmation_comparison import create_runner as shared_runner
from run_async_confirmation_comparison import create_stager as shared_stager

LEDGER = "bounded_payment_stall_diagnostics"
AUTHORIZATION = "adr0170-payment-stall-control-2026-10-06"
OPTIONS = {"policy_module":policy,"ledger":LEDGER,"authorization":AUTHORIZATION,"decision":"ADR0170",
           "profile_name":"payment_stall","artifact_directory":"partial-timeout-reclamation",
           "patch_name":"adr0163.patch","arms":("control",),"runner_filename":"run_payment_stall_diagnostics.py"}


def create_runner():
    return shared_runner(**OPTIONS)


def create_stager():
    return shared_stager(**OPTIONS)


if __name__ == "__main__":create_runner().main()
