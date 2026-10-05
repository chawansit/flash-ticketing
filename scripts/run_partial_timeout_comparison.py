"""ADR0164 default-local runner; fresh bounded scope required for cloud stages."""

import partial_timeout_contract as policy
from run_async_confirmation_comparison import create_runner as shared_runner
from run_async_confirmation_comparison import create_stager as shared_stager

LEDGER = "bounded_partial_timeout_reclamation"
AUTHORIZATION = "adr0164-partial-timeout-pair-2026-10-06"

OPTIONS = {"policy_module": policy, "ledger": LEDGER,
           "authorization": AUTHORIZATION, "decision": "ADR0164",
           "profile_name": "partial_timeout", "artifact_directory": "partial-timeout-reclamation",
           "patch_name": "adr0163.patch"}


def create_runner():
    return shared_runner(**OPTIONS)


def create_stager():
    return shared_stager(**OPTIONS)


if __name__ == "__main__":
    create_runner().main()
