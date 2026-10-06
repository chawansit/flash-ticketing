"""ADR0181 separate registered placement profile with protected observer context."""
from types import MappingProxyType

import diagnostic_placement_contract as policy
from diagnostic_runner_connection import ProtectedContext, validate_target
from run_async_confirmation_comparison import create_runner as shared_runner
from status_refresh_contract import digest

LEDGER = "bounded_diagnostic_placement"
AUTHORIZATION = "adr0181-diagnostic-placement-pair-2026-10-06"


def create_runner():
    engine = shared_runner(policy_module=policy, ledger=LEDGER, authorization=AUTHORIZATION,
                           decision="ADR0181", profile_name="diagnostic_placement",
                           artifact_directory="partial-timeout-reclamation", patch_name="adr0163.patch",
                           arms=("control", "candidate"), runner_filename="run_diagnostic_placement_comparison.py")
    original_binding = engine.binding_for
    original_contract = engine.StatusRefreshContract
    engine.diagnostic_context = None
    engine.diagnostic_target = None

    class BoundContract(original_contract):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.diagnostic_context = engine.diagnostic_context

    def configure_target(target):
        value = validate_target(target)
        if engine.diagnostic_target is not None and value != engine.diagnostic_target:
            raise ValueError("Diagnostic target cannot change within an engine")
        engine.diagnostic_target = MappingProxyType(value)

    def configure(target, password):
        configure_target(target)
        engine.diagnostic_context = ProtectedContext(target, password)

    def binding_for(*args, **kwargs):
        if engine.diagnostic_target is None:
            raise ValueError("Protected diagnostic context required before reservation")
        return {**original_binding(*args, **kwargs),
                "diagnostic_target_sha256": digest(dict(engine.diagnostic_target))}

    def clear():
        if engine.diagnostic_context is not None:
            engine.diagnostic_context.clear()

    engine.configure_diagnostic, engine.clear_diagnostic = configure, clear
    engine.configure_diagnostic_target = configure_target
    engine.StatusRefreshContract, engine.binding_for = BoundContract, binding_for
    return engine
