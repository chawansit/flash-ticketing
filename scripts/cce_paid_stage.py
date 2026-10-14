"""ADR0228 reuse of the proven paid generator/jobs/financial audits on CCE.

The registered caller integrates topology, observers, safety and restoration.
Historical scopes remain consumed; repository integration authorizes no load.
"""

import hashlib
import json
import math
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import cce_api_adapter as cce
import work_envelope as policy
from cce_frozen_harness import qualified_helper
from cce_paid_profiles import for_guard
from fixture_identity_evidence import retain_fixture_identity
from prepare_generator_completion import ARTIFACTS, PATH, corrected_bundle
from qualify_two_host_deployment import GENERATOR_IDLE
from run_two_host_paid_comparison import CORE, Stages, drain, fetch, financial_audit_program, frozen_bundle


class OwnedJobs:
    # Reuse the exact proven identity-bound supervisor and cleanup implementation.
    launch = Stages.launch
    wait = Stages.wait
    stop = Stages.stop


def sha(raw):
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def identity():
    paths = (
        "scripts/cce_ingress_hourly_generator.py",
        "scripts/cce_bounded_financial_audit.py", "scripts/independent_database_sampling.py",
        "scripts/cce_runtime_cohort_profile.py", "scripts/paid_cohort_query.py",
        "scripts/cce_customer_outcome_slo.py", "scripts/cce_payment_slot_profile.py",
        "scripts/cce_committed_hourly_profile.py", "scripts/cce_terminal_audit_admission.py",
        "scripts/cce_inventory_renewal.py", "scripts/cce_recovery_headroom.py", "scripts/cce_throughput_push.py",
        "scripts/cce_writer_scale_verification.py",
        "scripts/cce_backend_partition.py", "scripts/cce_backend_partition_runtime.py",
        "scripts/cce_measured_payment_context_profile.py", "scripts/cce_measured_payment_context_identity.py",
        "docs/capacity/cce/measured-payment-context-image-2026-10-14.json",
        "scripts/cce_explicit_start_profile.py", "scripts/cce_explicit_start_identity.py",
        "scripts/cce_fulfillment_batch_profile.py", "scripts/cce_fulfillment_batch_identity.py",
        "docs/capacity/cce/fulfillment-transaction-batching-local-2026-10-14.json",
        "docs/capacity/cce/explicit-transaction-start-image-2026-10-14.json",
        "scripts/cce_notification_dedup_profile.py", "scripts/cce_notification_dedup_identity.py",
        "docs/capacity/cce/ticket-notification-dedup-image-2026-10-14.json",
        "scripts/cce_writer_sale_context_profile.py", "scripts/cce_writer_sale_context_identity.py",
        "docs/capacity/cce/writer-sale-context-image-2026-10-14.json",
        "scripts/cce_gateway_read_profile.py", "scripts/customer_gateway_read_bundle.py",
        "scripts/customer_gateway_read_recovery.py", "scripts/cce_customer_slo_reevaluation.py",
        "artifacts/customer-gateway-read-recovery/manifest.json",
        "scripts/cce_committed_status_identity.py", "scripts/cce_committed_status_profile.py",
        "scripts/cce_committed_status_contract.py",
        "docs/capacity/cce/committed-fulfillment-status-image-2026-10-13.json",
        "scripts/cce_verification_contract.py", "scripts/cce_metric_admission.py", "scripts/cce_payment_pipeline_identity.py",
        "scripts/cce_payment_pipeline_profile.py", "scripts/cce_payment_pipeline_contract.py",
        "docs/capacity/cce/payment-write-pipeline-image-2026-10-12.json",
        "scripts/collect_two_host_inventory.py", "scripts/runtime_source_identity.py",
        "scripts/cce_ticket_target_profile.py", "scripts/customer_timeout_bundle.py", "scripts/cce_native_database_bridge.py", "scripts/cce_status_refresh_reuse.py", "scripts/cce_status_reuse_hourly.py",
        "docs/capacity/cce/committed-status-reuse-result-2026-10-11.json",
        "artifacts/customer-timeout/manifest.json", "artifacts/customer-timeout/coordinator.py",
        "artifacts/customer-timeout/paid_ticket_load_generator.py",
        "artifacts/customer-timeout/paid_ticket_sharded_generator.py",
        "scripts/cce_live_admission.py", "scripts/cce_worker_rebalance_profile.py",
        "scripts/cce_event_lane_identity.py", "scripts/cce_event_lane_profile.py",
        "scripts/cce_event_lane_contract.py", "scripts/observe_projection_kafka.py",
        "docs/capacity/cce/event-lane-fetch-image-2026-10-10.json",
        "scripts/cce_shared_worker_comparison.py", "scripts/cce_shared_worker_profile.py",
        "scripts/cce_payment_dispatch_profile.py", "scripts/cce_shared_worker_image_identity.py",
        "scripts/cce_shared_worker_contract.py", "scripts/cce_shared_application.py",
        "scripts/cce_native_workers.py", "scripts/cce_worker_transition.py",
        "scripts/cce_worker_identity.py", "scripts/observe_cce_workers.py",
        "docs/capacity/cce/shared-application-image-2026-10-10.json",
        "scripts/cce_nginx_ingress.py",
        "scripts/cce_paid_stage.py",
        "scripts/cce_frozen_harness.py",
        "scripts/cce_historical_sources.py",
        "scripts/check_cce_reproducibility.py",
        "artifacts/cce-frozen-harness/manifest.json",
        "artifacts/cce-historical-sources/manifest.json",
        "docs/capacity/cce/reproduction-lock-2026-10-09.json",
        "scripts/cce_paid_profiles.py",
        "scripts/cce_transaction_profile.py",
        "scripts/cce_recovery_hourly_profile.py", "artifacts/hourly-customer-recovery/manifest.json",
        "scripts/cce_customer_recovery_profile.py", "scripts/customer_recovery_bundle.py",
        "artifacts/customer-recovery/manifest.json", "artifacts/customer-recovery/coordinator.py",
        "docs/capacity/cce/customer-recovery-image-2026-10-10.json",
        "scripts/cce_reproduction_overlay.py",
        "scripts/cce_slot_trace_transport.py",
        "scripts/prepare_slot_comparison_sources.py",
        "scripts/prepare_payment_context_sources.py",
        "scripts/cce_payment_context_images.py",
        "scripts/observe_slot_paid_pipeline.py", "scripts/cce_simulator_dispatch_profile.py", "scripts/prepare_simulator_dispatch_sources.py",
        "docs/capacity/cce/simulator-dispatch-image-2026-10-09.json",
        "docs/capacity/cce/payment-context-images-2026-10-09.json",
        "scripts/observe_slot_paid_pipeline.py",
        "scripts/slot_failure_evidence.py",
        "scripts/bounded_trace_transport.py",
        "artifacts/cce-transaction-runner/manifest.json",
        "docs/capacity/cce/slot-comparison-images-2026-10-09.json",
        "scripts/fixture_identity_evidence.py",
        "scripts/cce_paid_inputs.py",
        "scripts/cce_paid_resources.py",
        "scripts/cce_paid_lifecycle.py",
        "scripts/cce_paid_observers.py",
        "scripts/observe_cce_paid_pipeline.py",
        "scripts/observe_two_host_cpu.py",
        "scripts/observe_two_host_pipeline.py",
        "scripts/cce_paid_safety.py",
        "scripts/cce_api_adapter.py",
        "scripts/cce_dependency_probe.py",
        "scripts/cce_ecs_transition.py",
        "scripts/run_two_host_paid_comparison.py",
        "scripts/run_synchronized_paid_generator.py",
        "scripts/prepare_generator_completion.py",
        "docs/capacity/cce/api-adapter-contract-2026-10-08.json",
    )
    return {p: sha((policy.ROOT / p).read_bytes()) for p in paths}


def generator_bundle():
    bundle = corrected_bundle(frozen_bundle())
    raw = qualified_helper()
    return {**bundle, "scripts/checkout_journey_probe.py": raw}


def generator_arguments(directory, origin, start_at_epoch, *, profile=None, recovery_max_attempts=1):
    from cce_paid_profiles import HEADROOM, HOURLY, RATE_PUSH, SHORT, TICKET_TARGET
    profile = SHORT if profile is None else profile
    if recovery_max_attempts not in (1, 3) or (recovery_max_attempts == 3 and profile not in (SHORT, HOURLY, TICKET_TARGET, HEADROOM, RATE_PUSH)):
        raise ValueError("Recovery requires the explicit registered comparison or hourly binding")
    if profile not in (SHORT, HOURLY, TICKET_TARGET, HEADROOM, RATE_PUSH):
        raise ValueError("Exact CCE stage profile required")
    if not re.fullmatch(r"/[^\x00\r\n]*?/tmp/adr0151-[0-9a-f]{12}-cce-candidate", directory):
        raise ValueError("Canonical owned generator directory required")
    import cce_transaction_profile as transaction
    if origin != __import__("cce_nginx_ingress").origin(transaction.active()):
        raise ValueError("Existing private shared load-balancer origin required")
    if type(start_at_epoch) not in (int, float) or not math.isfinite(start_at_epoch):
        raise ValueError("Finite synchronized start required")
    ingress_hourly = profile == HOURLY and (transaction.active() or {}).get("ingress_hourly_decision") == "ADR0293"
    return [
        "/root/http-load-venv/bin/python",
        directory + ("/cce_ingress_hourly_generator.py" if ingress_hourly else
                     "/cce_hourly_paid_generator.py" if profile == HOURLY else "/run_synchronized_paid_generator.py"),
        *([] if profile == HOURLY else ["--frozen-generator", directory + "/paid_ticket_sharded_generator.py"]),
        "--start-at-epoch",
        str(start_at_epoch),
        "--manifest",
        directory + "/manifest.private.json",
        "--origin",
        origin,
        "--output",
        directory + "/customer.json",
        "--rate",
        str(profile.expected // profile.duration),
        "--seconds",
        str(profile.duration),
        "--completion-deadline-seconds",
        str(profile.completion_deadline),
        "--concurrency",
        "1000" if profile == TICKET_TARGET else "500",
        "--http-client-count",
        "8",
        "--poll-seconds",
        "1",
        "--duplicates",
        "1",
        "--lifecycle-diagnostics",
        *(["--recovery-max-attempts", "3"] if recovery_max_attempts == 3 else []),
    ]


class PaidStage:
    def __init__(self, session, guard, run, audit_container, output, *, bundle=None, jobs=None):
        if not re.fullmatch(r"adr0151-[0-9a-f]{12}", run) or not re.fullmatch(
            r"[0-9a-f]{64}", audit_container
        ):
            raise ValueError("Actual canonical experiment and Docker audit-container identity required")
        output = Path(output)
        if (
            output.name != "candidate"
            or not output.resolve().is_relative_to((policy.ROOT / "tmp" / run).resolve())
            or output.is_symlink()
            or not output.is_dir()
        ):
            raise ValueError("Fresh owned native stage directory required")
        self.session, self.guard, self.run, self.cid, self.output = (
            session,
            guard,
            run,
            audit_container,
            output,
        )
        self.profile = for_guard(guard)
        self.bundle = bundle if bundle is not None else generator_bundle()
        parent = policy.read(
            policy.ROOT
            / "docs/capacity/flash-sale-opening/frozen-baseline-cpu-diagnostic-plan-2026-10-05.json"
        )
        expected = parent["harness_source_sha256"]
        correction = policy.read(ARTIFACTS / "manifest.json")
        if (
            len(self.bundle) != 78
            or sha(self.bundle[PATH]) != correction["candidate_sha256"]
            or any(
                sha(self.bundle[p]) != expected[p]
                for p in expected
                if p not in {PATH, "scripts/checkout_journey_probe.py"}
            )
            or self.bundle["scripts/checkout_journey_probe.py"].replace(b"\r\n", b"\n")
            != qualified_helper()
        ):
            raise ValueError("Exact corrected 78-file workload bundle required")
        import cce_transaction_profile as transaction
        from customer_recovery_bundle import enabled, qualify
        self.recovery_enabled = enabled(transaction.active())
        bound_attempts = guard.binding.get("cce_recovery_max_attempts", 1)
        if (type(bound_attempts) is not int or bound_attempts not in {1, 3}
                or (not self.recovery_enabled and bound_attempts != 1)):
            raise ValueError("Recovery profile and binding mismatch")
        self.coordinator = (policy.ROOT / "scripts/run_synchronized_paid_generator.py").read_bytes()
        if self.recovery_enabled:
            if guard.binding.get("cce_recovery_max_attempts") != 3:
                raise ValueError("Explicit short recovery binding required")
            if transaction.active()["extension_decision"] == "ADR0277":
                from cce_ticket_target_profile import qualify_workload
                self.bundle, self.coordinator = qualify_workload(self.bundle)
            else:
                self.bundle, self.coordinator = qualify(self.bundle)
            from customer_timeout_bundle import MANIFEST_SHA256 as timeout_sha
            from customer_timeout_bundle import qualify as retain_timeout
            from customer_timeout_bundle import selected
            if selected(transaction.active()):
                from cce_paid_profiles import HEADROOM, RATE_PUSH
                if (self.profile.name not in {"cce_paid_comparison", "cce_hourly_qualification"} and self.profile not in (HEADROOM, RATE_PUSH)
                        or guard.binding.get("cce_timeout_retention_decision") != "ADR0278"
                        or guard.binding.get("cce_timeout_bundle_sha256") != timeout_sha):
                    raise ValueError("Exact timeout-retention binding required")
                self.bundle, self.coordinator = retain_timeout(self.bundle, self.coordinator)
            elif "cce_timeout_bundle_sha256" in guard.binding:
                raise ValueError("Unselected timeout-retention binding")
            from customer_gateway_read_bundle import contract as gateway_contract
            from customer_gateway_read_bundle import qualify as gateway_qualify
            from customer_gateway_read_bundle import selected as gateway_selected
            if gateway_selected(transaction.active()):
                if guard.binding.get("cce_gateway_read_recovery_contract") != gateway_contract():
                    raise ValueError("Exact sealed gateway-read workload binding required")
                self.bundle, self.coordinator = gateway_qualify(self.bundle, self.coordinator)
            elif "cce_gateway_read_recovery_contract" in guard.binding:
                raise ValueError("Unselected gateway-read workload binding")
            from cce_paid_profiles import HEADROOM, RATE_PUSH
            if self.profile in (HEADROOM, RATE_PUSH):
                from cce_recovery_headroom import contract as headroom_contract
                from cce_recovery_headroom import qualify_workload
                if guard.binding.get("cce_headroom_contract") != headroom_contract(transaction.active()):
                    raise ValueError("Exact headroom workload binding required")
                self.bundle, self.coordinator = qualify_workload(self.bundle, self.coordinator, transaction.active())
            elif "cce_headroom_contract" in guard.binding:
                raise ValueError("Unselected headroom workload binding")
            if self.profile.duration == 3600:
                from cce_recovery_hourly_profile import qualify_hourly_adapters
                qualify_hourly_adapters()
                from cce_status_reuse_hourly import adapters
                from cce_status_reuse_hourly import selected as hourly_selected
                if hourly_selected(transaction.active()):
                    if (guard.binding.get("cce_hourly_qualification_decision") != "ADR0281"
                            or guard.binding.get("cce_hourly_adapters_sha256") != policy.digest(adapters())):
                        raise ValueError("Exact hourly timeout-retention adapters required")
                elif selected(transaction.active()) or guard.binding.get("cce_recovery_hourly_decision") != "ADR0256":
                    raise ValueError("Explicit hourly recovery binding required")
        self.jobs = jobs or OwnedJobs()
        self.job_list = []
        self.record = {
            "arm": "candidate",
            "customers_dispatched": False,
            "pass": False,
            "capacity_stages_started": 0,
            "pinned_harness_files": len(self.bundle),
            "customer_recovery_max_attempts": 3 if self.recovery_enabled else 1,
            "gateway_read_recovery_contract": (gateway_contract() if self.recovery_enabled
                and gateway_selected(transaction.active()) else None),
        }
        self.gen = session.config["generator"]["repo"] + "/tmp/" + run + "-cce-candidate"
        from cce_nginx_ingress import origin, validate_binding
        from cce_nginx_ingress import selected as ingress_selected
        goal = transaction.active()
        if ingress_selected(goal):
            validate_binding(goal, guard.binding)
        self.origin = origin(goal)
        generator_arguments(self.gen, self.origin, 0, profile=self.profile, recovery_max_attempts=3 if self.recovery_enabled else 1)
        from cce_customer_outcome_slo import contract as outcome_contract
        from cce_customer_outcome_slo import selected as outcome_selected
        self.outcome_enabled = outcome_selected(goal)
        if self.outcome_enabled and guard.binding.get("cce_customer_outcome_contract") != outcome_contract():
            raise ValueError("Exact customer outcome binding required before dispatch")
        self.events = []
        self.prepared = False

    def check(self, timeout=0):
        if (
            self.guard is None
            or self.session.action_guard is not self.guard
            or for_guard(self.guard).name not in policy.PROFILES
            or for_guard(self.guard).name not in policy.PROFILES
            or for_guard(self.guard).name not in policy.envelope()["qualified_profiles"]
            or self.guard.binding.get("cce_paid_core_sources") != identity()
            or self.guard.binding.get("configuration_sha256") != policy.digest(self.session.config)
        ):
            raise ValueError("Separately registered and exactly bound CCE paid profile required")
        cce.dependency.authorized_today(policy.envelope())
        self.guard.check(timeout)

    def checkpoint(self):
        policy.write(self.output / "stage.private.json", self.record)
        self.session.checkpoint()

    def prepare(self, fixture, manifest):
        self.check(60)
        if self.prepared:
            raise ValueError("A stage cannot reuse a prepared fixture")
        self.record["fixture_creation_attempted"] = True
        retain_fixture_identity(
            self.output,
            self.record,
            fixture,
            self.profile.shows,
            producer_sha256=sha(self.bundle["scripts/prepare_capacity_fixture.py"]),
            persist_stage=False,
        )
        self.checkpoint()
        self.events = list(fixture["show_ids"])
        from cce_paid_profiles import HEADROOM, RATE_PUSH
        if self.profile in (HEADROOM, RATE_PUSH):
            from cce_bounded_financial_audit import program
            program(self.events, self.profile.expected)
            self.record["financial_audit_program_preflight_pass"] = True
        if self.profile.duration == 3600 and (datetime.fromisoformat(fixture["sale_ends"]) - datetime.now(UTC)).total_seconds() < 4200:
            raise ValueError("Hourly sale window must cover offered traffic and mandatory audit headroom")
        expiry = datetime.fromisoformat(manifest["expires_at"])
        if (
            manifest.get("schema_version") != 1
            or manifest.get("environment") != "development"
            or manifest.get("origin") != self.origin
            or manifest.get("fixture_layout") != "distributed"
            or manifest.get("show_ids") != self.events
            or manifest.get("seat_offset") != 0
            or manifest.get("seats_per_show") != 300
            or len(manifest.get("viewer_tokens", [])) != self.profile.expected
            or len(set(manifest["viewer_tokens"])) != self.profile.expected
            or expiry.tzinfo is None
            or (expiry - datetime.now(UTC)).total_seconds() < self.profile.duration + 600
        ):
            raise ValueError("Fresh complete private paid manifest required")
        UUID(manifest["id"])
        self.manifest = manifest
        expected_helpers = {
            name: sha(self.bundle["scripts/" + name])
            for name in (
                "prepare_capacity_fixture.py",
                "audit_checkout_smoke.py",
                "capacity_queue_state.py",
                "kafka_lag_observe.py",
            )
        }
        proof = self.session.api(
            self.cid,
            "import hashlib,json;from pathlib import Path;expected="
            + repr(expected_helpers)
            + ";assert {k:hashlib.sha256((Path('/app/scripts')/k).read_bytes().replace(b'\\r\\n',b'\\n')).hexdigest() for k in expected}==expected;print(json.dumps({'frozen_helpers_verified':True}))",
            45,
        )
        if proof.get("frozen_helpers_verified") is not True:
            raise ValueError("Frozen native audit helper proof missing")
        self.record.update(proof)
        self.record["generator_directory_creation_attempted"] = True
        self.checkpoint()
        owner = {"run": self.run, "binding_sha256": policy.digest(self.guard.binding)}
        self.session.call(
            "generator",
            "import json,os;from pathlib import Path;p=Path("
            + repr(self.gen)
            + ");p.mkdir(mode=0o700);q=p/'.cce-stage-owner.json';q.write_text(json.dumps("
            + repr(owner)
            + "));os.chmod(q,0o600);print(json.dumps({'fresh':True}))",
            45,
        )
        self.record["generator_directory_created"] = True
        self.checkpoint()
        self.session.put("generator", self.gen + "/manifest.private.json", json.dumps(manifest), True)
        self.record["private_manifest_uploaded"] = True
        transferred = {}
        for name in (*CORE, *(("customer_recovery_client.py", "paid_fixture_layout.py") if self.recovery_enabled else ()), *(("cce_hourly_paid_generator.py", "cce_hourly_paid_leaf.py", "cce_ingress_hourly_generator.py") if self.profile.duration == 3600 else ())):
            raw = self.bundle["scripts/" + name] if name in CORE or name in {"customer_recovery_client.py", "paid_fixture_layout.py"} else (policy.ROOT / "scripts" / name).read_bytes()
            self.session.put("generator", self.gen + "/" + name, raw.decode(), True)
            transferred[name] = sha(raw)
        raw = self.coordinator
        self.session.put("generator", self.gen + "/run_synchronized_paid_generator.py", raw.decode(), True)
        transferred["run_synchronized_paid_generator.py"] = sha(raw)
        receipt = self.session.call(
            "generator",
            "import hashlib,json;from pathlib import Path;p=Path("
            + repr(self.gen)
            + ");expected="
            + repr(transferred)
            + ";assert {k:hashlib.sha256((p/k).read_bytes().replace(b'\\r\\n',b'\\n')).hexdigest() for k in expected}==expected;print(json.dumps({'transferred_generator_identity':True}))",
            45,
        )
        if receipt.get("transferred_generator_identity") is not True:
            raise ValueError("Transferred generator proof missing")
        self.record.update(receipt, transferred_generator_source_sha256=transferred)
        self.prepared = True
        self.checkpoint()

    def dispatch(self, admission, *, start_at_epoch=None):
        self.check(self.profile.duration + 600)
        if not self.prepared or self.record["customers_dispatched"]:
            raise ValueError("Fresh prepared paid stage required")
        receipts = admission.get("cce_api_receipts", [])
        from cce_ecs_transition import receipt_gate

        receipt_gate(receipts, self.run)
        if (
            admission.get("run") != self.run
            or admission.get("binding_sha256") != policy.digest(self.guard.binding)
            or admission.get("all_required_pre_dispatch_gates_pass") is not True
            or any(
                r.get("startup_proof", {}).get("sources") != cce.contract()["api_sources"] for r in receipts
            )
            or any(r.get("resources") != cce.contract()["resources"] for r in receipts)
        ):
            raise ValueError("Bound native safety/observer/background admission required")
        start = time.time() + 30 if start_at_epoch is None else start_at_epoch
        if type(start) not in (int, float) or not math.isfinite(start) or not 3 <= start - time.time() <= 60:
            raise ValueError("Synchronized start must remain 3 to 60 seconds ahead")
        arguments = generator_arguments(self.gen, self.origin, start, profile=self.profile, recovery_max_attempts=3 if self.recovery_enabled else 1)
        if self.profile.duration == 3600:
            from cce_ingress_hourly_generator import preflight
            preflight(arguments, self.manifest)
            self.record["hourly_generator_preflight_pass"] = True
            self.record["hourly_generator_preflight_decision"] = "ADR0295"
            self.checkpoint()
        state = policy.read(policy.STATE)
        scope = state[self.guard.key]
        if (
            state.get("current_run") != self.run
            or scope.get("active_run") != self.run
            or scope.get("paid_protocols_started") != 1
            or scope.get("paid_protocol_arms") != ["candidate"]
            or scope.get("paid_runs_started") != 0
            or scope.get("paid_runs_authorized") != 1
            or scope.get("attempted_paid_arms")
            or self.session.state.get("capacity_stages_started") != 0
        ):
            raise ValueError("Fresh reserved CCE paid allowance required")
        self.record["admission"] = admission
        self.record["customers_dispatched"] = True  # Includes an ambiguous remote launch.
        self.record["capacity_stages_started"] = 1
        self.record["scheduled_offered_start_utc"] = datetime.fromtimestamp(start, UTC).isoformat()
        self.checkpoint()
        scope["paid_runs_started"] = 1
        scope["attempted_paid_arms"] = ["candidate"]
        policy.write(policy.STATE, state)  # Consume before any possible customer dispatch.
        self.session.state["capacity_stages_started"] = 1
        self.session.checkpoint()
        self.record["generator_launch_attempted"] = True
        self.checkpoint()
        job = self.jobs.launch(
            self.session, "generator", self.cid, arguments, self.gen, self.job_list, self.record
        )
        self.jobs.wait(self.session, job, "generator", self.profile.job_timeout, allowed_returncodes={0, 1})
        try:
            self.record["customer"] = json.loads(fetch(self.session, "generator", self.gen + "/customer.json"))
        except FileNotFoundError:
            # Retain bounded stderr before removing private inputs; never manufacture a report.
            try:
                path = self.gen + "/job-" + job["name"] + ".log"
                program = (
                    "import json;from pathlib import Path;p=Path(" + repr(path) + ")\n"
                    "assert p.is_file() and not p.is_symlink()\n"
                    "with p.open('rb') as stream:\n"
                    " stream.seek(max(0,p.stat().st_size-65536));tail=stream.read(65536).decode('utf-8',errors='replace')\n"
                    "print(json.dumps({'log_tail':tail}))"
                )
                log = self.session.call("generator", program, 30)
                policy.write(self.output / "generator-exit-log.private.json", log)
                self.record["generator_exit_log_retained"] = True
            except Exception as error:  # noqa: BLE001 - Never replace the original missing-report failure.
                self.record["generator_exit_log_failure_type"] = type(error).__name__
            self.checkpoint()
            raise
        self.checkpoint()
        if self.outcome_enabled:
            from cce_customer_outcome_slo import evaluate
            customer = self.record["customer"]
            customer["strict_generator_pass"] = customer["pass"]
            outcome = evaluate(customer, self.profile.expected)
            self.record["customer_outcome_gate"] = outcome
            customer["pass"] = outcome["pass"]
            self.checkpoint()
        return self.record["customer"]

    def audit(self, global_audit):
        """Mandatory even after lost response, failed SLO or generator failure."""
        if not self.record["customers_dispatched"]:
            return
        import cce_transaction_profile as transaction
        from cce_verification_contract import selected as verification_selected
        audit_program, audit_timeout = financial_audit_program, 175
        if verification_selected(transaction.active()):
            from cce_bounded_financial_audit import TRANSPORT_SECONDS, program
            audit_program, audit_timeout = program, TRANSPORT_SECONDS
        from cce_terminal_audit_admission import admit
        from cce_terminal_audit_admission import selected as terminal_selected
        if terminal_selected(transaction.active()):
            admit(self)
        errors = []
        for name, operation in (
            (
                "financial",
                lambda: self.session.api(self.cid, audit_program(self.events, self.profile.expected), audit_timeout),
            ),
            ("global_queues", lambda: drain(self.session, self.cid, global_audit)),
        ):
            try:
                self.record[name] = operation()
                if name == "financial" and self.outcome_enabled and "customer" in self.record:
                    from cce_customer_outcome_slo import financial
                    self.record[name] = financial(self.record[name], self.record["customer"], self.profile.expected)
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Preserve independent mandatory recovery operations.
                self.record[name + "_failure_type"] = type(error).__name__
                errors.append(name)
            self.checkpoint()
        import cce_transaction_profile as transaction
        from cce_paid_profiles import HEADROOM, RATE_PUSH
        from customer_timeout_bundle import issuance_program as short_window
        from customer_timeout_bundle import selected
        if selected(transaction.active()) and self.profile.duration == 300 and self.profile not in (HEADROOM, RATE_PUSH):
            try:
                start = datetime.fromisoformat(self.record["scheduled_offered_start_utc"]).timestamp()
                self.record["short_issuance_window"] = self.session.api(self.cid, short_window(self.events, start), 90)
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Preserve independent financial audits.
                self.record["short_issuance_window_failure_type"] = type(error).__name__
                errors.append("short_issuance_window")
            self.checkpoint()
        if self.profile.name == "cce_ticket_target":
            if self.profile in (HEADROOM, RATE_PUSH):
                from cce_recovery_headroom import issuance_program
            else:
                from cce_ticket_target_profile import issuance_program
            start = datetime.fromisoformat(self.record["scheduled_offered_start_utc"]).timestamp()
            self.record["ticket_target_issuance"] = self.session.api(self.cid, issuance_program(self.events, start, transaction.active()) if self.profile in (HEADROOM, RATE_PUSH) else issuance_program(self.events, start), 90)
            self.checkpoint()
        if self.profile.duration == 3600:
            try:
                from cce_hourly_window import program
                start = datetime.fromisoformat(self.record["scheduled_offered_start_utc"]).timestamp()
                self.record["hourly_issuance"] = self.session.api(self.cid, program(self.events, start), 90)
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Keep mandatory terminal audit independent.
                self.record["hourly_issuance_failure_type"] = type(error).__name__
                errors.append("hourly_issuance")
            self.checkpoint()
        if errors:
            raise ValueError("Mandatory native financial/queue audit failed")
        return {
            "post_ttl_financial_pass": self.record["financial"].get("pass") is True
            and self.record["financial"].get("hold_deadlines_elapsed") is True,
            "global_queues_pass": self.record["global_queues"].get("pass") is True,
        }

    def stop_jobs(self):
        errors = []
        for role, job in reversed(self.job_list):
            try:
                if self.jobs.stop(self.session, role, self.cid, job).get("running") is not False:
                    raise ValueError("Owned job still running")
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Retain independent mandatory recovery failures.
                errors.append(type(error).__name__)
        # A missing launch receipt is not evidence that no process was started.
        try:
            idle = self.session.call("generator", GENERATOR_IDLE, 45)
            self.record["generator_idle_after_stop"] = idle.get("generator_idle") is True
            if not self.record["generator_idle_after_stop"]:
                errors.append("GeneratorStillRunning")
        except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Unknown idle state must block input removal.
            self.record["generator_idle_after_stop"] = False
            errors.append(type(error).__name__)
        self.record["job_cleanup_errors"] = errors
        self.checkpoint()
        if errors:
            raise ValueError("Owned native stage jobs require recovery")
        self.job_list.clear()

    def cleanup(self, global_audit):
        """Run every mandatory operation despite a prior failure; retain failed receipts."""
        self.session.begin_cleanup()
        errors = []
        jobs_stopped = False
        try:
            self.stop_jobs()
            jobs_stopped = True
        except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Still execute financial and queue audits.
            errors.append({"operation": "stop_jobs", "type": type(error).__name__})
        try:
            self.audit(global_audit)
        except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Preserve failed audits while restoring owned resources.
            errors.append({"operation": "audit", "type": type(error).__name__})
        self.record["private_inputs_preserved_for_recovery"] = not jobs_stopped
        if self.events and jobs_stopped:
            try:
                receipt = self.session.api(
                    self.cid,
                    "import json,os,psycopg\nwith psycopg.connect(os.environ['DATABASE_URL']) as conn:\n rows=conn.execute('UPDATE events SET sale_ends=clock_timestamp() WHERE id=ANY(%s::uuid[]) RETURNING id',("
                    + repr(self.events)
                    + ",)).fetchall()\nprint(json.dumps({'retired_shows':len(rows)}))",
                    45,
                )
                self.record["retired_shows"] = receipt["retired_shows"]
                if receipt["retired_shows"] != self.profile.shows:
                    raise ValueError("Incomplete owned fixture retirement")
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Retain independent mandatory recovery failures.
                errors.append({"operation": "retire_fixture", "type": type(error).__name__})
        if jobs_stopped and self.record.get("generator_directory_creation_attempted"):
            try:
                owner = {"run": self.run, "binding_sha256": policy.digest(self.guard.binding)}
                code = (
                    "from pathlib import Path;import json\np=Path("
                    + repr(self.gen)
                    + ")\nexpected="
                    + repr(owner)
                    + r"""
if p.is_symlink() or not p.is_dir() or json.loads((p/'.cce-stage-owner.json').read_text())!=expected:raise ValueError('Unknown generator directory ownership')
def remove(path):
 if path.exists():
  if path.is_symlink() or not path.is_file():raise ValueError('Unexpected manifest path')
  path.unlink()
remove(p/'manifest.private.json')
for child in [*p.glob('paid-shards-*'),*p.glob('paid-hourly-shards-*')]:
 if child.is_symlink() or not child.is_dir() or child.resolve().parent!=p.resolve():raise ValueError('Unknown shard ownership')
 for file in child.iterdir():
  if file.name not in {'manifest-0.json','manifest-1.json','result-0.json','result-1.json'}:raise ValueError('Unknown shard artifact')
  remove(file)
 child.rmdir()
print(json.dumps({'private_manifests_removed':True}))
"""
                )
                receipt = self.session.call("generator", code, 45)
                self.record["private_manifests_removed"] = receipt.get("private_manifests_removed") is True
                if not self.record["private_manifests_removed"]:
                    raise ValueError("Private manifest cleanup unverified")
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Retain independent mandatory recovery failures.
                errors.append({"operation": "private_manifests", "type": type(error).__name__})
        self.record["cleanup_errors"] = errors
        self.record["private_cleanup_pass"] = not errors
        self.checkpoint()
        if errors:
            raise ValueError("Native paid stage recovery remains incomplete")
        return self.record
