"""ADR0228 one fixed CCE API isolation experiment; no generalized deployment runner."""

import copy
import json
import sys
import time
from pathlib import Path
from uuid import uuid4

import cce_api_adapter as native
import cce_paid_stage as paid
import generator_completion_probe_contract as baseline
import qualify_two_host_deployment as driver
import work_envelope as policy
from cce_ecs_transition import Transition
from cce_paid_inputs import Inputs
from cce_paid_lifecycle import Lifecycle
from cce_paid_observers import Observers
from cce_paid_profiles import HOURLY, SHORT, for_guard
from cce_paid_resources import BRIDGE_CPU_LIMIT, Resources
from diagnostic_runner_connection import ProtectedContext, validate_target
from run_status_refresh_comparison import RunLock, restoration_complete

BASELINE = "docs/capacity/flash-sale-opening/cce-admission-control-2026-10-09.json"


def identity():

    return {
        **paid.identity(),
        **{
            p: paid.sha((policy.ROOT / p).read_bytes())
            for p in (
                "scripts/run_cce_paid_comparison.py",
                "scripts/cce_paid_inputs.py",
                "scripts/run_work_envelope.py",
                "scripts/work_envelope.py",
                "scripts/diagnostic_runner_connection.py",
                "scripts/qualify_two_host_deployment.py",
            )
        },
    }


def plan(*, profile=SHORT):
    import cce_transaction_profile as transaction
    goal = transaction.active()
    if goal is not None:
        if profile == HOURLY and goal.get("extension_decision") == "ADR0251":
            from run_cce_hourly_qualification import plan as hourly_plan
            return hourly_plan()
        if profile != SHORT or goal.get("profile") != SHORT.name:
            raise ValueError("New transaction images require separate hourly qualification binding")
        return transaction.plan()
    if profile == HOURLY:
        from run_cce_hourly_qualification import plan as hourly_plan
        return hourly_plan()
    if profile != SHORT:
        raise ValueError("Exact registered native profile required")

    return {
        "decision": "ADR0228",
        "arms": ["candidate"],
        "common": {"buyer_journeys_per_second": 84, "duration_seconds": 300},
        "diagnostic_connection_decision": "ADR0180",
        "kind": "fixed_native_api_isolation",
        "candidate_decision": "ADR0234",
        "single_changed_factor": "api_shared_acquisition_budget",
        "baseline_acquisition_budget": 12,
        "candidate_acquisition_budget": native.admission_budget(),
        "bridge_cpu_limit": BRIDGE_CPU_LIMIT,
        "baseline_bridge_cpu_limit": 1,
        "baseline_evidence": BASELINE,
        "baseline_sha256": policy.digest(policy.read(policy.ROOT / BASELINE)),
        "pod_resources": native.contract()["resources"],
        "replicas": 4,
        "pooler_server_connections": 24,
        "qualification_runs_authorized": 1,
        "paid_runs_authorized": 1,
        "safety_tickets_authorized": 2,
        "duration_reason": "Match the retained five-minute paid baseline; required TTL, drain and exact restoration remain outside the offered window.",
    }


def binding_for(config, proof, manifests, target, snapshot, *, profile=SHORT):
    entry = entry_for(profile)

    native.dependency.validate_proof(proof)

    validate_target(target)

    import cce_transaction_profile as transaction
    extra = ({"cce_transaction_arm": transaction.active()["comparison_arm"],
              "cce_transaction_pair_sha256": transaction.proof_digest(transaction.active())}
             if transaction.active() is not None else {})
    goal = transaction.active()
    if goal is not None and goal["extension_decision"] == "ADR0245":
        extra.update(cce_partition_decision="ADR0245", cce_payment_pool_max=transaction.payment_connections(goal))
    if goal is not None and goal["extension_decision"] == "ADR0249":
        extra.update(cce_payment_context_decision="ADR0249", cce_payment_pool_max=2)
    if goal is not None and goal["extension_decision"] == "ADR0251":
        from cce_simulator_dispatch_profile import receipt
        extra.update(cce_simulator_decision="ADR0251", cce_simulator_concurrency=12,
                     cce_simulator_database_pool_max=10, cce_simulator_image_id=receipt()["local_image_id"])
    return {
        **extra,
        "configuration_sha256": policy.digest(config),
        "cce_acquisition_budget": native.admission_budget(),
        "cce_paid_entry_sources": entry.identity(),
        "cce_paid_core_sources": paid.identity(),
        "cce_manifest_sha256": policy.digest(manifests),
        "cce_bridge_cpu_limit": BRIDGE_CPU_LIMIT,
        "cce_resource_source_sha256": paid.sha((policy.ROOT / "scripts/cce_paid_resources.py").read_bytes()),
        "cce_transition_source_sha256": paid.sha(
            (policy.ROOT / "scripts/cce_ecs_transition.py").read_bytes()
        ),
        "diagnostic_target_sha256": policy.digest(target),
        "image_proof_sha256": policy.digest(proof),
        "saved_api_service_sha256": policy.digest(api_semantics(service_for(snapshot))),
        "baseline_sha256": entry.plan()["baseline_sha256"],
    }



def entry_for(profile):
    if profile == SHORT:
        return sys.modules[__name__]
    if profile == HOURLY:
        import run_cce_hourly_qualification
        return run_cce_hourly_qualification
    raise ValueError("Exact registered native profile required")

def service_for(saved):

    service = copy.deepcopy(saved["model"]["services"]["api"])

    service["image"] = native.dependency.INDEX

    service["environment"].update(native.legacy_contract()["api_settings"])

    native.api_environment(service, "10.1.137.69")

    return service


def api_semantics(service):
    return {
        "image": service["image"],
        "environment": native.api_environment(service, "10.1.137.69"),
        "command": service.get("command"),
        "volumes": service.get("volumes", []),
    }


def http_reader(session, address, path, *, metrics=False):

    address = native.private_ip(address)

    if path not in ("/health/ready", "/metrics"):
        raise ValueError("Read-only native application evidence only")

    code = """import json,urllib.request

with urllib.request.urlopen(URL,timeout=3) as response:

 raw=response.read(4194305)

if len(raw)>4194304:raise ValueError('Response too large')

print(json.dumps(raw.decode()))

""".replace("URL", repr("http://" + address + ":8000" + path))

    raw = session.call("primary", code, 15)

    if not metrics:
        return json.loads(raw)

    values = {}

    # The shared parser intentionally retains workload metrics only.

    for line in raw.splitlines():
        fields = line.split()

        if len(fields) == 2 and fields[0] in ("process_cpu_seconds_total", "process_start_time_seconds"):
            values[fields[0]] = float(fields[1])

    return values


class ReadyDeployment(native.Deployment):
    def observe(self, manifests, http_ready, http_metrics, *, previous=None):

        if previous is None:
            deadline = time.monotonic() + 180

            while True:
                self.authorize()

                ready = True
                pending_views = []

                for item in manifests[3:]:
                    pod = self.request("GET", self.path() + "/pods/" + item["metadata"]["name"], None)

                    if pod is None or pod.get("metadata", {}).get("uid") != self.pod_uids.get(
                        item["metadata"]["name"]
                    ):
                        raise ValueError("Owned pod disappeared or was replaced before readiness")

                    pending_views.append((pod, item))
                    statuses = pod.get("status", {}).get("containerStatuses", [])

                    if any(
                        s.get("restartCount", 0) > 0 or s.get("state", {}).get("terminated") for s in statuses
                    ):
                        context = native.verification_summary(pod, item)
                        context["verification_gate"] = "initial_pod_startup"
                        self.persist({"failed_pod_verification": context})
                        raise ValueError("Native startup failed; no paid progression")

                    ready &= any(
                        c.get("type") == "Ready" and c.get("status") == "True"
                        for c in pod.get("status", {}).get("conditions", [])
                    )

                if ready:
                    break

                if time.monotonic() >= deadline:
                    self.persist({"readiness_timeout_views": [
                        native.verification_summary(pod, item) for pod, item in pending_views
                    ]})
                    raise TimeoutError("Bounded four-pod readiness expired")

                time.sleep(2)

        return super().observe(manifests, http_ready, http_metrics, previous=previous)


def activate_scope(guard, run):

    profile = for_guard(guard)
    guard.check(profile.duration + 900)

    state = policy.read(policy.STATE)

    scope = state[guard.key]

    if (
        state.get("current_run")
        or scope.get("active_run")
        or any(
            scope.get(k)
            for k in (
                "qualification_protocols_started",
                "paid_protocols_started",
                "safety_protocols_started",
                "paid_runs_started",
            )
        )
    ):
        raise ValueError("Fresh CCE reservation required; no replay")

    scope.update(
        active_run=run,
        qualification_protocols_started=1,
        paid_protocols_started=1,
        paid_protocol_arms=["candidate"],
        safety_protocols_started=2,
    )

    state["current_run"] = run

    policy.write(policy.STATE, state)


def worker_contract():
    import cce_transaction_profile as transaction
    goal = transaction.active()
    if goal is not None and goal["extension_decision"] == "ADR0251":
        from cce_simulator_dispatch_profile import worker_contract as simulator_contract
        return simulator_contract(goal)
    return baseline.GeneratorCompletionProbeContract(
        baseline.plan()["artifact_receipt"], "candidate", native.legacy_contract()["api_sources"])


def run(config, output, guard, manifests, kubeconfig, context):
    profile = for_guard(guard)

    output = Path(output)

    run_id = output.name

    record = {
        "run": run_id,
        "pass": False,
        "capacity_stages_started": 0,
        "baseline_evidence": plan(profile=profile)["baseline_evidence"],
        "binding_sha256": policy.digest(guard.binding),
        "native_resources_creation_attempted": False,
        "transport_creation_attempted": False,
    }

    contract = worker_contract()

    contract.diagnostic_context = context

    native_record = {}

    def persist(value):

        native_record.update(value)

        policy.write(output / "native-lifecycle.private.json", native_record)

    def hook(session, arm, routes, saved, owner, _output):

        if arm == "control":
            return

        guard.check(profile.duration + 900)

        service = service_for(saved)

        if (
            policy.digest(api_semantics(service)) != guard.binding["saved_api_service_sha256"]
            or policy.digest(
                native.objects(
                    run_id,
                    service,
                    config["primary"]["private_ipv4"],
                    registry_username,
                    registry_password,
                    acquisition_budget=native.admission_budget(),
                    profile=profile,
                )
            )
            != guard.binding["cce_manifest_sha256"]
        ):
            raise ValueError("Current API environment differs from the pre-bound deployment")

        local = output / "candidate"

        local.mkdir()

        resources = Resources(
            session, guard, run_id, owner, service["environment"], lambda value: persist({"resources": value})
        )

        transport = None

        lifecycle = None

        try:
            record["native_resources_creation_attempted"] = True
            policy.write(output / "cce-comparison.private.json", record)
            cid = resources.create()

            stage = paid.PaidStage(session, guard, run_id, cid, local)

            inputs = Inputs(stage, owner, contract.global_audit)

            record["transport_creation_attempted"] = True
            policy.write(output / "cce-comparison.private.json", record)
            transport = native.KubernetesTransport(session, kubeconfig)

            deployment = ReadyDeployment(transport.request, guard, persist)

            transition = Transition(
                session, guard, run_id, routes, owner, lambda value: persist({"transition": value})
            )

            inventory = policy.read(output / "pre-safety-inventory.private.json")

            observers = Observers(stage, inventory, contract, context, owner)

            background = session.call("primary", driver.INSPECT, 45)

            from cce_ecs_transition import execution_identity

            record["pre_transition_execution_sha256"] = policy.digest(
                [execution_identity(r) for r in background]
            )

            def refresh_inventory():
                fresh = contract.pre_safety(session, routes, saved)
                if any(
                    fresh["hosts"][role]["machine_id_sha256"] != inventory["hosts"][role]["machine_id_sha256"]
                    for role in ("primary", "secondary")
                ):
                    raise ValueError("Transition inventory machine identity changed")
                policy.write(local / "pre-transition-inventory.private.json", fresh)
                observers.inventory = fresh
                return {"captured_at": fresh["captured_at"], "sha256": policy.digest(fresh)}

            lifecycle = Lifecycle(
                stage,
                resources,
                deployment,
                transition,
                observers,
                manifests,
                qualify_safety=inputs.safety,
                cleanup_safety=inputs.cleanup,
                prepare_fixture=inputs.paid,
                identities={r: inventory["hosts"][r]["machine_id_sha256"] for r in ("primary", "secondary")},
                background_before=background,
                persist=lambda value: persist({"lifecycle": value}),
                refresh_inventory=refresh_inventory,
            )

            record["native"] = lifecycle.run(
                lambda ip: http_reader(session, ip, "/health/ready"),
                lambda ip: http_reader(session, ip, "/metrics", metrics=True),
                contract.global_audit,
            )

        finally:
            if lifecycle is not None:
                record["native"] = lifecycle.record

                record["paid_stage"] = stage.record

            else:
                session.begin_cleanup()

                resources.cleanup()

            if transport is not None:
                transport.close()

                record["transport_credentials_cleared"] = True

            policy.write(output / "cce-comparison.private.json", record)

    # Secrets stay in this private closure; never emitted into reports.

    registry_username = REGISTRY_CREDENTIALS["username"]

    registry_password = REGISTRY_CREDENTIALS["password"]

    activate_scope(guard, run_id)

    restored = driver.run(config, output, stage_hook=hook, runtime_policy=contract, action_guard=guard)

    record["restoration"] = restored

    record["restoration_complete"] = restoration_complete(restored)

    record["capacity_stages_started"] = restored.get("capacity_stages_started", 0)

    record["pass"] = (
        restored.get("pass") is True
        and record["restoration_complete"]
        and record.get("native", {}).get("pass") is True
    )

    record["integrity_verified"] = (
        restored.get("post_ttl_financial", {}).get("pass") is True
        and restored.get("post_ttl_financial", {}).get("hold_deadlines_elapsed") is True
        and (
            record["capacity_stages_started"] == 0
            or all(
                record.get("native", {}).get("measurement_gates", {}).get(k) is True
                for k in (
                    "post_ttl_financial",
                    "zero_double_booking",
                    "payment_durability",
                    "full_queue_drain",
                )
            )
        )
    )

    policy.write(output / "cce-comparison.private.json", record)

    return record


# Populated only by the protected entry point and erased in its finally block.

REGISTRY_CREDENTIALS = {}


def execute(
    config_path,
    proof_path,
    ssh_runtime,
    kubeconfig,
    target_path,
    snapshot_path,
    ecs_password,
    diagnostic_password,
    registry_username,
    registry_password,
    *, profile=SHORT,
):
    entry_module = entry_for(profile)

    native.authorized_creation(policy.envelope())

    config, proof, target = (policy.read(p) for p in (config_path, proof_path, target_path))

    from run_two_host_paid_comparison import validate_config

    validate_config(config)

    target = validate_target(target)

    snapshot = policy.read(snapshot_path)["saved"]

    output = policy.ROOT / "tmp" / ("adr0151-" + uuid4().hex[:12])

    output.mkdir()

    # Use only the canonical generated ownership identity for all run paths.

    run_id = output.name

    manifests = native.objects(
        run_id,
        service_for(snapshot),
        config["primary"]["private_ipv4"],
        registry_username,
        registry_password,
        acquisition_budget=native.admission_budget(),
        profile=profile,
    )

    binding = binding_for(config, proof, manifests, target, snapshot, profile=profile)

    lock = RunLock(run_id)

    entry = guard = None

    report = {"run": run_id, "pass": False, "restoration_complete": False, "integrity_verified": False}

    started = time.monotonic()

    context = ProtectedContext(target, diagnostic_password)

    sys.path.insert(0, str(Path(ssh_runtime).resolve()))

    import getpass

    original_prompt = getpass.getpass

    try:
        entry = policy.reserve(binding, entry_module.plan(), profile=profile.name)

        guard = policy.ActionGuard(entry["ledger"], binding)

        # The existing outer preflight verifies every required cached image before mutation.

        REGISTRY_CREDENTIALS.update(username=registry_username, password=registry_password)

        getpass.getpass = lambda prompt="": ecs_password

        report = run(config, output, guard, manifests, kubeconfig, context)

    finally:
        getpass.getpass = original_prompt

        REGISTRY_CREDENTIALS.clear()

        context.clear()

        if entry is not None:
            state = policy.read(policy.STATE)

            scope = state[entry["ledger"]]

            if report.get("restoration_complete") is True and report.get("integrity_verified") is True:
                scope["active_run"] = None

                if state.get("current_run") == run_id:
                    state["current_run"] = None

            scope["cce_paid_result_sha256"] = policy.digest(report)

            policy.write(policy.STATE, state)

            receipt = guard.finish([report], time.monotonic() - started)

        policy.write(output / "cce-comparison.private.json", report)

        lock.release()

    return receipt


def outcome(report, scope, binding, *, profile=SHORT):
    entry = entry_for(profile)

    valid = (
        isinstance(report, dict)
        and scope.get("binding") == binding
        and scope.get("cce_paid_result_sha256") == policy.digest(report)
        and scope.get("qualification_protocols_started") == 1
        and scope.get("paid_protocols_started") == 1
        and scope.get("safety_protocols_started") == 2
        and report.get("binding_sha256") == policy.digest(binding)
        and binding.get("cce_paid_entry_sources") == entry.identity()
    )

    native_result = report.get("native", {}) if isinstance(report, dict) else {}

    restored = (
        valid
        and report.get("restoration_complete") is True
        and native_result.get("cleanup_complete") is True
        and report.get("transport_credentials_cleared") is True
    )

    integrity = valid and report.get("integrity_verified") is True

    passed = (
        restored
        and integrity
        and report.get("pass") is True
        and report.get("capacity_stages_started") == scope.get("paid_runs_started") == 1
    )

    return restored, integrity, passed
