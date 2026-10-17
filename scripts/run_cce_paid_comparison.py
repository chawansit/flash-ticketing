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
from cce_paid_profiles import (
    HEADROOM,
    HIGHER_RATE_PUSH,
    HOURLY,
    RATE_PUSH,
    RATE_PUSH_160,
    SHORT,
    TICKET_TARGET,
    for_guard,
)
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
    from cce_recovery_headroom import selected as headroom_selected
    if headroom_selected(goal):
        from cce_recovery_headroom import plan as headroom_plan
        return headroom_plan(goal)
    if goal is not None:
        if profile == HOURLY and (goal.get("extension_decision") in {"ADR0251", "ADR0256"}
                                  or goal.get("hourly_qualification_decision") == "ADR0281"):
            from run_cce_hourly_qualification import plan as hourly_plan
            return hourly_plan()
        if goal.get("extension_decision") == "ADR0277" and profile in (SHORT, TICKET_TARGET):
            return transaction.plan()
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
    from cce_auth_dispatch_profile import contract as authentication_contract
    from cce_auth_dispatch_profile import selected as authentication_selected
    if authentication_selected(goal):
        extra["cce_auth_dispatch_contract"] = authentication_contract()
    from cce_status_reuse_hourly import adapters
    from cce_status_reuse_hourly import selected as hourly_selected
    if hourly_selected(goal):
        extra.update(cce_hourly_qualification_decision="ADR0281", cce_hourly_adapters_sha256=policy.digest(adapters()))
    import cce_native_database_bridge as bridge
    if bridge.selected(goal):
        extra.update(cce_native_database_bridge_decision="ADR0279", cce_native_database_bridge_sha256=policy.digest(bridge.contract()))
    from cce_status_refresh_reuse import contract as reuse_contract
    from cce_status_refresh_reuse import selected as reuse_selected
    if reuse_selected(goal):
        extra.update(cce_status_refresh_reuse_decision="ADR0280", cce_status_refresh_reuse_sha256=policy.digest(reuse_contract()))
    from customer_timeout_bundle import MANIFEST_SHA256 as timeout_sha
    from customer_timeout_bundle import selected
    if selected(goal):
        extra.update(cce_timeout_retention_decision="ADR0278", cce_timeout_bundle_sha256=timeout_sha)
    if goal is not None and goal["extension_decision"] in {"ADR0266", "ADR0271", "ADR0277"}:
        from customer_recovery_bundle import MANIFEST_SHA256
        extra.update(cce_event_lane_decision="ADR0266", cce_event_consumer_pool_budget=48,
                     cce_fulfillment_consumers=6, cce_projection_consumers=1,
                     cce_simulator_concurrency=16, cce_simulator_database_pool_max=10,
                     cce_recovery_max_attempts=3, cce_recovery_bundle_sha256=MANIFEST_SHA256,
                     cce_order_status_read_pipeline="0")
    from cce_payment_delivery_headroom import selected as delivery_selected
    if delivery_selected(goal):
        from cce_payment_delivery_headroom import contract as delivery_contract
        extra.update(cce_simulator_concurrency=32, cce_payment_delivery_contract=delivery_contract())
    if goal is not None and goal["extension_decision"] == "ADR0263":
        from customer_recovery_bundle import MANIFEST_SHA256
        extra.update(cce_dispatch_decision="ADR0263", cce_simulator_concurrency=16,
                     cce_simulator_database_pool_max=10, cce_recovery_max_attempts=3,
                     cce_recovery_bundle_sha256=MANIFEST_SHA256, cce_order_status_read_pipeline="0")
    if goal is not None and goal["extension_decision"] in {"ADR0271", "ADR0277"}:
        from cce_worker_rebalance_profile import factor
        extra.update(cce_worker_placement_decision="ADR0271", cce_worker_placement_factor=factor(),
                     cce_live_admission_decision="ADR0272")
    if goal is not None and goal["extension_decision"] == "ADR0259":
        import cce_shared_worker_comparison as shared
        extra.update(cce_worker_placement_decision="ADR0259", cce_worker_placement_plan_sha256=shared.digest(shared.plan()),
                     cce_recovery_max_attempts=3, cce_order_status_read_pipeline="0")
    if goal is not None and goal["extension_decision"] == "ADR0245":
        extra.update(cce_partition_decision="ADR0245", cce_payment_pool_max=transaction.payment_connections(goal))
    if goal is not None and goal["extension_decision"] == "ADR0249":
        extra.update(cce_payment_context_decision="ADR0249", cce_payment_pool_max=2)
    if goal is not None and goal["extension_decision"] in {"ADR0255", "ADR0256"}:
        from customer_recovery_bundle import MANIFEST_SHA256
        if goal["extension_decision"] == "ADR0256":
            extra.update(cce_recovery_hourly_decision="ADR0256")
        extra.update(cce_recovery_decision="ADR0254", cce_status_pipeline_decision="ADR0255",
                     cce_order_status_read_pipeline="1" if goal["comparison_arm"] == "candidate" else "0",
                     cce_recovery_max_attempts=3, cce_recovery_bundle_sha256=MANIFEST_SHA256)
    if goal is not None and goal["extension_decision"] in {"ADR0251", "ADR0255", "ADR0256"}:
        from cce_simulator_dispatch_profile import receipt
        extra.update(cce_simulator_decision="ADR0251", cce_simulator_concurrency=12,
                     cce_simulator_database_pool_max=10, cce_simulator_image_id=receipt()["local_image_id"])
    from cce_payment_pipeline_profile import selected as pipeline_selected
    if pipeline_selected(goal):
        from cce_payment_pipeline_identity import RECEIPT_SHA256
        extra.update(cce_transaction_arm=goal["payment_pipeline_arm"],
                     cce_payment_pipeline_arm=goal["payment_pipeline_arm"], cce_payment_pipeline_decision="ADR0284",
                     cce_payment_write_pipeline="1" if goal["payment_pipeline_arm"] == "candidate" else "0",
                     cce_payment_pipeline_receipt_sha256=RECEIPT_SHA256,
                     cce_verification_contract_sha256=goal["verification_contract_sha256"])
    from cce_committed_status_profile import selected as committed_selected
    if committed_selected(goal):
        from cce_committed_status_identity import RECEIPT_SHA256
        extra.update(cce_transaction_arm=goal["committed_status_arm"],
                     cce_committed_status_decision="ADR0298", cce_committed_status_arm=goal["committed_status_arm"],
                     cce_committed_status_snapshot="1" if goal["committed_status_arm"] == "candidate" else "0",
                     cce_committed_status_receipt_sha256=RECEIPT_SHA256,
                     cce_verification_contract_sha256=goal["verification_contract_sha256"])
    from cce_measured_payment_context_profile import contract as context_contract
    from cce_measured_payment_context_profile import selected as context_selected
    if context_selected(goal):
        extra["cce_measured_payment_context_contract"] = context_contract()
    from cce_fulfillment_batch_profile import contract as fulfillment_contract
    from cce_fulfillment_batch_profile import selected as fulfillment_selected
    if fulfillment_selected(goal):
        extra["cce_fulfillment_batch_contract"] = fulfillment_contract()
    from cce_explicit_start_profile import contract as explicit_contract
    from cce_explicit_start_profile import selected as explicit_selected
    if explicit_selected(goal):
        extra["cce_explicit_start_contract"] = explicit_contract()
    from cce_explicit_start_profile import selected as explicit_selected
    from cce_notification_dedup_profile import contract as notification_contract
    from cce_notification_dedup_profile import selected as notification_selected
    if notification_selected(goal):
        extra["cce_notification_dedup_contract"] = notification_contract()
    from cce_writer_sale_context_profile import contract as writer_context_contract
    from cce_writer_sale_context_profile import selected as writer_context_selected
    if writer_context_selected(goal):
        extra["cce_writer_sale_context_contract"] = writer_context_contract()
    from cce_metric_admission import contract as metric_contract
    from cce_metric_admission import selected as metric_selected
    if metric_selected(goal):
        extra.update(cce_live_admission_decision="ADR0287",
                     cce_metric_admission_contract_sha256=policy.digest(metric_contract()))
    from cce_nginx_ingress import contract as ingress_contract
    from cce_nginx_ingress import selected as ingress_selected
    if ingress_selected(goal):
        extra.update(cce_ingress_route_decision="ADR0291",
                     cce_ingress_route_contract_sha256=policy.digest(ingress_contract()),
                     cce_ingress_route_arm=goal["ingress_route_arm"])
        if goal.get("ingress_hourly_decision") is not None:
            extra.update(cce_ingress_hourly_decision=goal["ingress_hourly_decision"],
                         cce_ingress_hourly_prerequisite_sha256=goal["ingress_hourly_prerequisite_sha256"])
    if goal is not None and goal.get("hourly_observer_bootstrap_decision") == "ADR0294":
        extra.update(cce_hourly_observer_bootstrap_decision="ADR0294",
                     cce_hourly_observer_seconds=4200)
    from cce_runtime_cohort_profile import selected as cohort_selected
    if cohort_selected(goal):
        from paid_cohort_query import runtime_contract
        extra.update(cce_runtime_cohort_contract=runtime_contract(goal["runtime_cohort_arm"]))
    from cce_customer_outcome_slo import contract as outcome_contract
    from cce_customer_outcome_slo import selected as outcome_selected
    if outcome_selected(goal):
        extra.update(cce_customer_outcome_contract=outcome_contract())
    from cce_payment_slot_profile import connections
    from cce_payment_slot_profile import selected as slot_selected
    if slot_selected(goal):
        extra.update(cce_payment_slot_decision="ADR0306", cce_payment_slot_arm=goal["payment_slot_arm"],
                     cce_payment_pool_max=connections(goal), cce_transaction_arm=goal["payment_slot_arm"])
    from cce_inventory_renewal import contract as renewal_contract
    from cce_inventory_renewal import selected as renewal_selected
    if renewal_selected(goal):
        extra.update(cce_inventory_renewal_contract=renewal_contract())
    from cce_terminal_audit_admission import contract as terminal_contract
    from cce_terminal_audit_admission import selected as terminal_selected
    if terminal_selected(goal):
        extra.update(cce_terminal_audit_admission_contract=terminal_contract())
    from customer_gateway_read_bundle import selected as gateway_selected
    if gateway_selected(goal):
        from customer_gateway_read_bundle import contract as gateway_contract
        extra.update(cce_gateway_read_recovery_contract=gateway_contract())
    from cce_recovery_headroom import selected as headroom_selected
    if headroom_selected(goal):
        from cce_recovery_headroom import contract as headroom_contract
        extra.update(cce_headroom_contract=headroom_contract(goal))
        from cce_throughput_push import contract as push_contract
        from cce_throughput_push import selected as push_selected
        if push_selected(goal):
            extra["cce_throughput_push_contract"] = push_contract(goal)
        from cce_recovery_headroom import writer_scale_contract, writer_scale_selected
        if writer_scale_selected(goal):
            from cce_worker_rebalance_profile import factor
            scale_contract = writer_scale_contract()
            extra.update(cce_writer_scale_contract=scale_contract,
                         cce_worker_placement_factor={**factor(), "worker_counts": scale_contract["native_counts"]})
    from cce_recovery_headroom import writer_batch_contract, writer_batch_selected
    if writer_batch_selected(goal):
        extra.update(cce_writer_batch_contract=writer_batch_contract())
    from cce_backend_partition import contract as partition_contract
    from cce_backend_partition import selected as partition_selected
    if partition_selected(goal):
        extra.update(cce_backend_partition_contract=partition_contract(goal["backend_partition_decision"]))
    import cce_primary_bootstrap as bootstrap
    if bootstrap.enabled(config) != bootstrap.selected(goal):
        raise ValueError("Primary-only configuration and authorization must match")
    if bootstrap.selected(goal):
        extra["cce_primary_bootstrap_contract"] = bootstrap.contract()
    import cce_generator_process_scaling as generator_scaling
    if generator_scaling.selected(goal):
        extra["cce_generator_process_contract"] = generator_scaling.contract(goal)
        import cce_fixture_clock as clock
        if clock.selected(goal):
            extra["cce_fixture_clock_contract"] = clock.contract()
    binding = {
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
    if cohort_selected(goal) and goal["runtime_cohort_arm"] == "candidate":
        from cce_runtime_cohort_profile import validate_candidate_binding
        validate_candidate_binding(goal, binding)
    from cce_committed_hourly_profile import selected as committed_hourly
    if committed_hourly(goal):
        from cce_committed_hourly_profile import validate_binding as validate_hourly
        binding["cce_committed_hourly_qualification"] = validate_hourly(goal, binding)
    elif headroom_selected(goal):
        from cce_recovery_headroom import validate_binding as validate_headroom
        validate_headroom(goal, binding)
        from cce_throughput_push import selected as push_selected
        from cce_throughput_push import validate_binding as validate_push
        if push_selected(goal):
            validate_push(goal, binding)
    elif slot_selected(goal) and goal["payment_slot_arm"] == "candidate":
        from cce_payment_slot_profile import validate_candidate_binding as validate_slots
        validate_slots(goal, binding)
    return binding



def entry_for(profile):
    if profile in (SHORT, TICKET_TARGET, HEADROOM, HIGHER_RATE_PUSH, RATE_PUSH, RATE_PUSH_160):
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


WORKER_METRICS_READ = r"""
import json,urllib.request,urllib.error
try:
 with urllib.request.urlopen('http://'+address+':9101/metrics',timeout=3) as response:
  raw=response.read(4194305)
 if len(raw)>4194304:raise ValueError('Worker metrics exceed bound')
 result={'ready':True,'metrics':raw.decode()}
except urllib.error.URLError as error:
 if isinstance(error.reason,ConnectionRefusedError):result={'ready':False,'kind':'connection_refused'}
 elif isinstance(error.reason,TimeoutError):result={'ready':False,'kind':'timeout'}
 else:raise
print(json.dumps(result))
"""


def worker_metrics_reader(session, address):
    address = native.private_ip(address)
    result = session.call("primary", "address=" + repr(address) + "\n" + WORKER_METRICS_READ, 15)
    if result == {"ready": False, "kind": "connection_refused"}:
        raise ConnectionRefusedError("Worker metrics listener not ready")
    if result == {"ready": False, "kind": "timeout"}:
        raise TimeoutError("Worker metrics startup request timed out")
    if not isinstance(result, dict) or set(result) != {"ready", "metrics"} or result["ready"] is not True:
        raise ValueError("Exact bounded worker metrics response required")
    raw = result["metrics"]
    if not isinstance(raw, str) or len(raw.encode()) > 4194304:
        raise ValueError("Bounded worker metrics payload required")
    import math
    values = {}
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[0] in {"process_cpu_seconds_total", "process_start_time_seconds"}:
            if fields[0] in values:
                raise ValueError("Duplicate worker process metric")
            values[fields[0]] = float(fields[1])
    if any(type(values.get(k)) not in (int, float) or not math.isfinite(values[k]) or values[k] < 0 for k in ("process_cpu_seconds_total", "process_start_time_seconds")) or values["process_start_time_seconds"] <= 0:
        raise ValueError("Complete worker process metrics required")
    return values


class ReadyDeployment(native.Deployment):
    ingress_route = None

    def cleanup(self):
        if self.ingress_route is not None:
            proof = self.ingress_route.cleanup()
            if proof != {"owned_route_removed": True, "existing_addon_unchanged": True}:
                raise ValueError("Owned ingress removal and unchanged addon required")
        return super().cleanup()

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
    import cce_transaction_profile as notification_transaction
    from cce_notification_dedup_profile import selected as notification_selected
    notification_goal = notification_transaction.active()
    from cce_fulfillment_batch_profile import selected as fulfillment_selected
    if fulfillment_selected(notification_goal):
        import cce_fulfillment_batch_identity as fulfillment_pins
        from cce_committed_status_contract import CommittedStatusContract
        return CommittedStatusContract(arm="candidate", identity=fulfillment_pins)
    from cce_explicit_start_profile import selected as explicit_selected
    if explicit_selected(notification_goal):
        import cce_explicit_start_identity as explicit_pins
        from cce_committed_status_contract import CommittedStatusContract
        return CommittedStatusContract(arm="candidate", identity=explicit_pins)
    if notification_selected(notification_goal):
        import cce_notification_dedup_identity as notification_pins
        from cce_committed_status_contract import CommittedStatusContract
        return CommittedStatusContract(arm=notification_goal["committed_status_arm"], identity=notification_pins)
    import cce_transaction_profile as transaction
    goal = transaction.active()
    from cce_writer_sale_context_profile import selected as writer_context_selected
    if writer_context_selected(goal):
        import cce_writer_sale_context_identity as writer_pins
        from cce_committed_status_contract import CommittedStatusContract
        return CommittedStatusContract(arm=goal["committed_status_arm"], identity=writer_pins)
    from cce_measured_payment_context_profile import selected as context_selected
    if context_selected(goal):
        import cce_measured_payment_context_identity as context_pins
        from cce_committed_status_contract import CommittedStatusContract
        return CommittedStatusContract(arm=goal["committed_status_arm"], identity=context_pins)
    from cce_committed_status_profile import selected as committed_selected
    if committed_selected(goal):
        from cce_committed_status_contract import CommittedStatusContract
        return CommittedStatusContract(arm=goal["committed_status_arm"])
    from cce_payment_pipeline_profile import selected as pipeline_selected
    if pipeline_selected(goal):
        from cce_payment_pipeline_contract import PaymentPipelineContract
        return PaymentPipelineContract(arm=goal["payment_pipeline_arm"])
    if goal is not None and goal["extension_decision"] in {"ADR0266", "ADR0271", "ADR0277"}:
        from cce_event_lane_contract import EventLaneContract
        from cce_status_refresh_reuse import selected as reuse_selected
        return EventLaneContract(status_refresh_reuse=reuse_selected(goal))
    if goal is not None and goal["extension_decision"] in {"ADR0259", "ADR0263"}:
        from cce_shared_worker_contract import worker_contract as shared_contract
        return shared_contract(dispatch_slots=16 if goal["extension_decision"] == "ADR0263" else 12)
    if goal is not None and goal["extension_decision"] in {"ADR0251", "ADR0255", "ADR0256"}:
        from cce_simulator_dispatch_profile import worker_contract as simulator_contract
        return simulator_contract(goal if goal["extension_decision"] == "ADR0251" else None)
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

    contract = __import__("cce_primary_bootstrap").configure(worker_contract(), config)

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

            import cce_transaction_profile as transaction
            goal = transaction.active()
            from cce_nginx_ingress import Route
            from cce_nginx_ingress import http_reader as ingress_http
            from cce_nginx_ingress import selected as ingress_selected
            ingress_route = None
            if ingress_selected(goal) and goal["ingress_route_arm"] == "candidate":
                ingress_route = Route(deployment,
                    lambda role, path: ingress_http(session, role, path),
                    lambda value: persist({"ingress_route": {**native_record.get("ingress_route", {}), **value}}))
                deployment.ingress_route = ingress_route
            transition_class = Transition
            transition_options = {}
            if goal is not None and goal["extension_decision"] in {"ADR0259", "ADR0271", "ADR0277"}:
                from cce_worker_transition import WorkerTransition
                transition_class = WorkerTransition

                def save_comparison(binding):
                    record["comparison_binding"] = binding
                    policy.write(output / "cce-comparison.private.json", record)

                def worker_metrics(ip):
                    return worker_metrics_reader(session, ip)

                transition_options = {"deployment": deployment, "service": service, "read_metrics": worker_metrics,
                                      "comparison_persist": save_comparison,
                                      "failure_persist": lambda value: policy.write(output / "worker-startup-failure.private.json", value)}
            transition = transition_class(
                session, guard, run_id, routes, owner, lambda value: persist({"transition": value}), **transition_options
            )

            from cce_backend_partition import selected as backend_selected
            if backend_selected(goal):
                from cce_backend_partition_runtime import Partition
                def save_partition(value):
                    transition.record["backend_partition"] = value
                    transition.checkpoint()
                transition.backend_partition = Partition(session,guard,run_id,cid,owner,save_partition)

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
                    for role in __import__("cce_primary_bootstrap").ecs_roles(config)
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
                identities={r: inventory["hosts"][r]["machine_id_sha256"] for r in __import__("cce_primary_bootstrap").ecs_roles(config)},
                background_before=background,
                persist=lambda value: persist({"lifecycle": value}),
                refresh_inventory=refresh_inventory,
                live_admission=goal is not None and goal["extension_decision"] in {"ADR0271", "ADR0277"},
                metric_admission=__import__("cce_metric_admission").selected(goal),
                ingress_route=ingress_route,
                early_hourly_observers=goal is not None and goal.get("hourly_observer_bootstrap_decision") == "ADR0294",
            )

            from cce_inventory_renewal import renew
            from cce_inventory_renewal import selected as renewal_selected
            if renewal_selected(goal):
                from cce_paid_observers import background_unchanged, receipt_bundle
                def renew_inventory(value, admitted):
                    def live_workers():
                        transition.verify_workers()
                        return transition.worker_runtime.bundle
                    return renew(value, run=run_id, binding=guard.binding,
                        native_before=receipt_bundle(run_id, admitted), workers_before=observers.native_workers,
                        observe_native=lambda: receipt_bundle(run_id, deployment.observe(
                            manifests, lambda ip: http_reader(session, ip, "/health/ready"),
                            lambda ip: http_reader(session, ip, "/metrics", metrics=True), previous=admitted)),
                        observe_workers=live_workers,
                        verify_background=lambda: background_unchanged(
                            lifecycle.background_before, session.call("primary", driver.INSPECT, 45)))
                observers.renew_inventory = renew_inventory

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
