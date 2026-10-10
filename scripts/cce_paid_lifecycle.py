"""ADR0228 compose native stage components; entry-point registration is separate.

A caller must supply executed native safety/TTL qualification and prepared private
fixture inputs. This composition never turns a component check into a cloud permit.
"""

import copy
import time

import work_envelope as policy
from cce_ecs_transition import SECONDARY_INSPECT
from cce_paid_observers import background_unchanged
from qualify_two_host_deployment import INSPECT

SAFETY = (
    "all_four_pods_exercised",
    "exactly_one_hold",
    "one_durable_owner",
    "cross_pod_hold_replay",
    "cross_pod_payment_replay",
    "other_actor_denied",
    "customer_ticket_confirmed",
    "post_ttl_durable",
    "duplicate_callbacks_durable",
    "zero_double_booking",
    "full_queue_drain",
)


def require_safety(value):
    if (
        not isinstance(value, dict)
        or set(value.get("gates", {})) != set(SAFETY)
        or any(value["gates"][key] is not True for key in SAFETY)
    ):
        raise ValueError("Complete executed native safety and post-TTL evidence required")
    return True


class Lifecycle:
    def __init__(
        self,
        stage,
        resources,
        deployment,
        transition,
        observers,
        manifests,
        *,
        qualify_safety,
        cleanup_safety,
        prepare_fixture,
        identities,
        background_before,
        persist,
        refresh_inventory=None,
        live_admission=False,
    ):
        self.stage, self.session = stage, stage.session
        self.resources, self.deployment, self.transition = resources, deployment, transition
        self.observers, self.manifests = observers, manifests
        self.qualify_safety, self.prepare_fixture = qualify_safety, prepare_fixture
        self.cleanup_safety = cleanup_safety
        self.identities, self.background_before, self.persist = identities, background_before, persist
        self.live_admission = live_admission
        self.live_capture = None
        self.refresh_inventory = refresh_inventory
        self.record = {"pass": False, "cleanup_complete": False, "customers_dispatched": False}

    def checkpoint(self):
        self.persist(copy.deepcopy(self.record))
        self.stage.checkpoint()

    def capture_admission_diagnostics(self):
        """Retain bounded failure-time context without masking customer results."""
        try:
            if self.live_capture is not None:
                live = self.live_capture.finish()
                self.observers.slot_log_evidence = live
                policy.write(self.stage.output / "live-admission.private.json", live)
                self.record["live_admission_complete"] = live["complete"]
            evidence = self.deployment.admission_diagnostics()
            path = self.stage.output / "admission-failures.private.json"
            if path.exists() or path.is_symlink():
                raise ValueError("Admission evidence already exists")
            policy.write(path, evidence)
            self.record["admission_diagnostics"] = {
                "sha256": policy.digest(evidence),
                "all_pods_captured": evidence["all_pods_captured"],
                "failure_events": sum(p["failure_events"] for p in evidence["pods"].values()),
                "capture_incomplete": bool(evidence["errors"])
                or any(
                    p["byte_limit_reached"] or p["overflow_events"] or p["malformed_events"]
                    for p in evidence["pods"].values()
                ),
            }
        except Exception as error:  # noqa: BLE001 - Diagnosis cannot prevent financial verification and cleanup.
            self.record["admission_diagnostics"] = {
                "capture_error": type(error).__name__,
                "capture_incomplete": True,
            }
        self.checkpoint()

    def snapshots(self):
        rows = {
            role: self.session.call(role, program, 45)
            for role, program in (("primary", INSPECT), ("secondary", SECONDARY_INSPECT))
        }
        from cce_ecs_transition import execution_identity

        # Project discovery intentionally excludes non-Compose helper containers.
        # Add only the two separately captured owned identities, never all Docker.
        for role in getattr(self.resources, "names", {"audit": None, "bridge": None}):
            actual = self.resources.inspect_name(role)
            expected = self.resources.rows.get(role)
            if (
                len(actual) != 1
                or expected is None
                or not actual[0]["State"]["Running"]
                or execution_identity(actual[0]) != execution_identity(expected)
                or actual[0]["State"]["StartedAt"] != expected["State"]["StartedAt"]
            ):
                raise ValueError("Owned observation helper changed")
            if any(row["Id"] == actual[0]["Id"] for row in rows["primary"]):
                raise ValueError("Helper must not emulate a Compose project container")
            rows["primary"].append(actual[0])
        return rows

    def run(self, http_ready, http_metrics, global_audit):
        self.stage.check(self.stage.profile.duration + 900)
        if self.record.get("started"):
            raise ValueError("Fresh native lifecycle required; no ambiguous replay")
        if self.stage.cid != self.resources.record.get("audit", {}).get("container_id"):
            raise ValueError("The stage must use the captured owned audit helper")
        self.record["started"] = True
        self.checkpoint()
        original_error = None
        try:
            self.transition.capture()
            self.deployment.create(self.manifests)
            # The entry point must await Kubernetes readiness within its bounded guard
            # before supplying successful HTTP/readiness evidence; no customer retry.
            receipts = self.deployment.observe(self.manifests, http_ready, http_metrics)
            self.record["native_receipts"] = receipts
            if self.refresh_inventory is not None:
                self.record["transition_inventory_refresh"] = self.refresh_inventory()
                self.checkpoint()
            self.transition.activate(receipts)
            if hasattr(self.transition, "worker_runtime") and self.transition.worker_runtime is not None:
                self.observers.native_workers = self.transition.worker_runtime.bundle
                self.background_before = self.session.call("primary", INSPECT, 45)
            self.record["safety"] = self.qualify_safety(receipts)
            require_safety(self.record["safety"])
            fixture, manifest = self.prepare_fixture()
            self.stage.prepare(fixture, manifest)
            self.observers.start(receipts)
            if hasattr(self.transition, "verify_workers"):
                self.transition.verify_workers()
            before = self.snapshots()
            background_unchanged(self.background_before, before["primary"])
            # Re-observe immutable pod/process receipts immediately before scheduling.
            self.deployment.observe(self.manifests, http_ready, http_metrics, previous=receipts)
            start = time.time() + 30
            self.observers.launch_cpu(start, before, self.identities)
            admission = {
                "run": self.stage.run,
                "binding_sha256": policy.digest(self.stage.guard.binding),
                "cce_api_receipts": receipts,
                "all_required_pre_dispatch_gates_pass": True,
            }
            if self.live_admission:
                from cce_live_admission import Capture
                self.live_capture = Capture(self.deployment)
                self.live_capture.start()
            self.record["dispatch_attempted"] = True
            self.checkpoint()
            customer = self.stage.dispatch(admission, start_at_epoch=start)
            self.capture_admission_diagnostics()
            observed = self.observers.collect()
            financial = self.stage.audit(global_audit)
            self.deployment.observe(self.manifests, http_ready, http_metrics, previous=receipts)
            if hasattr(self.transition, "verify_workers"):
                self.transition.verify_workers()
            after = self.snapshots()
            background_unchanged(before["primary"], after["primary"])
            self.record["measurement_gates"] = {
                "customer_load": customer.get("pass") is True,
                "post_ttl_financial": financial.get("post_ttl_financial_pass") is True,
                "zero_double_booking": self.stage.record.get("financial", {}).get("pass") is True
                and self.stage.record.get("financial", {}).get("duplicate_booked_seats") == 0,
                "payment_durability": financial.get("post_ttl_financial_pass") is True,
                "full_queue_drain": financial.get("global_queues_pass") is True,
                "native_observer": observed.get("observer_pass") is True,
                "native_cpu": observed.get("native_api_cpu", {}).get("pass") is True,
                "unchanged_background": True,
                "unchanged_native_pods": True,
            }
            if self.live_admission:
                self.record["measurement_gates"]["live_admission"] = self.record.get("live_admission_complete") is True
            if self.stage.profile.duration == 3600:
                self.record["measurement_gates"]["hourly_issuance"] = self.stage.record.get("hourly_issuance", {}).get("pass") is True
            self.record["pass"] = all(self.record["measurement_gates"].values())
            self.checkpoint()
        except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Always restore independently owned resources.
            original_error = error
            self.record["failure_type"] = type(error).__name__
            try:
                import traceback

                path = self.stage.output / "original-lifecycle-failure.private.json"
                if path.exists() or path.is_symlink():
                    raise ValueError("Original failure evidence already exists")
                policy.write(
                    path, {"failure_type": type(error).__name__, "traceback": traceback.format_exc()[-65536:]}
                )
                self.record["original_failure_retained"] = True
            except Exception as capture_error:  # noqa: BLE001 - Failure capture cannot prevent owned cleanup.
                self.record["original_failure_capture_error"] = type(capture_error).__name__
            safe_messages = {
                "Qualified exactly bound native transition required",
                "Candidate API inventory changed",
                "Candidate API image/settings changed",
                "Owned load-balancer mount required",
                "Candidate route policy changed",
            }
            if str(error) in safe_messages:
                self.record["failure_reason"] = str(error)
        finally:
            self.record["customers_dispatched"] = self.stage.record.get("customers_dispatched") is True
            failures = []
            if (self.record["customers_dispatched"] or self.live_capture is not None) and "admission_diagnostics" not in self.record:
                self.capture_admission_diagnostics()
            # Restore the exact ECS candidate first: outer cleanup uses its captured CID.
            from run_two_host_paid_comparison import drain

            operations = (
                ("stop_jobs", self.stage.stop_jobs),
                ("safety_cleanup", self.cleanup_safety),
                ("candidate_restore", self.transition.restore),
                ("stage_cleanup", lambda: self.stage.cleanup(global_audit)),
                ("global_drain", lambda: drain(self.session, self.stage.cid, global_audit)),
                ("observer_cleanup", self.observers.cleanup),
            )
            for name, operation in operations:
                try:
                    self.record[name] = operation()
                except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - One failure cannot skip financial/queue verification.
                    failures.append({"operation": name, "type": type(error).__name__})
                self.checkpoint()
            # Unknown jobs keep audit/inputs and pods recoverable; never claim cleanup.
            if (
                self.stage.record.get("generator_idle_after_stop") is True
                and not self.stage.record.get("job_cleanup_errors")
                and not self.stage.record.get("recovery_identity_unknown")
            ):
                for name, operation in (
                    ("namespace_cleanup", self.deployment.cleanup),
                    ("resource_cleanup", self.resources.cleanup),
                ):
                    try:
                        self.record[name] = operation()
                    except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Attempt separately owned removals.
                        failures.append({"operation": name, "type": type(error).__name__})
                    self.checkpoint()
            else:
                failures.append({"operation": "resource_removal", "type": "UnknownJobOwnership"})
            self.record["cleanup_failures"] = failures
            self.record["cleanup_complete"] = not failures
            self.record["pass"] = self.record["pass"] and not failures
            self.checkpoint()
        if original_error is not None:
            raise original_error
        if not self.record["cleanup_complete"]:
            raise ValueError("Native lifecycle restoration or ownership requires recovery")
        return self.record
