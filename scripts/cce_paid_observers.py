"""ADR0228 native API and unchanged ECS background observations."""

import json
import math
import time
from datetime import UTC, datetime, timedelta

import cce_api_adapter as cce
import observe_cce_paid_pipeline as native
import observe_two_host_cpu as cpu
import observe_two_host_pipeline as pipeline
import work_envelope as policy
from cce_ecs_transition import execution_identity
from run_two_host_paid_comparison import api_upload_runtime_owned as api_upload
from run_two_host_paid_comparison import copy_out, fetch, kafka_pass, pipeline_pass


def receipt_bundle(run, receipts):
    value = {
        "decision": "ADR0228",
        "run": run,
        "api_sources": cce.contract()["api_sources"],
        "resources": cce.contract()["resources"],
        "manifest_digest": cce.api_manifest(),
        "receipts": receipts,
    }
    native.validate_receipts(value)
    return value


def summarize_independent_diagnostics(record, rows, path, receipts, start, end, *, hourly, summarize_waits,
                                     summarize_slots=None, summarize_pipeline=None, native_workers=None, slot_log_evidence=None,
                                     diagnostic_path=None, metric_admission=False):
    """ADR0257/0260 preserve independent diagnostics despite a missing slot history."""
    failures = []
    operations = []
    if summarize_slots is not None:
        def slot_history():
            if metric_admission:
                if slot_log_evidence is not None:
                    raise ValueError("Metric admission cannot silently supplement missing history with logs")
                from cce_metric_admission import summarize
                return summarize(rows, receipts, start, end, hourly=hourly)
            admitted = native.validate_receipts(receipts)
            if slot_log_evidence is None:
                return summarize_slots(rows, admitted)
            from cce_live_admission import endpoints
            return summarize_slots(rows, admitted, extra_records=endpoints(slot_log_evidence, receipts))
        operations.append(("slot_failure_capture", slot_history))
    if summarize_pipeline is not None:
        from cce_writer_scale_verification import summary as declared_summary
        operations.append(("pipeline_summary", lambda: declared_summary(rows, native_workers, summarize_pipeline)))
    if native_workers is not None:
        from observe_cce_workers import summarize as summarize_workers
        operations.append(("native_worker_cpu", lambda: summarize_workers(rows, native_workers, start, end)))
    def database_capture():
        if diagnostic_path is None:
            return summarize_waits(path, hourly=hourly)
        from independent_database_sampling import summarize
        return summarize(diagnostic_path, start, end, hourly=hourly)
    operations.extend((
        ("database_wait_capture", database_capture),
        ("native_api_cpu", lambda: api_cpu(rows, receipts, start, end, hourly=hourly)),
    ))
    for operation, collect in operations:
        try:
            record[operation] = collect()
        except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Every independent diagnostic must be attempted.
            failure = {"operation": operation, "type": type(error).__name__}
            from cce_metric_admission import CaptureError
            if isinstance(error, CaptureError):
                failure["reason_code"] = error.reason_code
            failures.append(failure)
    return failures


def background_spec(role, rows, instance_sha, receipts_sha):
    entries = []
    for row in rows:
        if not row["State"]["Running"]:
            continue
        labels = row["Config"].get("Labels", {})
        service = labels.get("com.docker.compose.service")
        if labels.get("codex-purpose") in {"cce-audit", "cce-pooler", "cce-kafka"}:
            service = labels["codex-purpose"]
        if service is None:
            raise ValueError("Unknown active container in native measurement")
        entries.append(
            {
                "id": row["Id"],
                "role": service,
                "image_id": row["Image"],
                "project": labels.get("com.docker.compose.project"),
                "started_at": row["State"]["StartedAt"],
                "owner": labels.get("codex-owner"),
            }
        )
    spec = {
        "schema": 1,
        "arm": "candidate",
        "host_role": role,
        "placement": "cce-api-isolation",
        "decision": "ADR0228",
        "inventory_sha256": receipts_sha,
        "instance_uuid_sha256": instance_sha,
        "containers": entries,
    }
    if any(entry["role"] == "cce-kafka" for entry in entries):
        spec.update(placement="cce-worker-isolation", decision="ADR0259")
    if any(entry["role"] == "projection-consumer" for entry in entries):
        spec["event_lane_decision"] = "ADR0266"
    cpu.validate_spec(spec)
    return spec


def background_unchanged(before, after):
    """Compare actual background identities, never map Docker APIs to pods."""

    def relevant(rows):
        return {
            r["Id"]: {"execution": execution_identity(r), "started_at": r["State"]["StartedAt"]}
            for r in rows
            if r["State"]["Running"]
            and r["Config"].get("Labels", {}).get("com.docker.compose.service") in cpu.BACKGROUND_ROLES
        }

    prior, current = relevant(before), relevant(after)
    if not prior or prior != current:
        raise ValueError("Background deployment changed during CCE measurement")
    return True


def api_cpu(rows, receipts, start, end, *, hourly=False):
    """Strict offered-window CPU from native process counters, not node capacity."""
    labels = set(native.validate_receipts(receipts))
    distribution = pipeline.summarize_endpoint_distribution(
        rows, labels, offered_start_utc=start, offered_end_utc=end, hourly=hourly
    )
    if (
        distribution.get("all_four_replicas_observed") is not True
        or distribution.get("per_replica_traffic_distribution") is not True
    ):
        raise ValueError("Complete native traffic distribution required")
    left, right = cpu.timestamp(start), cpu.timestamp(end)
    seconds = (right - left).total_seconds()
    selected = sorted(rows, key=lambda r: cpu.timestamp(r["utc"]))
    first = max(
        (r for r in selected if cpu.timestamp(r["utc"]) <= left), key=lambda r: cpu.timestamp(r["utc"])
    )
    last = min(
        (r for r in selected if cpu.timestamp(r["utc"]) >= right), key=lambda r: cpu.timestamp(r["utc"])
    )
    # The coverage gate brackets within two seconds. Divide by actual counter interval.
    observed = (cpu.timestamp(last["utc"]) - cpu.timestamp(first["utc"])).total_seconds()
    cores = {}
    starts = native.validate_receipts(receipts)
    previous = {}
    for row in selected:
        at = cpu.timestamp(row["utc"])
        if at < cpu.timestamp(first["utc"]) or at > cpu.timestamp(last["utc"]):
            continue
        for address in labels:
            metrics = row["api_replicas"][address]
            value = metrics.get("process_cpu_seconds_total")
            if (
                metrics.get("process_start_time_seconds") != starts[address]
                or type(value) not in (int, float)
                or not math.isfinite(value)
                or value < 0
                or value < previous.get(address, 0)
            ):
                raise ValueError("Native CPU identity or counter reset")
            previous[address] = value
    for address in labels:
        delta = (
            last["api_replicas"][address]["process_cpu_seconds_total"]
            - first["api_replicas"][address]["process_cpu_seconds_total"]
        )
        if not math.isfinite(delta) or delta < 0 or observed <= 0:
            raise ValueError("Invalid native process CPU interval")
        cores[address] = delta / observed
    return {
        "pass": True,
        "scope": "API process CPU; CCE node CPU is not measured",
        "offered_seconds": seconds,
        "counter_interval_seconds": observed,
        "api_cpu_cores_by_endpoint": cores,
        "aggregate_api_cpu_cores": sum(cores.values()),
        "distribution": distribution,
    }


class Observers:
    """Use the original bounded jobs and diagnostics with a separate native receipt."""

    def __init__(self, stage, inventory, contract, diagnostic_context, owner):
        self.stage, self.session = stage, stage.session
        self.inventory, self.contract = inventory, contract
        self.context, self.owner = diagnostic_context, owner + "/cce-observers"
        self.directory = "/tmp/" + stage.run + "/cce-observers"
        self.record = {}
        self.diagnostic_attempted = False
        self.started = False
        self.cpu_jobs = []
        self.native = None

    def checkpoint(self):
        self.stage.record["observation"] = self.record
        self.stage.checkpoint()

    def upload(self, name, content):
        api_upload(self.session, self.stage.cid, self.owner, self.directory, name, content)

    def start(self, receipts):
        from diagnostic_runner_connection import prepare, qualify_bound_inventory
        from run_two_host_paid_comparison import kafka_startup_view, pipeline_startup_view

        self.stage.check(900)
        if self.started:
            raise ValueError("Fresh observer lifecycle required")
        self.native = receipt_bundle(self.stage.run, receipts)
        self.record["observer_directory_attempted"] = True
        self.checkpoint()
        self.session.call(
            "primary",
            "from pathlib import Path;import json;p=Path("
            + repr(self.owner)
            + ");p.mkdir(mode=0o700);print(json.dumps({'fresh':True}))",
            45,
        )
        self.session.api(
            self.stage.cid,
            "from pathlib import Path;import json;p=Path("
            + repr(self.directory)
            + ");p.mkdir(mode=0o700,parents=True,exist_ok=False);print(json.dumps({'fresh':True}))",
            45,
        )
        import cce_transaction_profile as transaction
        goal = transaction.active()
        extended = goal is not None
        from cce_verification_contract import contract as verification_contract
        from cce_verification_contract import selected as verification_selected
        self.independent_database_sampling = verification_selected(goal)
        if self.independent_database_sampling:
            self.inventory["verification_contract"] = verification_contract()
            self.record["independent_database_sampling"] = True
        self.event_lanes = extended and transaction.active()["extension_decision"] in {"ADR0266", "ADR0271", "ADR0277"}
        from cce_runtime_cohort_profile import selected as cohort_selected
        self.runtime_cohort = None
        if cohort_selected(goal):
            from paid_cohort_query import runtime_contract
            self.runtime_cohort = runtime_contract(goal["runtime_cohort_arm"])
            if self.stage.guard.binding.get("cce_runtime_cohort_contract") != self.runtime_cohort:
                raise ValueError("Observer query differs from reserved identity")
            self.record["runtime_cohort_contract"] = self.runtime_cohort
        helpers = (
            "cce_primary_bootstrap.py",
            *(("paid_cohort_query.py",) if self.runtime_cohort is not None else ()),
            "observe_cce_paid_pipeline.py",
            "observe_two_host_pipeline.py",
            "prepare_two_host_scaling.py",
            "database_wait_evidence.py",
            "diagnostic_connection.py",
            "cce_verification_contract.py",
            *(("independent_database_sampling.py",) if self.independent_database_sampling else ()),
            *(("cce_shared_worker_image_identity.py", "cce_worker_identity.py", "observe_cce_workers.py") if extended and transaction.active()["extension_decision"] in {"ADR0259", "ADR0263", "ADR0271", "ADR0277"} else ()),
            *(("observe_slot_paid_pipeline.py", "slot_failure_evidence.py", "bounded_trace_transport.py") if extended else ()),
        )
        if self.event_lanes:
            helpers += ("cce_event_lane_identity.py", "observe_projection_kafka.py", "cce_status_refresh_reuse.py")
        from cce_payment_pipeline_profile import selected as pipeline_selected
        if pipeline_selected(goal):
            helpers += ("cce_payment_pipeline_identity.py",)
        from cce_committed_status_profile import selected as committed_selected
        if committed_selected(goal):
            helpers += ("cce_committed_status_identity.py",)
            from cce_measured_payment_context_profile import selected as context_selected
            if context_selected(goal):
                helpers += ("cce_measured_payment_context_identity.py",)
            from cce_writer_sale_context_profile import selected as writer_context_selected
            if writer_context_selected(goal):
                helpers += ("cce_writer_sale_context_identity.py",)
            from cce_fulfillment_batch_profile import selected as fulfillment_selected
            if fulfillment_selected(goal):
                helpers += ("cce_fulfillment_batch_identity.py", "cce_explicit_start_identity.py", "cce_notification_dedup_identity.py")
            from cce_explicit_start_profile import selected as explicit_selected
            if explicit_selected(goal):
                helpers += ("cce_explicit_start_identity.py", "cce_notification_dedup_identity.py")
            from cce_notification_dedup_profile import selected as notification_selected
            if notification_selected(goal):
                helpers += ("cce_notification_dedup_identity.py",)
        from cce_backend_partition import contract as partition_contract
        from cce_backend_partition import selected as partition_selected
        if partition_selected(goal):
            helpers += ("cce_backend_partition.py","cce_backend_partition_runtime.py")
            self.backend_partition = partition_contract(self.stage.guard.binding["cce_backend_partition_contract"]["decision"])
            self.upload("backend-partition.json",json.dumps(self.backend_partition))
        from cce_simulator_roundtrip_profile import selected as roundtrip_selected
        if roundtrip_selected(goal):
            helpers += ("cce_combined_worker_identity.py",)
        expected = {}
        for name in helpers:
            raw = (policy.ROOT / "scripts" / name).read_text()
            self.upload(name, raw)
            from cce_paid_stage import sha

            expected[name] = sha(raw.encode())
        proof = self.session.api(
            self.stage.cid,
            "import hashlib,json;from pathlib import Path;p=Path("
            + repr(self.directory)
            + ");expected="
            + repr(expected)
            + ";assert {k:hashlib.sha256((p/k).read_bytes().replace(b'\\r\\n',b'\\n')).hexdigest() for k in expected}==expected;print(json.dumps({'native_helpers_verified':True}))",
            45,
        )
        if proof != {"native_helpers_verified": True}:
            raise ValueError("Transferred native observer changed")
        self.record.update(proof)
        self.diagnostic_attempted = True
        self.checkpoint()

        def verified_upload(session, cid, owner, directory, name, content):
            # Supporting helpers were all transferred and checked above. The
            # diagnostic preflight rechecks the full helper map before use.
            from cce_paid_stage import sha

            if name in expected:
                if (
                    session is not self.session
                    or cid != self.stage.cid
                    or owner != self.owner
                    or directory != self.directory
                    or sha(content.encode()) != expected[name]
                ):
                    raise ValueError("Repeated supporting helper transfer differs")
                return directory + "/" + name
            return api_upload(session, cid, owner, directory, name, content)

        self.record.update(
            prepare(
                self.session,
                self.stage.cid,
                self.owner,
                self.directory,
                self.inventory,
                verified_upload,
                self.context,
            )
        )
        if getattr(self, "native_workers", None) is not None:
            # Keep immutable worker proofs even if renewal rejects before upload.
            policy.write(self.stage.output / "native-workers.private.json", self.native_workers)
        if getattr(self, "renew_inventory", None) is not None:
            policy.write(self.stage.output / "observer-original-inventory.private.json", self.inventory)
            self.inventory, self.record["inventory_renewal"] = self.renew_inventory(self.inventory, receipts)
            self.checkpoint()
        qualify_bound_inventory(self.contract, self.inventory, self.record, self.stage.output)
        self.upload("inventory.private.json", json.dumps(self.inventory))
        self.upload("native-receipts.json", json.dumps(self.native))
        if self.runtime_cohort is not None:
            self.upload("runtime-cohort-contract.json", json.dumps(self.runtime_cohort))
        if getattr(self, "native_workers", None) is not None:
            self.upload("native-workers.json", json.dumps(self.native_workers))
        early = getattr(self, "hourly_early_bootstrap", False)
        observation_seconds = str(4200 if early else self.stage.profile.observer_seconds)
        if early:
            self.record["hourly_observer_bootstrap_decision"] = "ADR0294"
            self.record["observer_seconds"] = 4200
        common = [
            "--manifest",
            self.directory + "/manifest.private.json",
            "--seconds",
            observation_seconds,
            "--interval",
            "1",
        ]
        # An observer-only copy remains owned until all jobs stop.
        self.upload("manifest.private.json", json.dumps({k: self.stage.manifest[k] for k in ("environment", "show_ids")}))
        commands = {
            "pipeline": [
                "python",
                self.directory + ("/observe_slot_paid_pipeline.py" if extended else "/observe_cce_paid_pipeline.py"),
                *(["--runtime-cohort-contract", self.directory + "/runtime-cohort-contract.json",
                   "--runtime-cohort-contract-sha256", policy.digest(self.runtime_cohort)]
                  if self.runtime_cohort is not None else []),
                "--native-receipts",
                self.directory + "/native-receipts.json",
                "--native-receipts-sha256",
                policy.digest(self.native),
                "--inventory",
                self.directory + "/inventory.private.json",
                "--approved-inventory-sha256",
                policy.digest(self.inventory),
                "--frozen-observer",
                "/app/scripts/observe_paid_pipeline.py",
                "--database-wait-diagnostics",
                *(["--independent-database-sampling"] if self.independent_database_sampling else []),
                "--diagnostic-connection-bundle",
                self.directory + "/diagnostic.private.json",
                "--output",
                self.directory + "/pipeline.jsonl",
                *common,
                *(["--backend-partition-contract",self.directory+"/backend-partition.json",
                   "--backend-partition-contract-sha256",policy.digest(self.backend_partition)]
                  if hasattr(self,"backend_partition") else []),
                *(["--native-workers", self.directory + "/native-workers.json", "--native-workers-sha256", policy.digest(self.native_workers)] if getattr(self, "native_workers", None) is not None else []),
                *(["--hourly"] if self.stage.profile.duration == 3600 else []),
                *(["--early-hourly-bootstrap"] if early else []),
            ],
            "kafka": [
                "python",
                "/app/scripts/kafka_lag_observe.py",
                "--output",
                self.directory + "/kafka.jsonl",
                "--seconds",
                observation_seconds,
                "--interval",
                "1",
                "--backend",
                "python",
            ],
        }
        if self.event_lanes:
            commands["projection-kafka"] = ["python", self.directory + "/observe_projection_kafka.py",
                "--output", self.directory + "/projection-kafka.jsonl", "--seconds", observation_seconds,
                "--interval", "1", "--backend", "python", "--group", "ticketing-seat-projection-v1"]
        self.collector_started_monotonic = time.monotonic()
        for name, arguments in commands.items():
            self.record[name + "_launch_attempted"] = True
            self.checkpoint()
            self.record[name + "_job"] = self.stage.jobs.launch(
                self.session,
                "container",
                self.stage.cid,
                arguments,
                self.directory,
                self.stage.job_list,
                self.stage.record,
                database=True,
            )
        names = tuple(commands)
        deadline = time.monotonic() + 30
        while True:
            status = self.session.api(
                self.stage.cid,
                "import json;from pathlib import Path;p=Path("
                + repr(self.directory)
                + ");print(json.dumps({n:json.loads((p/(n+'.jsonl')).read_text().splitlines()[0]) if (p/(n+'.jsonl')).exists() and (p/(n+'.jsonl')).stat().st_size else None for n in " + (repr(names) if self.event_lanes else "('pipeline','kafka')") + "}))",
                45,
            )
            if all(status.values()):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("Native observer startup exceeded bound")
            time.sleep(1)
        if self.runtime_cohort is not None and status["pipeline"].get("runtime_cohort_contract") != self.runtime_cohort:
            raise ValueError("Runtime observer query receipt missing before dispatch")
        from cce_writer_scale_verification import startup as declared_startup
        policy.write(self.stage.output / "pipeline-startup-row.private.json", status["pipeline"])
        self.record["pipeline_startup"] = declared_startup(
            status["pipeline"], getattr(self, "native_workers", None), pipeline_startup_view)
        self.record["kafka_startup"] = kafka_startup_view(status["kafka"], __import__("cce_fixed_worker_allocation").kafka_members(getattr(self, "native_workers", None)))
        if (
            not self.record["pipeline_startup"]["pass"]
            or not self.record["kafka_startup"]["pass"]
            or status["pipeline"].get("database_wait_diagnostics", {}).get("complete") is not True
        ):
            raise ValueError("Native observer/diagnostic startup failed before buyer dispatch")
        if self.event_lanes:
            row = status["projection-kafka"]
            self.record["projection_kafka_startup"] = kafka_startup_view(row, 1)
            if (not self.record["projection_kafka_startup"]["pass"] or row.get("assigned_partitions") != 6
                    or row.get("group") != "ticketing-seat-projection-v1"
                    or sorted(p["partition"] for p in row.get("partitions", [])) != list(range(6))):
                raise ValueError("Projection ownership incomplete before dispatch")
        self.contract.verify_pipeline_startup(status["pipeline"])
        self.started = True
        self.checkpoint()

    def ensure_offered_coverage(self, start):
        if not getattr(self, "hourly_early_bootstrap", False) or not self.started:
            raise ValueError("Qualified early hourly observers required")
        elapsed = time.monotonic() - self.collector_started_monotonic
        delay = start - time.time()
        required = elapsed + delay + self.stage.profile.duration + 2
        self.record["early_observer_coverage"] = {
            "elapsed_seconds": elapsed, "scheduled_delay_seconds": delay,
            "required_seconds": required, "observer_seconds": 4200,
            "pass": 0 <= elapsed and 0 <= delay and required <= 4200}
        self.checkpoint()
        if not self.record["early_observer_coverage"]["pass"]:
            raise ValueError("Early observers cannot cover complete offered hour")
        return True

    def launch_cpu(self, start, snapshots, identities):
        self.stage.check(900)
        if not self.started or self.cpu_jobs:
            raise ValueError("Qualified fresh native observations required")
        self.start_utc = datetime.fromtimestamp(start, UTC).isoformat()
        self.end_utc = (datetime.fromtimestamp(start, UTC) + timedelta(seconds=self.stage.profile.duration)).isoformat()
        for role in __import__("cce_primary_bootstrap").ecs_roles(getattr(self.session,"config",{})):
            spec = background_spec(role, snapshots[role], identities[role], policy.digest(self.native))
            if getattr(self, "native_workers", None) is not None:
                spec.update(placement="cce-worker-isolation", decision="ADR0259")
                cpu.validate_spec(spec)
            location = (
                self.session.config["primary"]["repo"] + "/tmp/"
                if role == "primary"
                else self.session.config["secondary"]["prepared_directory"] + "/"
            ) + self.stage.run + "-cce-cpu"
            self.session.call(
                role,
                "from pathlib import Path;import json;p=Path("
                + repr(location)
                + ");p.mkdir(mode=0o700);print(json.dumps({'fresh':True}))",
                45,
            )
            self.session.put(role, location + "/cpu-spec.json", json.dumps(spec), True)
            self.session.put(
                role,
                location + "/observe_two_host_cpu.py",
                (policy.ROOT / "scripts/observe_two_host_cpu.py").read_text(),
                True,
            )
            from cce_paid_stage import sha

            expected = {
                "observe_two_host_cpu.py": sha((policy.ROOT / "scripts/observe_two_host_cpu.py").read_bytes())
            }
            proof = self.session.call(
                role,
                "import hashlib,importlib.util,json;from pathlib import Path;p=Path("
                + repr(location)
                + ");expected="
                + repr(expected)
                + ";assert {k:hashlib.sha256((p/k).read_bytes().replace(b'\\r\\n',b'\\n')).hexdigest() for k in expected}==expected;"
                + "value=json.loads((p/'cpu-spec.json').read_text());assert hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()=="
                + repr(policy.digest(spec))
                + ";assert hashlib.sha256(Path('/sys/class/dmi/id/product_uuid').read_text().strip().lower().encode()).hexdigest()==value['instance_uuid_sha256'];"
                + "m=importlib.util.spec_from_file_location('owned_cpu',p/'observe_two_host_cpu.py');module=importlib.util.module_from_spec(m);m.loader.exec_module(module);"
                + "module.inspect_containers(module.validate_spec(value));print(json.dumps({'cpu_preflight_verified':True}))",
                45,
            )
            if proof != {"cpu_preflight_verified": True}:
                raise ValueError("Native CPU source, host or container preflight failed")
            self.record[role + "_cpu_preflight_verified"] = True
            self.checkpoint()
            args = [
                "python3",
                location + "/observe_two_host_cpu.py",
                "--spec",
                location + "/cpu-spec.json",
                "--output",
                location + "/cpu.json",
                "--start-at",
                self.start_utc,
                "--seconds",
                str(self.stage.profile.duration),
                *(["--hourly"] if self.stage.profile.duration == 3600 else []),
            ]
            job = self.stage.jobs.launch(
                self.session, role, self.stage.cid, args, location, self.stage.job_list, self.stage.record
            )
            self.cpu_jobs.append((role, job, location))
        self.checkpoint()

    def collect(self):
        failures = []
        for role, job, location in self.cpu_jobs:
            try:
                self.stage.jobs.wait(self.session, job, role, self.stage.profile.duration + 65)
                self.record[role + "_cpu"] = cpu.summarize(
                    json.loads(fetch(self.session, role, location + "/cpu.json"))
                )
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Preserve both hosts and traces independently.
                failures.append({"operation": role + "_cpu", "type": type(error).__name__})
        try:
            if __import__("cce_primary_bootstrap").enabled(getattr(self.session,"config",{})):
                self.record["cpu_window"] = __import__("cce_primary_bootstrap").cpu_window(
                    self.record["primary_cpu"], offered_start_utc=self.start_utc,
                    offered_end_utc=self.end_utc, hourly=self.stage.profile.duration == 3600)
            else:
                self.record["cpu_window"] = cpu.compare_windows(
                    self.record["primary_cpu"],
                    self.record["secondary_cpu"],
                    offered_start_utc=self.start_utc,
                    offered_end_utc=self.end_utc,
                    hourly=self.stage.profile.duration == 3600,
                )
            # Native API CPU is reported independently, rather than invented for ECS.
            self.record["cpu_window"].pop("aggregate_api_cpu_cores", None)
        except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Never infer successful host coverage.
            failures.append({"operation": "cpu_window", "type": type(error).__name__})
        stopped = False
        try:
            self.stage.stop_jobs()
            stopped = True
        except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Retain bounded partial traces even when stop is unknown.
            failures.append({"operation": "stop_jobs", "type": type(error).__name__})
        from summarize_paid_kafka_lag import summarize as summarize_kafka
        from summarize_paid_pipeline import summarize as summarize_pipeline

        traces = [("pipeline", summarize_pipeline), ("kafka", summarize_kafka)]
        if getattr(self, "independent_database_sampling", False):
            traces.insert(0, ("database-waits", None))
        if getattr(self, "event_lanes", False):
            traces.append(("projection-kafka", summarize_kafka))
        for name, summarize in traces:
            try:
                import cce_transaction_profile as transaction
                if transaction.active() is not None:
                    if not stopped:
                        raise ValueError("Stopped observer identity required before compression")
                    from cce_slot_trace_transport import collect
                    raw, transport = collect(self.stage, self.directory, self.owner, name, copy_out)
                    self.record[name + "_transport"] = transport
                else:
                    raw = copy_out(
                        self.session, self.stage.cid, self.directory + "/" + name + ".jsonl",
                        self.owner + "/" + name + ".jsonl",
                    )
                path = self.stage.output / (name + ".jsonl")
                path.write_bytes(raw)
                rows = [json.loads(line) for line in raw.decode().splitlines() if line.strip()]
                if name == "database-waits":
                    self.record["database_wait_trace_sha256"] = policy.digest(rows)
                    continue
                if name == "pipeline":
                    contract = getattr(self, "runtime_cohort", None)
                    if contract is not None:
                        if not rows or any(row.get("runtime_cohort_contract") != contract for row in rows):
                            raise ValueError("Runtime cohort query identity missing or changed")
                        self.record["runtime_cohort_receipts_complete"] = True
                    from database_wait_evidence import summarize as summarize_waits
                    from slot_failure_evidence import summarize as summarize_slots
                    failures.extend(summarize_independent_diagnostics(
                        self.record, rows, path, self.native, self.start_utc, self.end_utc,
                        hourly=self.stage.profile.duration == 3600, summarize_waits=summarize_waits,
                        summarize_slots=summarize_slots if transaction.active() is not None else None,
                        summarize_pipeline=summarize, native_workers=getattr(self, "native_workers", None),
                        slot_log_evidence=getattr(self, "slot_log_evidence", None),
                        metric_admission=getattr(self, "metric_admission", False),
                        diagnostic_path=(self.stage.output / "database-waits.jsonl"
                            if getattr(self, "independent_database_sampling", False) else None)))
                else:
                    self.record[name + "_summary"] = summarize(rows)
                    if name == "projection-kafka":
                        from cce_event_lane_identity import projection_summary_pass
                        self.record["projection_kafka_pass"] = (projection_summary_pass(self.record[name + "_summary"])
                            and all(row.get("group") == "ticketing-seat-projection-v1" for row in rows)
                            and len(rows) >= self.stage.profile.duration
                            and datetime.fromisoformat(rows[0]["utc"]) <= datetime.fromisoformat(self.start_utc)
                            and datetime.fromisoformat(rows[-1]["utc"]) >= datetime.fromisoformat(self.end_utc))

            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - One missing trace cannot skip the other.
                failures.append({"operation": name + "_trace", "type": type(error).__name__})
        self.record["collection_failures"] = failures
        self.record["observer_pass"] = (
            not failures
            and self.record.get("database_wait_capture", {}).get("complete") is True
            and __import__("cce_writer_scale_verification").pipeline(
                self.record.get("pipeline_summary", {}), getattr(self, "native_workers", None), pipeline_pass)
            and __import__("cce_fixed_worker_allocation").kafka_pass(
                self.record.get("kafka_summary", {}), getattr(self, "native_workers", None), kafka_pass)
            and (not getattr(self, "event_lanes", False) or self.record.get("projection_kafka_pass") is True)
        )
        self.checkpoint()
        if failures:
            raise ValueError("Native observation incomplete; original failure and available traces retained")
        return self.record

    def retain_startup_failure(self):
        """Keep bounded private diagnostics before the stopped helper is removed."""
        results = {}
        import cce_transaction_profile as transaction
        helper = "observe_slot_paid_pipeline" if transaction.active() is not None else "observe_cce_paid_pipeline"
        startup_jobs = [("pipeline", helper), ("kafka", "kafka_lag_observe")]
        if getattr(self, "event_lanes", False):
            startup_jobs.append(("projection-kafka", "observe_projection_kafka"))
        for label, name in startup_jobs:
            job = self.record.get(label + "_job")
            if job is None:
                continue
            try:
                if job.get("identity_path") != self.directory or job.get("name") != name:
                    raise ValueError("Foreign observer job receipt")
                code = (
                    "import base64,json;from pathlib import Path;p=Path("
                    + repr(self.directory)
                    + ");job=" + repr(job) + ";name=" + repr(name) + "\n"
                    + "assert not p.is_symlink()\n"
                    + "identity=p/('job-'+name+'-identity.json')\n"
                    + "assert not identity.is_symlink() and identity.stat().st_nlink==1\n"
                    + "assert json.loads(identity.read_text())==job\n"
                    + "result={'log_present':False,'exit_receipt':None}\n"
                    + "log=p/('job-'+name+'.log')\n"
                    + "if log.exists():\n"
                    + " assert not log.is_symlink() and log.is_file() and log.stat().st_nlink==1\n"
                    + " size=log.stat().st_size\n"
                    + " with log.open('rb') as stream:stream.seek(max(0,size-65536));raw=stream.read(65536)\n"
                    + " result.update(log_present=True,total_bytes=size,truncated=size>65536,log_tail_base64=base64.b64encode(raw).decode())\n"
                    + "exit_file=p/('job-'+name+'-exit.json')\n"
                    + "if exit_file.exists():\n"
                    + " assert not exit_file.is_symlink() and exit_file.stat().st_nlink==1 and exit_file.stat().st_size<=4096\n"
                    + " result['exit_receipt']=json.loads(exit_file.read_text())\n"
                    + "print(json.dumps(result))"
                )
                evidence = self.session.api(self.stage.cid, code, 45)
                path = self.stage.output / (label + "-startup.private.json")
                if path.exists() or path.is_symlink():
                    raise ValueError("Startup failure evidence already exists")
                policy.write(path, evidence)
                results[label] = {"retained": True, "log_present": evidence.get("log_present") is True}
            except Exception as error:  # noqa: BLE001 - Diagnostics cannot skip mandatory cleanup.
                results[label] = {"retained": False, "failure_type": type(error).__name__}
        self.record["startup_failure_capture"] = results
        self.checkpoint()
        return results

    def cleanup(self):
        from diagnostic_runner_connection import cleanup

        self.session.begin_cleanup()
        # Keep inputs for recovery if an observer or generator may still be running.
        self.stage.stop_jobs()
        if not self.started:
            self.retain_startup_failure()
        if self.diagnostic_attempted:
            self.record.update(cleanup(self.session, self.stage.cid, self.owner, self.directory))
        if self.record.get("observer_directory_attempted"):
            result = self.session.api(
                self.stage.cid,
                "from pathlib import Path;import json;p=Path("
                + repr(self.directory)
                + ");f=p/'manifest.private.json';assert not p.is_symlink() and not f.is_symlink();f.unlink(missing_ok=True);print(json.dumps({'observer_manifest_removed':not f.exists()}))",
                45,
            )
            if result.get("observer_manifest_removed") is not True:
                raise ValueError("Observer manifest cleanup unverified")
            self.record.update(result)
        if self.record.get("observer_directory_attempted"):
            host_receipt = self.session.call(
                "primary",
                "from pathlib import Path;import json;p=Path("
                + repr(self.owner)
                + ");f=p/'manifest.private.json'\nif p.is_symlink() or not p.is_dir() or f.is_symlink():raise ValueError('Unknown observer secret path')\nif f.exists():\n if not f.is_file() or f.stat().st_nlink!=1:raise ValueError('Unknown observer secret ownership')\n f.unlink()\nprint(json.dumps({'observer_host_manifest_removed':not f.exists()}))",
                45,
            )
            if host_receipt.get("observer_host_manifest_removed") is not True:
                raise ValueError("Observer host manifest cleanup unverified")
            self.record.update(host_receipt)
        self.checkpoint()
        return self.record
