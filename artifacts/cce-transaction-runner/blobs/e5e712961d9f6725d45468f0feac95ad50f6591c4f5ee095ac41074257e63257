"""ADR0228 native API and unchanged ECS background observations."""

import json
import math
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
        "manifest_digest": cce.dependency.MANIFEST,
        "receipts": receipts,
    }
    native.validate_receipts(value)
    return value


def background_spec(role, rows, instance_sha, receipts_sha):
    entries = []
    for row in rows:
        if not row["State"]["Running"]:
            continue
        labels = row["Config"].get("Labels", {})
        service = labels.get("com.docker.compose.service")
        if labels.get("codex-purpose") in {"cce-audit", "cce-pooler"}:
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
        helpers = (
            "observe_cce_paid_pipeline.py",
            "observe_two_host_pipeline.py",
            "prepare_two_host_scaling.py",
            "database_wait_evidence.py",
            "diagnostic_connection.py",
        )
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
        qualify_bound_inventory(self.contract, self.inventory, self.record, self.stage.output)
        self.upload("inventory.private.json", json.dumps(self.inventory))
        self.upload("native-receipts.json", json.dumps(self.native))
        observation_seconds = str(self.stage.profile.observer_seconds)
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
                self.directory + "/observe_cce_paid_pipeline.py",
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
                "--diagnostic-connection-bundle",
                self.directory + "/diagnostic.private.json",
                "--output",
                self.directory + "/pipeline.jsonl",
                *common,
                *(["--hourly"] if self.stage.profile.duration == 3600 else []),
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
        import time

        deadline = time.monotonic() + 30
        while True:
            status = self.session.api(
                self.stage.cid,
                "import json;from pathlib import Path;p=Path("
                + repr(self.directory)
                + ");print(json.dumps({n:json.loads((p/(n+'.jsonl')).read_text().splitlines()[0]) if (p/(n+'.jsonl')).exists() and (p/(n+'.jsonl')).stat().st_size else None for n in ('pipeline','kafka')}))",
                45,
            )
            if all(status.values()):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("Native observer startup exceeded bound")
            time.sleep(1)
        self.record["pipeline_startup"] = pipeline_startup_view(status["pipeline"], 6)
        self.record["kafka_startup"] = kafka_startup_view(status["kafka"], 6)
        if (
            not self.record["pipeline_startup"]["pass"]
            or not self.record["kafka_startup"]["pass"]
            or status["pipeline"].get("database_wait_diagnostics", {}).get("complete") is not True
        ):
            raise ValueError("Native observer/diagnostic startup failed before buyer dispatch")
        self.contract.verify_pipeline_startup(status["pipeline"])
        self.started = True
        self.checkpoint()

    def launch_cpu(self, start, snapshots, identities):
        self.stage.check(900)
        if not self.started or self.cpu_jobs:
            raise ValueError("Qualified fresh native observations required")
        self.start_utc = datetime.fromtimestamp(start, UTC).isoformat()
        self.end_utc = (datetime.fromtimestamp(start, UTC) + timedelta(seconds=self.stage.profile.duration)).isoformat()
        for role in ("primary", "secondary"):
            spec = background_spec(role, snapshots[role], identities[role], policy.digest(self.native))
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
        try:
            self.stage.stop_jobs()
        except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Retain bounded partial traces even when stop is unknown.
            failures.append({"operation": "stop_jobs", "type": type(error).__name__})
        from summarize_paid_kafka_lag import summarize as summarize_kafka
        from summarize_paid_pipeline import summarize as summarize_pipeline

        for name, summarize in (("pipeline", summarize_pipeline), ("kafka", summarize_kafka)):
            try:
                raw = copy_out(
                    self.session,
                    self.stage.cid,
                    self.directory + "/" + name + ".jsonl",
                    self.owner + "/" + name + ".jsonl",
                )
                path = self.stage.output / (name + ".jsonl")
                path.write_bytes(raw)
                rows = [json.loads(line) for line in raw.decode().splitlines() if line.strip()]
                self.record[name + "_summary"] = summarize(rows)
                if name == "pipeline":
                    from database_wait_evidence import summarize as summarize_waits

                    self.record["database_wait_capture"] = summarize_waits(path)
                    self.record["native_api_cpu"] = api_cpu(rows, self.native, self.start_utc, self.end_utc, hourly=self.stage.profile.duration == 3600)
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - One missing trace cannot skip the other.
                failures.append({"operation": name + "_trace", "type": type(error).__name__})
        self.record["collection_failures"] = failures
        self.record["observer_pass"] = (
            not failures
            and self.record.get("database_wait_capture", {}).get("complete") is True
            and pipeline_pass(self.record.get("pipeline_summary", {}))
            and kafka_pass(self.record.get("kafka_summary", {}))
        )
        self.checkpoint()
        if failures:
            raise ValueError("Native observation incomplete; original failure and available traces retained")
        return self.record

    def retain_startup_failure(self):
        """Keep bounded private diagnostics before the stopped helper is removed."""
        results = {}
        for label, name in (("pipeline", "observe_cce_paid_pipeline"), ("kafka", "kafka_lag_observe")):
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
