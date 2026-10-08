"""ADR0228 reuse of the proven paid generator/jobs/financial audits on CCE.

This stage core is not a registered cloud runner. Physical topology, observers,
full safety qualification and outer ECS restoration must be integrated first.
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
        "scripts/cce_paid_stage.py",
        "scripts/cce_paid_resources.py",
        "scripts/cce_paid_lifecycle.py",
        "scripts/cce_paid_observers.py",
        "scripts/observe_cce_paid_pipeline.py",
        "scripts/observe_two_host_cpu.py",
        "scripts/observe_two_host_pipeline.py",
        "scripts/cce_paid_safety.py",
        "scripts/cce_api_adapter.py",
        "scripts/cce_ecs_transition.py",
        "scripts/run_two_host_paid_comparison.py",
        "scripts/run_synchronized_paid_generator.py",
        "scripts/prepare_generator_completion.py",
        "docs/capacity/cce/api-adapter-contract-2026-10-08.json",
    )
    return {p: sha((policy.ROOT / p).read_bytes()) for p in paths}


def generator_bundle():
    bundle = corrected_bundle(frozen_bundle())
    parent = policy.read(
        policy.ROOT / "docs/capacity/flash-sale-opening/generator-completion-probe-plan-2026-10-08.json"
    )
    helper = policy.ROOT / parent["isolated_source_directory"] / "scripts/checkout_journey_probe.py"
    raw = helper.read_bytes().replace(b"\r\n", b"\n")
    if raw != (policy.ROOT / "scripts/checkout_journey_probe.py").read_bytes().replace(b"\r\n", b"\n"):
        raise ValueError("Exact qualified polling helper required")
    return {**bundle, "scripts/checkout_journey_probe.py": raw}


def generator_arguments(directory, origin, start_at_epoch):
    if not re.fullmatch(r"/[^\x00\r\n]*?/tmp/adr0151-[0-9a-f]{12}-cce-candidate", directory):
        raise ValueError("Canonical owned generator directory required")
    if origin != "http://10.1.137.69:8000":
        raise ValueError("Existing private shared load-balancer origin required")
    if type(start_at_epoch) not in (int, float) or not math.isfinite(start_at_epoch):
        raise ValueError("Finite synchronized start required")
    return [
        "/root/http-load-venv/bin/python",
        directory + "/run_synchronized_paid_generator.py",
        "--frozen-generator",
        directory + "/paid_ticket_sharded_generator.py",
        "--start-at-epoch",
        str(start_at_epoch),
        "--manifest",
        directory + "/manifest.private.json",
        "--origin",
        origin,
        "--output",
        directory + "/customer.json",
        "--rate",
        "84",
        "--seconds",
        "300",
        "--completion-deadline-seconds",
        "420",
        "--concurrency",
        "500",
        "--http-client-count",
        "8",
        "--poll-seconds",
        "1",
        "--duplicates",
        "1",
        "--lifecycle-diagnostics",
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
            != (policy.ROOT / "scripts/checkout_journey_probe.py").read_bytes().replace(b"\r\n", b"\n")
        ):
            raise ValueError("Exact corrected 78-file workload bundle required")
        self.jobs = jobs or OwnedJobs()
        self.job_list = []
        self.record = {
            "arm": "candidate",
            "customers_dispatched": False,
            "pass": False,
            "capacity_stages_started": 0,
            "pinned_harness_files": 78,
        }
        self.gen = session.config["generator"]["repo"] + "/tmp/" + run + "-cce-candidate"
        self.origin = "http://10.1.137.69:8000"
        generator_arguments(self.gen, self.origin, 0)
        self.events = []
        self.prepared = False

    def check(self, timeout=0):
        if (
            self.guard is None
            or self.session.action_guard is not self.guard
            or self.guard.key.split("__")[0] != "bounded_cce_paid_comparison"
            or cce.PROFILE not in policy.PROFILES
            or cce.PROFILE not in policy.envelope()["qualified_profiles"]
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
            84,
            producer_sha256=sha(self.bundle["scripts/prepare_capacity_fixture.py"]),
        )
        self.events = list(fixture["show_ids"])
        expiry = datetime.fromisoformat(manifest["expires_at"])
        if (
            manifest.get("schema_version") != 1
            or manifest.get("environment") != "development"
            or manifest.get("origin") != self.origin
            or manifest.get("fixture_layout") != "distributed"
            or manifest.get("show_ids") != self.events
            or manifest.get("seat_offset") != 0
            or manifest.get("seats_per_show") != 300
            or len(manifest.get("viewer_tokens", [])) != 25200
            or len(set(manifest["viewer_tokens"])) != 25200
            or expiry.tzinfo is None
            or (expiry - datetime.now(UTC)).total_seconds() < 900
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
        for name in CORE:
            raw = self.bundle["scripts/" + name]
            self.session.put("generator", self.gen + "/" + name, raw.decode(), True)
            transferred[name] = sha(raw)
        raw = (policy.ROOT / "scripts/run_synchronized_paid_generator.py").read_bytes()
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
        self.check(900)
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
        arguments = generator_arguments(self.gen, self.origin, start)
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
        self.jobs.wait(self.session, job, "generator", 480, allowed_returncodes={0, 1})
        self.record["customer"] = json.loads(fetch(self.session, "generator", self.gen + "/customer.json"))
        self.checkpoint()
        return self.record["customer"]

    def audit(self, global_audit):
        """Mandatory even after lost response, failed SLO or generator failure."""
        if not self.record["customers_dispatched"]:
            return
        errors = []
        for name, operation in (
            (
                "financial",
                lambda: self.session.api(self.cid, financial_audit_program(self.events, 25200), 175),
            ),
            ("global_queues", lambda: drain(self.session, self.cid, global_audit)),
        ):
            try:
                self.record[name] = operation()
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Preserve independent mandatory recovery operations.
                self.record[name + "_failure_type"] = type(error).__name__
                errors.append(name)
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
                if receipt["retired_shows"] != 84:
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
for child in p.glob('paid-shards-*'):
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
