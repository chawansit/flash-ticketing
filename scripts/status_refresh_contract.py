"""ADR0151 fixed-factor contract; preparation never connects to cloud services."""

import copy
import hashlib
import json
import re
import subprocess
from datetime import UTC, datetime

from prepare_two_host_scaling import API_SETTINGS, BACKGROUND, REVISION
from qualify_two_host_deployment import IMAGE, ROOT
from runtime_source_identity import source_identity_program

PLAN = ROOT / "docs/capacity/flash-sale-opening/order-status-event-refresh-comparison-plan-2026-10-05.json"
LOCAL = ROOT / "docs/capacity/flash-sale-opening/order-status-event-refresh-local-validation-2026-10-05.json"
ROLES = ("api", *BACKGROUND)
LABEL = "org.flash-ticketing.status-refresh.source-manifest"


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def source_contract():
    plan, report = json.loads(PLAN.read_text()), json.loads(LOCAL.read_text())
    if plan["base_revision"] != REVISION or report["isolated_candidate"]["frozen_base_revision"] != REVISION:
        raise ValueError("Frozen isolated revision required")
    source = ROOT / plan["isolated_source_directory"]
    if not source.resolve().is_relative_to((ROOT / "tmp").resolve()):
        raise ValueError("Owned isolated source required")
    files = subprocess.check_output(
        ["git", "ls-tree", "-r", "--name-only", REVISION, "src/ticketing"], cwd=ROOT, text=True, timeout=15
    ).splitlines()
    paths = {p for p in files if p.endswith(".py")} | {
        "src/ticketing/infrastructure/order_status_projector.py"
    }
    actual_paths = {p.relative_to(source).as_posix() for p in (source / "src/ticketing").rglob("*.py")}
    if paths != actual_paths:
        raise ValueError("Missing or extra isolated runtime modules")
    changes = report["isolated_candidate"]["changed_source_sha256"]
    expected = {}
    for path in sorted(paths):
        if path in changes:
            expected[path] = changes[path]
        else:
            original = subprocess.check_output(["git", "show", REVISION + ":" + path], cwd=ROOT, timeout=10)
            expected[path] = hashlib.sha256(original.replace(b"\r\n", b"\n")).hexdigest()
        if hashlib.sha256((source / path).read_bytes().replace(b"\r\n", b"\n")).hexdigest() != expected[path]:
            raise ValueError("Isolated runtime drift")
    if any(expected.get(k) != v for k, v in plan["expected_runtime_source_sha256"].items()):
        raise ValueError("Recorded runtime proof differs")
    source_identity_program(expected)  # Bound paths/hash count before constructing any remote program.
    return expected


class StatusRefreshContract:
    def __init__(self, artifact, arm, sources):
        if arm not in {"control", "candidate"}:
            raise ValueError("Only off/on arms allowed")
        source_identity_program(sources)
        self.arm, self.sources = arm, copy.deepcopy(sources)
        self.source_manifest = digest({"base_revision": REVISION, "runtime_source_sha256": self.sources})
        if (
            not isinstance(artifact, dict)
            or set(artifact) != {"images", "parent_images", "source_manifest_sha256"}
            or artifact["source_manifest_sha256"] != self.source_manifest
            or not isinstance(artifact["images"], dict)
            or not isinstance(artifact["parent_images"], dict)
            or set(artifact["images"]) != set(ROLES)
            or set(artifact["parent_images"]) != set(ROLES)
        ):
            raise ValueError("Exact per-role isolated artifact receipt required")
        for values in (artifact["images"], artifact["parent_images"]):
            if any(
                not isinstance(v, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", v) for v in values.values()
            ):
                raise ValueError("Immutable per-role image IDs required")
        if artifact["parent_images"]["api"] != IMAGE:
            raise ValueError("Frozen API parent required")
        self.images, self.parents = (
            copy.deepcopy(artifact["images"]),
            copy.deepcopy(artifact["parent_images"]),
        )
        self.api_settings = {**API_SETTINGS, "ORDER_STATUS_CACHE_MS": "1000"}

    def qualify_inventory(self, inventory, *, now=None):
        from prepare_two_host_scaling import validate_inventory

        now = now or datetime.now(UTC)
        validate_inventory(inventory, image_id=self.images["api"], contract=self, now=now)
        if inventory["arm"] != self.arm:
            raise ValueError("Qualification arm differs")
        return {
            "schema": 1,
            "arm": self.arm,
            "api_image_id": self.images["api"],
            "source_manifest_sha256": self.source_manifest,
            "inventory_sha256": digest(inventory),
            "captured_at_utc": inventory["captured_at"],
            "validated_at_utc": now.isoformat(),
        }

    def validate_inventory_receipt(self, record, inventory, *, final, now=None):
        from prepare_two_host_scaling import validate_inventory

        if type(final) is not bool:
            raise ValueError("Explicit admission or analysis context required")
        now = now or datetime.now(UTC)
        receipt = record.get("inventory_qualification")
        expected = {
            "schema": 1,
            "arm": self.arm,
            "api_image_id": self.images["api"],
            "source_manifest_sha256": self.source_manifest,
            "inventory_sha256": digest(inventory),
            "captured_at_utc": inventory["captured_at"],
        }
        if (
            not isinstance(receipt, dict)
            or set(receipt) != {*expected, "validated_at_utc"}
            or any(receipt.get(k) != v for k, v in expected.items())
            or type(receipt.get("schema")) is not int
            or inventory["arm"] != self.arm
            or record.get("arm") != self.arm
            or record.get("pre_dispatch_qualified") is not True
            or record.get("inventory_contract", {}).get("inventory_contract_pass") is not True
        ):
            raise ValueError("Exact qualified inventory receipt required")

        def timestamp(value):
            if not isinstance(value, str):
                raise TypeError("Recorded aware qualification timestamp required")
            parsed = datetime.fromisoformat(value)
            if parsed.tzinfo is None:
                raise ValueError("Recorded aware qualification timestamp required")
            return parsed

        captured = timestamp(receipt["captured_at_utc"])
        validated = timestamp(receipt["validated_at_utc"])
        ready = timestamp(record.get("pre_dispatch_qualified_at_utc"))
        requested = timestamp(record.get("dispatch_requested_at_utc"))
        scheduled = timestamp(record.get("scheduled_offered_start_utc"))
        if not captured <= validated <= ready <= requested <= now or not requested <= scheduled:
            raise ValueError("Qualification/dispatch chronology differs")
        if not 0 <= (scheduled - captured).total_seconds() <= 300:
            raise ValueError("Qualified inventory expired before scheduled dispatch")
        if final:
            start = timestamp(record.get("offered_start_utc"))
            end = timestamp(record.get("offered_end_utc"))
            if (
                record.get("customers_dispatched") is not True
                or not requested <= start < end <= now
                or not 0 <= (start - captured).total_seconds() <= 300
                or abs((start - scheduled).total_seconds()) > 1
                or (end - start).total_seconds() != 300
            ):
                raise ValueError("Recorded offered window differs from qualified dispatch")
        elif not 0 <= (now - captured).total_seconds() <= 300 or scheduled < now:
            raise ValueError("Qualified inventory stale at launch")
        view = validate_inventory(inventory, image_id=self.images["api"], contract=self, now=validated)
        return view["inventory_contract_pass"] is True

    @property
    def flag(self):
        return "1" if self.arm == "candidate" else "0"

    def settings(self, role):
        return {
            "ORDER_STATUS_CACHE_MS": "1000" if role in {"api", "consumer"} else "0",
            "ORDER_STATUS_EVENT_REFRESH": self.flag if role == "consumer" else "0",
        }

    def primary_model(self, model):
        result = copy.deepcopy(model)
        for role in ROLES:
            result["services"][role]["image"] = self.images[role]
            result["services"][role]["environment"].update(self.settings(role))
        return result

    def secondary_model(self, model):
        result = copy.deepcopy(model)
        result["services"]["api"]["image"] = self.images["api"]
        result["services"]["api"]["environment"].update(self.settings("api"))
        return result

    def stage_snapshot(self, saved):
        from two_host_topology import deployment_model

        result = copy.deepcopy(saved)
        model = self.primary_model(
            deployment_model(saved, primary_ip="10.0.0.1", nginx_path="/owned/nginx.conf")
        )
        result["model"]["services"] = model["services"]
        result["image_id"] = self.images["api"]
        return result

    def verify_worker_settings(self, role, environment):
        if any(environment.get(k, "0") != v for k, v in self.settings(role).items()):
            raise ValueError("Background cache/refresh factor drift")

    def verify_inventory(self, data):
        workers = data.get("worker_sources", [])
        if len(workers) != 13:
            raise ValueError("Complete worker source evidence required")
        ids, counts = set(), {role: 0 for role in BACKGROUND}
        for row in workers:
            role = row.get("role")
            if (
                role not in counts
                or row.get("container_id") in ids
                or row.get("image_id") != self.images[role]
            ):
                raise ValueError("Worker source image/identity differs")
            ids.add(row["container_id"])
            counts[role] += 1
            if row.get("source_identity", {}).get("source_hashes_match") is not True:
                raise ValueError("Worker import source differs")
            self.verify_worker_settings(role, row.get("settings", {}))
        if counts != {k: v["replicas"] for k, v in BACKGROUND.items()}:
            raise ValueError("Worker role counts differ")
        if any(a.get("ORDER_STATUS_EVENT_REFRESH", "0") != "0" for a in data["apis"]):
            raise ValueError("Only consumer refresh may change")

    def image_program(self, roles):
        if not set(roles) <= set(ROLES):
            raise ValueError("Unknown artifact role")
        values = {r: {"image": self.images[r], "parent": self.parents[r]} for r in roles}
        return (
            r"""import json,subprocess
values=VALUES
for role,value in values.items():
 candidate=json.loads(subprocess.check_output(['docker','image','inspect',value['image']],text=True,timeout=10))[0]
 parent=json.loads(subprocess.check_output(['docker','image','inspect',value['parent']],text=True,timeout=10))[0]
 layers=parent['RootFS']['Layers']
 if candidate['Id']!=value['image'] or parent['Id']!=value['parent'] or not layers or candidate['RootFS']['Layers'][:len(layers)]!=layers:raise ValueError('Artifact parent layers differ')
 if candidate['Config']['Labels'].get(LABEL)!=MANIFEST:raise ValueError('Artifact source manifest label differs')
 for key in ('Env','Cmd','Entrypoint','User','WorkingDir'):
  if candidate['Config'].get(key)!=parent['Config'].get(key):raise ValueError('Artifact inherited runtime config differs')
print(json.dumps({'artifact_roles_verified':list(values)}))
""".replace("VALUES", repr(values))
            .replace("LABEL", repr(LABEL))
            .replace("MANIFEST", repr(self.source_manifest))
        )

    def pre_mutation(self, session, saved):
        if any(saved["model"]["services"][r]["image"] != self.parents[r] for r in ROLES):
            raise ValueError("Saved per-role parents differ from artifact receipt")
        session.call("primary", self.image_program(ROLES), 120)
        session.call("secondary", self.image_program(("api",)), 45)

    def pre_safety(self, session, routes, saved):
        from collect_two_host_inventory import observe

        snapshot = self.stage_snapshot(saved)
        inventory, view, *_rows = observe(
            session,
            self.arm,
            routes,
            snapshot["model"]["services"]["api"]["environment"],
            expected_worker_images={r: self.images[r] for r in BACKGROUND},
            contract=self,
        )
        if view.get("inventory_contract_pass") is not True:
            raise ValueError("Pre-safety candidate contract failed")
        session.state["candidate_pre_safety_sources_verified"] = True
        session.checkpoint()
        return inventory
