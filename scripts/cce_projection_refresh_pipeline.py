"""ADR0363 projection transaction qualification on fixed ADR0359 resources."""
import hashlib
import json
import math
import re

import work_envelope as policy

KEY = "projection_refresh_pipeline_decision"
CONTRACT_KEY = "projection_refresh_pipeline"
SOURCE = "72fabf5016df66fd73cfdb0faf05e4c1f80fa41868afa044c266609ffed665f6"
MANIFEST = "sha256:a8531f98d23c4d5f3197cd5273d75c0508dbffb54e846a4f8963dd25d7787c7b"
CONFIG = "sha256:dd577b4adfdcea0bd295d47ce2c62d19b70fc84a343e22b3f23c3aa87f48e505"
LOCAL = "docs/capacity/cce/projection-refresh-pipeline-local-2026-10-19.json"
PUBLICATION = "docs/capacity/cce/projection-refresh-pipeline-registry-2026-10-19.json"
MODULES = {"src/ticketing/workers.py"}


def selected(goal):
    if goal is None or KEY not in goal:
        return False
    if (goal[KEY] != "ADR0363" or goal.get("fixed_worker_allocation_decision") != "ADR0359"
            or goal.get("rate_qualification_decision") != "ADR0355"
            or type(goal.get("rate_qualification_rate")) is not int
            or goal.get("rate_qualification_rate") not in (180,200)
            or goal.get("simulator_roundtrip_arm") != "candidate"):
        raise ValueError("Explicit fixed-resource projection qualification required")
    return True


def image():
    from cce_simulator_roundtrip_profile import registry_image
    before = registry_image()
    local = policy.read(policy.ROOT/LOCAL)
    published = policy.read(policy.ROOT/PUBLICATION)
    sources = published.get("runtime_source_sha256",{})
    benchmark = local.get("synthetic_latency_comparison",{})
    values = [benchmark.get(mode,{}).get(field) for mode in ("control","candidate")
              for field in ("median_ms","p95_ms")]
    if any(type(v) not in (int,float) or not math.isfinite(v) or v<=0 for v in values):
        raise ValueError("Finite executed local comparison required")
    if (published.get("decision") != "ADR0363"
            or published.get("registry_manifest_digest") != MANIFEST
            or published.get("registry_configuration_digest") != CONFIG
            or published.get("local_image_id") != MANIFEST
            or published.get("registry_pulled_image_id") != MANIFEST
            or published.get("source_identity_sha256") != SOURCE
            or published.get("registry_image") != "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@"+MANIFEST
            or any(published.get(k) is not True for k in
                   ("registry_published","permission_verified","registry_pulled_runtime_verified"))
            or published.get("validation") != {"runtime_modules_verified":22,"projection_transaction_pipeline":True,"installed_module_matches":True}
            or published.get("platform") != {"os":"linux","architecture":"amd64"}
            or published.get("runtime_user") != "ticketing"
            or set(sources) != set(before["runtime_source_sha256"])
            or {k for k in sources if sources[k] != before["runtime_source_sha256"][k]} != MODULES
            or hashlib.sha256(json.dumps(sources,sort_keys=True).encode()).hexdigest() != SOURCE
            or published.get("dependency_input_sha256") != before["dependency_input_sha256"]
            or published.get("parent_source_identity_sha256") != before["source_identity_sha256"]
            or local.get("source_identity_sha256") != SOURCE or local.get("runtime_source_sha256") != sources
            or local.get("integration_cases_passed") != 38 or local.get("scope_invariants_passed") != 2
            or local.get("owned_local_cleanup_pass") is not True
            or benchmark.get("candidate",{}).get("median_ms",float("inf")) >= benchmark.get("control",{}).get("median_ms",0)
            or benchmark.get("candidate",{}).get("p95_ms",float("inf")) >= benchmark.get("control",{}).get("p95_ms",0)):
        raise ValueError("Exact executed, published and pulled one-module candidate required")
    for name,key in (("prepare_projection_refresh_pipeline_image.py","preparation_recipe_sha256"),
                     ("projection_refresh_pipeline_overlay.py","overlay_sha256")):
        current = hashlib.sha256((policy.ROOT/"scripts"/name).read_bytes().replace(b"\r\n",b"\n")).hexdigest()
        if published.get(key) != current or local.get(key) != current:
            raise ValueError("Qualified candidate preparation drift")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}",published["registry_configuration_digest"]):
        raise ValueError("Immutable configuration digest required")
    return {**local,**published,"local_only":False,"runtime_sources_sha256":sources}


def contract(goal):
    if not selected(goal):
        raise ValueError("Explicit projection qualification required")
    value = image()
    return {"decision":"ADR0363","source":SOURCE,"manifest":MANIFEST,"configuration":CONFIG,
            "parent_source":value["parent_source_identity_sha256"],
            "changed_function":"consume_refresh_batch", "transaction_pipeline":True,
            "sql_unchanged":True,"writer_batch_size":4,"writer_replicas":4,"consumer_replicas":6,
            "aggregate_resources_unchanged":True,
            "publication_sha256":policy.digest(policy.read(policy.ROOT/PUBLICATION)),
            "local_evidence_sha256":policy.digest(policy.read(policy.ROOT/LOCAL))}


def environment(role,values,goal):
    if selected(goal) and "RESERVATION_LOOKUP_CLAIM" in values:
        raise ValueError("Projection experiment must retain original writer behavior")
    return values


def validate_previous(goal,previous):
    if not selected(goal):
        return
    expected = image()["parent_source_identity_sha256"] if goal["rate_qualification_rate"]==180 else SOURCE
    from cce_fixed_worker_allocation import contract as allocation
    expected_allocation = allocation(goal)
    if goal["rate_qualification_rate"]==180:
        expected_allocation = {**expected_allocation,"application_image_unchanged":True}
    if previous.get("worker_allocation") != expected_allocation:
        raise ValueError("New image qualification preserves exact parent resource allocation")
    if previous.get("source") != expected:
        raise ValueError("180 requires clean parent180;200 requires clean new-image180")
    if goal["rate_qualification_rate"]==200 and previous.get(CONTRACT_KEY) != contract(goal):
        raise ValueError("200 requires actual passed180 with identical new-image feature")
