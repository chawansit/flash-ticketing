"""ADR0364 durable UUID locality qualification on fixed ADR0359 resources."""
import hashlib
import json
import math
import re

import work_envelope as policy

KEY = "persistent_uuid_decision"
CONTRACT_KEY = "persistent_uuid"
SOURCE = "2c59942318c306c925e5dba3ae26e59c7af30260b4de4560fed823e73ecd6afe"
MANIFEST = "sha256:7793159270ac95e20c3224eaee3298d42ab6bfeb30088d65f925b8ee5ad3b979"
CONFIG = "sha256:06ee546fc6b01e8b0f1bd2638f91d8a194e5f1c8eeac80eec8ada617098d9448"
LOCAL = "docs/capacity/cce/persistent-uuid-local-2026-10-19.json"
PUBLICATION = "docs/capacity/cce/persistent-uuid-registry-2026-10-19.json"
MODULES = {"src/ticketing/config.py","src/ticketing/domain.py","src/ticketing/workers.py",
           "src/ticketing/infrastructure/reservations.py","src/ticketing/infrastructure/redis_reservations.py"}


def selected(goal):
    if goal is None or KEY not in goal:
        return False
    if (goal[KEY] != "ADR0364" or goal.get("fixed_worker_allocation_decision") != "ADR0359"
            or goal.get("rate_qualification_decision") != "ADR0355"
            or type(goal.get("rate_qualification_rate")) is not int
            or goal.get("rate_qualification_rate") not in (180,200)
            or goal.get("simulator_roundtrip_arm") != "candidate"):
        raise ValueError("Explicit fixed-resource durable UUID qualification required")
    return True


def image():
    from cce_projection_refresh_pipeline import image as parent_image
    before = parent_image()
    local = policy.read(policy.ROOT/LOCAL)
    published = policy.read(policy.ROOT/PUBLICATION)
    sources = published.get("runtime_source_sha256",{})
    benchmark = local.get("index_locality_comparison",{})
    values = [benchmark.get(mode,{}).get(field) for mode in ("control","candidate")
              for field in ("median_ms","p95_ms")]
    if any(type(v) not in (int,float) or not math.isfinite(v) or v<=0 for v in values):
        raise ValueError("Finite executed local comparison required")
    if (published.get("decision") != "ADR0364"
            or published.get("registry_manifest_digest") != MANIFEST
            or published.get("registry_configuration_digest") != CONFIG
            or published.get("local_image_id") != MANIFEST
            or published.get("registry_pulled_image_id") != MANIFEST
            or published.get("source_identity_sha256") != SOURCE
            or published.get("registry_image") != "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing@"+MANIFEST
            or any(published.get(k) is not True for k in
                   ("registry_published","permission_verified","registry_pulled_runtime_verified"))
            or published.get("validation") != {"runtime_modules_verified":22,"default_off":True,"enabled_uuid_version":7,"installed_module_matches":True}
            or published.get("platform") != {"os":"linux","architecture":"amd64"}
            or published.get("runtime_user") != "ticketing"
            or set(sources) != set(before["runtime_source_sha256"])
            or {k for k in sources if sources[k] != before["runtime_source_sha256"][k]} != MODULES
            or hashlib.sha256(json.dumps(sources,sort_keys=True).encode()).hexdigest() != SOURCE
            or published.get("dependency_input_sha256") != before["dependency_input_sha256"]
            or published.get("parent_source_identity_sha256") != before["source_identity_sha256"]
            or local.get("source_identity_sha256") != SOURCE or local.get("runtime_source_sha256") != sources
            or local.get("integration_cases_passed") != 122 or local.get("scope_invariants_passed") != 3
            or local.get("owned_local_cleanup_pass") is not True
            or benchmark.get("seed_uuid4_rows") != 1000000 or benchmark.get("all_counts_verified") is not True
            or benchmark.get("candidate",{}).get("median_wal_bytes",float("inf")) >= benchmark.get("control",{}).get("median_wal_bytes",0)*.5
            or benchmark.get("candidate",{}).get("median_ms",float("inf")) >= benchmark.get("control",{}).get("median_ms",0)
            or benchmark.get("candidate",{}).get("p95_ms",float("inf")) >= benchmark.get("control",{}).get("p95_ms",0)):
        raise ValueError("Exact executed, published and pulled five-module candidate required")
    for name,key in (("prepare_persistent_uuid_image.py","preparation_recipe_sha256"),
                     ("persistent_uuid_overlay.py","overlay_sha256")):
        current = hashlib.sha256((policy.ROOT/"scripts"/name).read_bytes().replace(b"\r\n",b"\n")).hexdigest()
        if published.get(key) != current or local.get(key) != current:
            raise ValueError("Qualified candidate preparation drift")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}",published["registry_configuration_digest"]):
        raise ValueError("Immutable configuration digest required")
    return {**local,**published,"local_only":False,"runtime_sources_sha256":sources}


def contract(goal):
    if not selected(goal):
        raise ValueError("Explicit durable UUID qualification required")
    value = image()
    return {"decision":"ADR0364","source":SOURCE,"manifest":MANIFEST,"configuration":CONFIG,
            "parent_source":value["parent_source_identity_sha256"],
            "admission_source":"19da6fb554d1e592e3eba59a52427b10510c213ab62c2a80181b8d5d27123925",
            "native_environment":{"PERSISTENT_UUID_V7":"1"},"uuid_version":7,
            "projection_transaction_pipeline":True,"sql_unchanged":True,
            "existing_identifiers_rewritten":False,"lease_cache_http_tokens_unchanged":True,
            "writer_batch_size":4,"writer_replicas":4,"consumer_replicas":6,
            "aggregate_resources_unchanged":True,
            "publication_sha256":policy.digest(policy.read(policy.ROOT/PUBLICATION)),
            "local_evidence_sha256":policy.digest(policy.read(policy.ROOT/LOCAL))}


def environment(role,values,goal):
    if not selected(goal):return values
    if any(k in values for k in ("PERSISTENT_UUID_V7","RESERVATION_LOOKUP_CLAIM")):
        raise ValueError("Unmodified default-off bootstrap required")
    return {**values,"PERSISTENT_UUID_V7":"1"}


def api_environment(values,goal):
    return environment("api",values,goal)


def normalize_api_environment(values,goal):
    if not selected(goal):return values
    if values.get("PERSISTENT_UUID_V7")!="1":
        raise ValueError("Exact enabled native API UUID flag required")
    return {k:v for k,v in values.items() if k!="PERSISTENT_UUID_V7"}


def validate_previous(goal,previous):
    if not selected(goal):
        return
    expected = "19da6fb554d1e592e3eba59a52427b10510c213ab62c2a80181b8d5d27123925" if goal["rate_qualification_rate"]==180 else SOURCE
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
