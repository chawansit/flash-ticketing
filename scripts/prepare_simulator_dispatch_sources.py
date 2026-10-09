"""ADR0251 prepare one worker-only validation change; never deploy or dispatch."""
import ast
import hashlib
import json
from pathlib import Path

import cce_historical_sources as historical
import generator_completion_probe_contract as baseline
from cce_api_adapter import legacy_contract
from prepare_slot_comparison_sources import pairs as api_pairs
from stage_status_refresh_images import new_stage_output
from status_refresh_contract import LABEL, REVISION, digest

ROOT = Path(__file__).resolve().parents[1]
CONFIG = "src/ticketing/config.py"
OLD = b'        if not 1 <= self.simulator_concurrency <= self.pool_max:\n            raise RuntimeError("SIMULATOR_CONCURRENCY must fit DB_POOL_MAX")\n'
NEW = b'        # HTTP delivery runs outside SQL transactions; bound its slots separately.\n        if type(self.simulator_concurrency) is not int or not 1 <= self.simulator_concurrency <= 32:\n            raise RuntimeError("SIMULATOR_CONCURRENCY must be an integer between 1 and 32")\n'


def source_plan():
    base = baseline.plan()
    contract = baseline.GeneratorCompletionProbeContract(base["artifact_receipt"], "candidate", legacy_contract()["api_sources"])
    hashes = contract.source_map("simulator")
    parent = {name: historical.blob(value) for name, value in hashes.items()}
    if parent[CONFIG].count(OLD) != 1 or (ROOT / CONFIG).read_bytes().replace(b"\r\n", b"\n").count(NEW) != 1:
        raise ValueError("Exact reviewed simulator validation required")
    candidate = {**parent, CONFIG: parent[CONFIG].replace(OLD, NEW, 1)}
    for name, raw in candidate.items():
        ast.parse(raw, filename=name)
    sources = {name: hashlib.sha256(raw).hexdigest() for name, raw in candidate.items()}
    if [name for name in hashes if hashes[name] != sources[name]] != [CONFIG]:
        raise ValueError("Simulator config must be the only changed module")
    _, api = api_pairs()
    return candidate, {"decision": "ADR0251", "parent_image": contract.images["simulator"],
        "parent_runtime_source_sha256": hashes, "runtime_source_sha256": sources,
        "dependency_input_sha256": api["dependency_input_sha256"],
        "source_manifest_sha256": digest({"base_revision": REVISION, "runtime_source_sha256": sources}),
        "changed_paths": [CONFIG], "simulator_concurrency": 12, "database_pool_max": 10,
        "cloud_load_started": False}


def prepare():
    files, plan = source_plan()
    output = new_stage_output()
    output.mkdir()
    for name, raw in files.items():
        path = output / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    (output / "manifest.json").write_bytes((json.dumps(plan, indent=2)+"\n").encode())
    (output / "install.py").write_bytes((ROOT / "scripts/status_refresh_image_install.py").read_bytes().replace(b"\r\n", b"\n"))
    (output / "Dockerfile").write_bytes(("FROM "+plan["parent_image"]+"\nUSER root\nCOPY . /tmp/simulator-payload\n"
        +"RUN python /tmp/simulator-payload/install.py /tmp/simulator-payload\n"
        +"LABEL "+LABEL+'="'+plan["source_manifest_sha256"]+'"\nUSER ticketing\n').encode())
    return output, plan
