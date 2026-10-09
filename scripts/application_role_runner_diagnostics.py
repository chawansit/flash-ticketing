"""ADR0177 explicit scoped preflight, transfer and startup boundaries."""
import application_database_wait_evidence as scoped
from kafka_lag_observe import source_sha256
from qualify_two_host_deployment import ROOT

LEDGER = "bounded_application_role_rebalance"
TRANSFER_FILES = ("observe_two_host_pipeline.py", "prepare_two_host_scaling.py",
                  "database_wait_evidence.py", "application_database_wait_evidence.py")


def prepare(session, cid, owner, directory, inventory, upload):
    binding = scoped.binding_from_record(inventory.get("application_database_role_binding"))
    expected = {}
    for name in TRANSFER_FILES:
        content = (ROOT / "scripts" / name).read_text()
        expected[name] = source_sha256(content.encode())
        upload(session, cid, owner, directory, name, content)
    proof = session.api(cid, "import json,hashlib;from pathlib import Path;p=Path(" + repr(directory)
                        + ");expected=" + repr(expected)
                        + ";assert {k:hashlib.sha256((p/k).read_bytes().replace(b'\\r\\n',b'\\n')).hexdigest() for k in expected}==expected;print(json.dumps({'transferred_scoped_observer_identity':True}))", 45)
    if proof != {"transferred_scoped_observer_identity": True}:
        raise ValueError("Exact scoped observer transfer proof required")
    preflight = session.api(cid, scoped.preflight_program(binding, directory), 45)
    if (preflight.get("pass") is not True or preflight.get("diagnostic_scope") != scoped.SCOPE
            or preflight.get("role_identity_sha256") != binding.identity_sha256
            or type(preflight.get("full_database_visibility_complete")) is not bool):
        raise ValueError("Exact effective application observer preflight required")
    return {"application_database_wait_preflight": preflight, **proof}


def startup(row, inventory):
    binding = scoped.binding_from_record(inventory.get("application_database_role_binding"))
    scoped.startup(row, binding)
    return {"application_database_wait_diagnostics": row["application_database_wait_diagnostics"]}


def summarize(path, inventory):
    binding = scoped.binding_from_record(inventory.get("application_database_role_binding"))
    return scoped.summarize(path, binding)


def complete(record, inventory):
    try:
        binding = scoped.binding_from_record(inventory.get("application_database_role_binding"))
        scoped.startup(record.get("application_database_wait_startup", {}), binding)
        capture = record.get("application_database_wait_capture", {})
        preflight = record.get("application_database_wait_preflight", {})
        return (record.get("transferred_scoped_observer_identity") is True
                and preflight.get("pass") is True and preflight.get("diagnostic_scope") == scoped.SCOPE
                and preflight.get("role_identity_sha256") == binding.identity_sha256
                and type(preflight.get("full_database_visibility_complete")) is bool
                and capture.get("decision") == "ADR0176" and capture.get("diagnostic_scope") == scoped.SCOPE
                and capture.get("role_identity_sha256") == binding.identity_sha256
                and capture.get("bound_replicas") == dict(binding.replicas)
                and capture.get("application_database_wait_evidence_complete") is True
                and type(capture.get("full_database_visibility_complete")) is bool)
    except (KeyError, TypeError, ValueError):
        return False
