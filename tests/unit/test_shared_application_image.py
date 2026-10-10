import hashlib
import json

import pytest
from cce_shared_application import REGISTRY, ROLES, render, resources
from prepare_shared_application_image import CONFIG, NEW, OLD, sha, source_patch


def receipt():
    sources = {f"src/ticketing/module{'a' * (i + 1)}.py": "b" * 64 for i in range(22)}
    return {"decision": "ADR0258", "registry_published": True,
            "registry_image": REGISTRY + "@sha256:" + "a" * 64,
            "registry_manifest_digest": "sha256:" + "a" * 64,
            "runtime_source_sha256": sources,
            "source_identity_sha256": hashlib.sha256(json.dumps(sources, sort_keys=True).encode()).hexdigest()}


def test_one_validated_digest_for_all_roles_and_no_automatic_activation():
    result = render(receipt())
    deployments = [item for item in result["items"] if item["kind"] == "Deployment"]
    assert len(deployments) == len(ROLES)
    assert all(item["spec"]["replicas"] == 0 for item in deployments)
    assert {item["spec"]["template"]["spec"]["containers"][0]["image"] for item in deployments} == {receipt()["registry_image"]}
    for item in deployments:
        role = item["metadata"]["labels"]["app.kubernetes.io/component"]
        pod = item["spec"]["template"]["spec"]
        container = pod["containers"][0]
        env = {v["name"]: v for v in container["env"]}
        assert env["DATABASE_URL"]["valueFrom"]["secretKeyRef"]["key"] == "DATABASE_URL_POOLER"
        assert env["DB_POOL_MAX"]["value"] == str(ROLES[role][1])
        assert "livenessProbe" not in container
        assert pod["terminationGracePeriodSeconds"] == 60
        assert pod["automountServiceAccountToken"] is False
        if role != "api":
            assert container["command"] == ["python", "-m", "ticketing.workers", role]
            assert "readinessProbe" not in container  # Metrics cannot prove business readiness.
        if role == "simulator":
            assert env["SIMULATOR_CONCURRENCY"]["value"] == "12"
            assert env["ENVIRONMENT"]["value"] == "development"
        elif role == "confirmation":
            assert env["PAYMENT_CONFIRMATION_ASYNC"]["value"] == "1"
            assert item["metadata"]["annotations"]["ticketing/suggested-test-replicas"] == "0"
        else:
            assert env["PAYMENT_CONFIRMATION_ASYNC"]["value"] == "0"


@pytest.mark.parametrize("change", [
    {"registry_image": REGISTRY + ":latest"}, {"registry_published": False},
    {"decision": "ADR0255"}, {"source_identity_sha256": "0" * 64},
    {"registry_manifest_digest": "sha256:" + "f" * 64}, {"runtime_source_sha256": {}},
])
def test_reject_ambiguous_or_drifted_receipt(change):
    with pytest.raises(ValueError):
        render({**receipt(), **change})


def test_budget_is_client_connections_separate_from_physical_server_budget():
    plan = resources()
    assert plan["suggested_test_pods"] == 17
    assert plan["requested_vcpu"] == 7.25 and plan["requested_memory_gib"] == 14.5
    assert plan["application_client_connections_max"] == 146
    assert plan["pgbouncer_server_connections_max"] == 24


def accepted_sources():
    return {CONFIG: b"class Settings:\n    def validate(self):\n" + OLD,
            "src/ticketing/workers.py": b"def worker():\n    return True\n"}


def test_patch_changes_only_validation_and_preserves_worker_bytes():
    files = accepted_sources()
    candidate = source_patch(files, {name: sha(raw) for name, raw in files.items()})
    assert candidate[CONFIG] == files[CONFIG].replace(OLD, NEW, 1)
    assert candidate["src/ticketing/workers.py"] == files["src/ticketing/workers.py"]


def test_wrong_parent_or_duplicate_patch_anchor_is_rejected():
    files = accepted_sources()
    expected = {name: sha(raw) for name, raw in files.items()}
    with pytest.raises(ValueError, match="Complete accepted"):
        source_patch({**files, "src/ticketing/workers.py": b"pass"}, expected)
    files[CONFIG] += OLD
    with pytest.raises(ValueError, match="Exactly one"):
        source_patch(files, {name: sha(raw) for name, raw in files.items()})


def test_writer_merge_preserves_other_methods_and_defaults():
    import ast

    from cce_historical_sources import blob
    from prepare_shared_application_image import WRITER_SOURCES, merge_writer, method_span

    files = {name: blob(digest) for name, digest in WRITER_SOURCES.items()}
    config = files[CONFIG].decode()
    field = '    reservation_write_pipeline: bool = os.getenv("RESERVATION_WRITE_PIPELINE", "0") == "1"\n'
    files[CONFIG] = config.replace(field, '').encode()
    name = "src/ticketing/workers.py"
    construction = ('    store = PostgresReservations(\n'
                    '        db, cache, settings.hold_seconds,\n'
                    '        persist_write_pipeline=role == "reservation-writer" and settings.reservation_write_pipeline,\n'
                    '    )')
    files[name] = files[name].replace(construction.encode(),
                                    b'    store = PostgresReservations(db, cache, settings.hold_seconds)')
    name = "src/ticketing/infrastructure/reservations.py"
    source = files[name].decode()
    start, end = method_span(source, "PostgresReservations", "__init__")
    source = ''.join(source.splitlines(True)[:start]) + (
        '    def __init__(self, db: Database, cache: SeatCache, hold_seconds=120):\n'
        '        self.db, self.cache, self.hold_seconds = db, cache, hold_seconds\n') + ''.join(source.splitlines(True)[end:])
    start, end = method_span(source, "PostgresReservations", "_persist_reservation_command")
    source = ''.join(source.splitlines(True)[:start]) + (
        '    def _persist_reservation_command(self, conn, payload):\n        pass\n') + ''.join(source.splitlines(True)[end:])
    files[name] = source.encode()
    result = merge_writer(files)
    for module, original in files.items():
        def methods(raw):
            tree = ast.parse(raw)
            return {node.name + '.' + child.name: ast.dump(child)
                    for node in tree.body if isinstance(node, ast.ClassDef)
                    for child in node.body if isinstance(child, ast.FunctionDef)}
        before, after = methods(original), methods(result[module])
        changes = {method for method in before if before[method] != after[method]}
        assert changes == ({"PostgresReservations.__init__", "PostgresReservations._persist_reservation_command"}
                           if module.endswith("infrastructure/reservations.py") else set())
    assert field.encode() in result[CONFIG]


def test_committed_deployments_and_compose_pin_the_selected_receipt():
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    selected = json.loads((root / "docs/capacity/cce/shared-application-image-2026-10-10.json").read_text())
    preview = root / "kubernetes/cce-shared-application.preview.json"
    assert json.loads(preview.read_text()) == render(selected)
    assert hashlib.sha256(preview.read_bytes()).hexdigest() == selected["deployment_preview_sha256"]
    references = [line.strip().split(": ", 1)[1] for line in (root / "compose.shared.yaml").read_text().splitlines()
                  if line.strip().startswith("image: ")]
    assert len(references) == 12
    assert set(references) == {selected["registry_image"]}
    assert selected["registry_pulled_runtime_verified"] is True
