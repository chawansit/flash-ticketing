"""Scope proofs over the actual verified parent, separate from live database tests."""
import ast
import sys
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"scripts"))
import cce_simulator_roundtrip_profile as parent_profile
from reservation_claim_lookup_overlay import MODULES, overlay

ROOT = Path(__file__).resolve().parents[2]
PARENT = ROOT/"tmp/adr0356-deployed-source"


@pytest.fixture
def source():
    if not PARENT.is_dir():
        pytest.skip("Retained exact deployed source export is required")
    files = {p.relative_to(PARENT).as_posix():p.read_bytes()
             for p in (PARENT/"src/ticketing").rglob("*.py")}
    return files,parent_profile.registry_image()["runtime_source_sha256"]


def functions(raw):
    return {n.name:ast.dump(n,include_attributes=False) for n in ast.walk(ast.parse(raw))
            if isinstance(n,ast.FunctionDef)}


def test_changes_only_reviewed_prelock_path_and_default_configuration(source):
    files,expected = source
    changed = overlay(files,expected)
    assert {name for name in files if files[name]!=changed[name]} == MODULES
    name = "src/ticketing/infrastructure/reservations.py"
    before,after = functions(files[name]),functions(changed[name])
    for method in ("persist_reservation_commands","reserve","checkout","request_payment",
                   "apply_callback","refund","cancel"):
        if method in before:
            assert before[method] == after[method]
    marker = b"        if replay is not None:\n            if (\n                replay[\"hold_id\"]"
    assert files[name].count(marker)==changed[name].count(marker)==1
    # Replay validation, seat locks, post-lock clock and all durable writes stay byte-identical.
    assert files[name].split(marker,1)[1] == changed[name].split(marker,1)[1]
    assert b'RESERVATION_LOOKUP_CLAIM", "0")' in changed["src/ticketing/config.py"]
    assert b'persist_lookup_claim=role == "reservation-writer"' in changed["src/ticketing/workers.py"]


@pytest.mark.parametrize("fault",["module","missing","insertion"])
def test_parent_drift_is_rejected_before_payload_generation(source,fault):
    files,expected = source
    files=dict(files)
    if fault=="missing":
        files.pop("src/ticketing/config.py")
    elif fault=="module":
        files["src/ticketing/config.py"] += b"\n# drift\n"
    else:
        files["src/ticketing/workers.py"] = files["src/ticketing/workers.py"].replace(
            b"persist_write_pipeline=role",b"persist_write_pipeline=False and role")
    with pytest.raises(ValueError):
        overlay(files,expected)
