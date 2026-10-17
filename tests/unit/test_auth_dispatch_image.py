import hashlib
import importlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/"scripts"))
preparation = importlib.import_module("prepare_auth_dispatch_image")
AFTER, BEFORE, MODULE, overlay = preparation.AFTER, preparation.BEFORE, preparation.MODULE, preparation.overlay


def source():
    parent = {MODULE: b"# baseline\n"+BEFORE+b"\n    return credentials\n",
              "src/ticketing/infrastructure/postgres.py": b"# unchanged SQL implementation\n"}
    return parent, {"runtime_source_sha256": {
        name: hashlib.sha256(raw).hexdigest() for name, raw in parent.items()}}


def test_candidate_changes_only_authentication_dispatch_keyword():
    parent, receipt = source()
    candidate = overlay(parent, receipt)
    assert candidate[MODULE] == parent[MODULE].replace(BEFORE, AFTER)
    assert {name for name in parent if candidate[name] != parent[name]} == {MODULE}
    assert candidate[MODULE].replace(AFTER, BEFORE) == parent[MODULE]


@pytest.mark.parametrize("change", ["missing", "modified", "extra"])
def test_source_drift_rejected(change):
    parent, receipt = source()
    if change == "missing":
        parent.pop("src/ticketing/infrastructure/postgres.py")
    elif change == "modified":
        parent[MODULE] += b"# unrelated change\n"
    else:
        parent["src/ticketing/extra.py"] = b"# unrelated\n"
    with pytest.raises(ValueError, match="Complete measured"):
        overlay(parent, receipt)


def test_already_async_parent_is_not_silently_accepted():
    parent, receipt = source()
    parent[MODULE] = parent[MODULE].replace(BEFORE, AFTER)
    receipt["runtime_source_sha256"][MODULE] = hashlib.sha256(parent[MODULE]).hexdigest()
    with pytest.raises(ValueError, match="Exact synchronous"):
        overlay(parent, receipt)
