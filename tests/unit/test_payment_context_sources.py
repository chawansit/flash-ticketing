import ast
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import prepare_payment_context_sources as source


def without_payment(raw):
    tree = ast.parse(raw)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "PostgresReservations")
    cls.body = [n for n in cls.body if not isinstance(n, ast.FunctionDef) or n.name != "initiate_payment"]
    return ast.dump(tree)


def test_candidate_changes_only_reviewed_payment_method_and_keeps_accepted_begin():
    pair, plan = source.pairs()
    assert len(pair["candidate"]) == 22
    assert [k for k in pair["control"] if pair["control"][k] != pair["candidate"][k]] == [source.MODULE]
    assert without_payment(pair["control"][source.MODULE]) == without_payment(pair["candidate"][source.MODULE])
    assert plan["comparison_factor"] == "post_lock_payment_context"
    startup = pair["candidate"]["src/ticketing/infrastructure/postgres.py"].decode()
    assert 'conn.execute("BEGIN")' in startup
    assert 'if getattr(conn, "autocommit", False)' not in startup


def test_reviewed_candidate_hash_rejects_unreviewed_method(monkeypatch):
    original = source.blob

    def damaged(digest):
        return original(digest).replace(b"clock_timestamp() AS now", b"transaction_timestamp() AS now")

    monkeypatch.setattr(source, "blob", damaged)
    with pytest.raises(ValueError, match="reviewed payment"):
        source.pairs()


def test_historical_payment_method_does_not_read_mutable_runtime(monkeypatch):
    original = Path.read_text

    def forbidden(path, *args, **kwargs):
        if path == source.ROOT / source.MODULE:
            raise AssertionError("Historical reproduction read live runtime")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", forbidden)
    pair, _ = source.pairs()
    assert b"payment_write_pipeline" not in pair["candidate"][source.MODULE]
