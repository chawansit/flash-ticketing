import hashlib
import json
import marshal
import os
import py_compile
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from runtime_source_identity import source_identity_program

RELATIVE = "src/ticketing/identity_fixture.py"


def fixture(tmp_path):
    app = tmp_path / "app"
    installed = tmp_path / "installed"
    for root in (app / "src", installed):
        (root / "ticketing").mkdir(parents=True)
        (root / "ticketing" / "__init__.py").write_text("")
        (root / "ticketing" / "identity_fixture.py").write_text("raise RuntimeError('must not execute application module')\n")
    raw = (app / RELATIVE).read_bytes()
    digest = hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()
    return app, installed, {RELATIVE: digest}


def execute(app, installed, expected):
    # Only the test redirects the fixed /app fixture root; production has no root override.
    code = source_identity_program(expected).replace("Path('/app')", "Path(" + repr(str(app)) + ")")
    result = subprocess.run([sys.executable, "-c", code], cwd=installed,
                            env={**os.environ, "PYTHONPATH": str(installed)},
                            capture_output=True, text=True, timeout=15, check=False)
    return result


def test_source_proof_resolves_installed_copy_without_executing_target(tmp_path):
    app, installed, expected = fixture(tmp_path)
    result = execute(app, installed, expected)
    assert result.returncode == 0, result.stderr
    proof = json.loads(result.stdout)
    assert proof["source_hashes_match"] is True
    assert proof["import_code_matches_source"] is True
    assert proof["resolved_imports"][RELATIVE]["origin"] == str(installed / "ticketing/identity_fixture.py")
    assert not list(installed.rglob("*.pyc"))


def test_app_hash_can_match_while_installed_source_differs(tmp_path):
    app, installed, expected = fixture(tmp_path)
    (installed / "ticketing/identity_fixture.py").write_text("other = 1\n")
    proof = json.loads(execute(app, installed, expected).stdout)
    assert proof["app_source_hashes_match"] is True
    assert proof["import_source_hashes_match"] is False
    assert proof["source_hashes_match"] is False


def test_valid_timestamp_pyc_with_different_code_is_rejected(tmp_path):
    app, installed, expected = fixture(tmp_path)
    source = installed / "ticketing/identity_fixture.py"
    cached = Path(py_compile.compile(str(source), doraise=True))
    header = cached.read_bytes()[:16]
    changed = compile("different = 42\n", str(source), "exec")
    cached.write_bytes(header + marshal.dumps(changed))
    proof = json.loads(execute(app, installed, expected).stdout)
    assert proof["app_source_hashes_match"] is True
    assert proof["import_source_hashes_match"] is True
    assert proof["import_code_matches_source"] is False
    assert proof["source_hashes_match"] is False


def test_missing_import_aborts_proof(tmp_path):
    app, installed, expected = fixture(tmp_path)
    (installed / "ticketing/identity_fixture.py").unlink()
    result = execute(app, installed, expected)
    assert result.returncode != 0
    assert not result.stdout


@pytest.mark.parametrize("expected", [None, {}, {"../secret": "0" * 64},
    {"src/ticketing/../../secret.py": "0" * 64}, {"src/ticketing/api.py": "bad"},
    {"src/ticketing/api.py": "A" * 64}, {1: "0" * 64},
    {f"src/ticketing/mod{i}.py": "0" * 64 for i in range(65)}])
def test_unsafe_or_unbounded_source_map_rejected_before_remote_work(expected):
    with pytest.raises(ValueError):
        source_identity_program(expected)


def test_invalid_readiness_option_rejected():
    with pytest.raises(ValueError):
        source_identity_program({RELATIVE: "0" * 64}, readiness="yes")


def test_zip_loader_is_not_accepted_as_source_proof(tmp_path):
    import zipfile

    app, installed, expected = fixture(tmp_path)
    archive = tmp_path / "package.zip"
    with zipfile.ZipFile(archive, "w") as zipped:
        zipped.write(installed / "ticketing/__init__.py", "ticketing/__init__.py")
        zipped.write(installed / "ticketing/identity_fixture.py", "ticketing/identity_fixture.py")
    code = source_identity_program(expected).replace("Path('/app')", "Path(" + repr(str(app)) + ")")
    result = subprocess.run([sys.executable, "-c", code], cwd=tmp_path,
                            env={**os.environ, "PYTHONPATH": str(archive)},
                            capture_output=True, text=True, timeout=15, check=False)
    assert result.returncode != 0 and "Unsupported source loader" in result.stderr


@pytest.mark.parametrize("changed", [None, "image", "start", "role", "stopped", "import", "changed-after"])
def test_container_identity_guard_rechecks_around_source_proof(tmp_path, monkeypatch, capsys, changed):
    from copy import deepcopy

    from runtime_source_identity import container_identity_program

    row = {"Id": "1" * 64, "Image": "sha256:" + "2" * 64,
           "State": {"Running": True, "StartedAt": "2026-10-05T00:00:00Z"},
           "Config": {"Labels": {"com.docker.compose.service": "consumer"}}}
    calls = []

    def command(args, **kwargs):
        calls.append(args[1])
        if args[1] == "exec":
            return json.dumps({"source_hashes_match": changed != "import", "resolved_imports": {}})
        observed = deepcopy(row)
        if changed == "image":
            observed["Image"] = "other"
        if changed == "start" or (changed == "changed-after" and len(calls) == 3):
            observed["State"]["StartedAt"] = "restarted"
        if changed == "role":
            observed["Config"]["Labels"]["com.docker.compose.service"] = "api"
        if changed == "stopped":
            observed["State"]["Running"] = False
        return json.dumps([observed])

    monkeypatch.setattr(subprocess, "check_output", command)
    code = container_identity_program(row, "consumer", {RELATIVE: "0" * 64})
    if changed is None:
        exec(compile(code, "identity", "exec"), {})  # noqa: S102
        result = json.loads(capsys.readouterr().out)
        assert result["started_at"] == row["State"]["StartedAt"]
        assert result["source_identity"]["source_hashes_match"] is True
        assert calls == ["inspect", "exec", "inspect"]
    else:
        with pytest.raises(ValueError):
            exec(compile(code, "identity", "exec"), {})  # noqa: S102
        assert len(calls) <= 3
        assert not capsys.readouterr().out


def test_container_identity_requires_full_immutable_identity():
    from runtime_source_identity import container_identity_program

    with pytest.raises(ValueError):
        container_identity_program({"Id": "short", "Image": "tag"}, "consumer", {RELATIVE: "0" * 64})
