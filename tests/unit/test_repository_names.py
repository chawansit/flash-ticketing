"""ADR0169 real staged/outgoing snapshots and identifier faults; no cloud calls."""
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"scripts"))
import check_repository_names as check
import fetch_status_refresh_parents as owners
import stage_status_refresh_images as staging


def documents():
    return {"docs/adr/0001-first-decision.md":b"# ADR0001: First decision\n",
            "docs/adr/README.md":b"[ADR0001: First decision](0001-first-decision.md)\n",
            "docs/capacity/result.json":b'{"decision":"ADR0001"}'}


def test_valid_catalog_and_report():
    data=documents();assert check.validate(set(data),data)==[]


@pytest.mark.parametrize("target,expected",[
    ("0001-frist-decision.md","missing reference"),
    ("0001-First-decision.md","case mismatch"),
    ("0002-first-decision.md","missing reference"),
])
def test_reference_typos_are_errors(target,expected):
    data=documents();data["docs/adr/README.md"]=("[ADR0001]("+target+")").encode()
    assert any(expected in error for error in check.validate(set(data),data))


def test_duplicate_number():
    data=documents();data["docs/adr/0001-another-decision.md"]=b"# ADR0001: Other"
    assert any("duplicate ADR0001" in e for e in check.validate(set(data),data))


@pytest.mark.parametrize("field,text,expected",[
    ("docs/adr/0001-first-decision.md",b"# ADR0002: Wrong","title identifier"),
    ("docs/adr/README.md",b"[ADR0002](0001-first-decision.md)","index label"),
    ("docs/adr/README.md",b"# No links","missing entry"),
    ("docs/capacity/result.json",b'{"decision":"ADR001"}',"unknown decision ADR001"),
    ("docs/capacity/result.json",b'{"decisions":["ADR0001","ADR9999"]}',"unknown decision ADR9999"),
    ("docs/capacity/result.json",b'{"decision":"docs/adr/0001-frist-decision.md"}',"missing reference"),
])
def test_identifier_faults(field,text,expected):
    data=documents();data[field]=text
    assert any(expected in e for e in check.validate(set(data),data))


def test_missing_and_invalid_filename():
    data=documents();names=set(data);del data["docs/adr/0001-first-decision.md"]
    assert any("ADR missing" in e for e in check.validate(names,data))
    data=documents();data["docs/adr/0001-Bad_Name.md"]=b"# ADR0001: Bad"
    assert any("kebab-case" in e for e in check.validate(set(data),data))


def test_legacy_windows_markdown_bytes_remain_supported():
    data=documents();data["docs/adr/0001-first-decision.md"]+=b"Legacy \x96 context"
    assert not check.validate(set(data),data)


def test_canonical_staging_name_matches_existing_ownership_validator(monkeypatch,tmp_path):
    monkeypatch.setattr(staging,"ROOT",tmp_path)
    output=staging.new_stage_output()
    assert output.parent==tmp_path/"tmp" and not output.exists()
    assert owners.owner_path("/root/flash-fixture",output.name).endswith(output.name)
    with pytest.raises(ValueError):owners.owner_path("/root/flash-fixture","adr0169-stage-"+"a"*12)


def test_actual_staged_and_outgoing_snapshot_ignore_worktree_drift(monkeypatch,tmp_path):
    def git(*args):return subprocess.check_output(["git",*args],cwd=tmp_path)
    git("init","-q");git("config","user.email","fixture@example.invalid");git("config","user.name","Fixture")
    for name,raw in documents().items():
        p=tmp_path/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
    git("add",".");git("commit","-qm","valid baseline")
    monkeypatch.setattr(check,"ROOT",tmp_path)
    paths,data=check.snapshot("HEAD");assert not check.validate(paths,data)
    readme=tmp_path/"docs/adr/README.md";readme.write_text("[ADR0001](0001-frist-decision.md)")
    git("add","docs/adr/README.md");readme.write_bytes(documents()["docs/adr/README.md"])
    assert not check.validate(*check.snapshot())
    assert any("missing reference" in e for e in check.validate(*check.snapshot(staged=True)))
    assert not check.validate(*check.snapshot("HEAD"))
    git("commit","-qm","invalid outgoing fixture")
    assert any("missing reference" in e for e in check.validate(*check.snapshot("HEAD")))


def test_real_hooks_block_commit_and_push_of_bad_revision(tmp_path):
    source=Path(__file__).resolve().parents[2]
    env={**os.environ,"REPOSITORY_NAMES_PYTHON":sys.executable}
    def git(*args,check=True,input_text=None):
        return subprocess.run(["git",*args],cwd=tmp_path,env=env,text=True,input=input_text,
                              capture_output=True,check=check)
    git("init","-q");git("config","user.email","fixture@example.invalid");git("config","user.name","Fixture")
    for name,raw in documents().items():
        p=tmp_path/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
    shutil.copytree(source/".githooks",tmp_path/".githooks")
    (tmp_path/"scripts").mkdir();shutil.copyfile(source/"scripts/check_repository_names.py",tmp_path/"scripts/check_repository_names.py")
    for hook in (tmp_path/".githooks").iterdir():hook.chmod(0o755)
    git("config","core.hooksPath",".githooks");git("add",".");git("commit","-qm","valid fixture")
    valid=git("rev-parse","HEAD").stdout.strip()
    (tmp_path/"docs/adr/README.md").write_text("[ADR0001](0001-frist-decision.md)")
    git("add","docs/adr/README.md")
    blocked=git("commit","-qm","must be blocked",check=False)
    assert blocked.returncode!=0 and "missing reference" in blocked.stdout+blocked.stderr
    assert git("rev-parse","HEAD").stdout.strip()==valid
    # Simulate bad outgoing history imported from elsewhere, without committing via the hook.
    tree=git("write-tree").stdout.strip()
    invalid=git("commit-tree",tree,"-p",valid,input_text="invalid outgoing fixture\n").stdout.strip()
    git("update-ref","refs/heads/invalid-fixture",invalid)
    git("reset","--hard","HEAD")  # A valid worktree must not hide bad outgoing history.
    remote=tmp_path/"remote.git";git("init","--bare","-q",str(remote));git("remote","add","fixture",str(remote))
    rejected=git("push","fixture","invalid-fixture",check=False)
    assert rejected.returncode!=0 and "missing reference" in rejected.stdout+rejected.stderr
    assert not git("--git-dir",str(remote),"show-ref",check=False).stdout
