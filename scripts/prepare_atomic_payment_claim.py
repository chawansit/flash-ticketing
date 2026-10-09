"""ADR0192: export only the approved atomic claim into the frozen control source."""
import argparse
import ast
import json
import subprocess
from pathlib import Path

import prepare_partial_timeout_reclamation as parent

ROOT = parent.base.ROOT
REVISION = "7ca20ed3e541f1853a937a991e40383f3854a1e3"
WORKERS = "src/ticketing/workers.py"
CONSTANT = "CLAIM_PAYMENT_SQL"
FUNCTION = "simulate_one"


def approved_workers():
    return subprocess.check_output(["git", "show", REVISION + ":" + WORKERS], cwd=ROOT, timeout=15).decode()


def node(raw, name):
    matches = [n for n in ast.parse(raw).body if (
        isinstance(n, ast.FunctionDef) and n.name == name
        or isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in n.targets))]
    if len(matches) != 1:
        raise ValueError("One exact approved AST node required")
    return matches[0]


def dump(value):
    return ast.dump(value, include_attributes=False)


def verify_port(original, candidate, approved):
    old, new, selected = [node(raw, FUNCTION) for raw in (original, candidate, approved)]
    if dump(new) != dump(selected) or dump(node(candidate, CONSTANT)) != dump(node(approved, CONSTANT)):
        raise ValueError("Approved claim implementation differs")
    if (dump(old.args) != dump(new.args)
            or [dump(n) for n in old.decorator_list] != [dump(n) for n in new.decorator_list]
            or [dump(n) for n in old.body[2:]] != [dump(n) for n in new.body[2:]]
            or dump(old.body[0]) != dump(new.body[0])):
        raise ValueError("Delivery, acknowledgement or transaction boundary changed")
    def remaining(raw):
        return [dump(n) for n in ast.parse(raw).body if not (
            isinstance(n, ast.FunctionDef) and n.name == FUNCTION
            or isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == CONSTANT for t in n.targets))]
    if remaining(original) != remaining(candidate):
        raise ValueError("Unrelated worker implementation changed")
    if any(isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == CONSTANT for t in n.targets)
           for n in ast.parse(original).body):
        raise ValueError("Baseline already contains the candidate")


def port(original):
    approved = approved_workers()
    old = node(original, FUNCTION)
    new = node(approved, FUNCTION)
    constant = node(approved, CONSTANT)
    lines = original.splitlines(keepends=True)
    start = min([old.lineno, *(d.lineno for d in old.decorator_list)]) - 1
    chosen = approved.splitlines(keepends=True)
    selected_start = min([new.lineno, *(d.lineno for d in new.decorator_list)]) - 1
    replacement = "".join(chosen[constant.lineno - 1:constant.end_lineno]) + "\n\n\n"
    replacement += "".join(chosen[selected_start:new.end_lineno])
    candidate = "".join(lines[:start]) + replacement + "".join(lines[old.end_lineno:])
    verify_port(original, candidate, approved)
    return candidate.encode()


def verify(source):
    source = Path(source)
    if source.is_symlink() or not source.resolve().is_relative_to((ROOT / "tmp").resolve()):
        raise ValueError("Owned candidate source under tmp required")
    original = source.parent / "control-workers.py"
    receipt_path = source.parent / "atomic-claim-source-receipt.json"
    if original.is_symlink() or receipt_path.is_symlink():
        raise ValueError("Symlink receipt or input rejected")
    expected = parent.expected_source_map()
    if parent.base.sha(original.read_bytes()) != expected[WORKERS]:
        raise ValueError("Frozen baseline worker fingerprint differs")
    candidate = port(original.read_text(encoding="utf-8"))
    expected[WORKERS] = parent.base.sha(candidate)
    parent.base.verify_tree(source, expected)
    runtime = {**parent.manifest()["runtime_source_sha256"], WORKERS: expected[WORKERS]}
    receipt = json.loads(receipt_path.read_text())
    exact = {"decision":"ADR0192", "implementation_decision":"ADR0189", "implementation_revision":REVISION,
             "parent_manifest_sha256":parent.base.sha((parent.ARTIFACTS / "manifest.json").read_bytes()),
             "source_file_sha256":expected, "runtime_source_sha256":runtime,
             "source_manifest_sha256":parent.base.digest({"base_revision":parent.base.REVISION,
                                                           "runtime_source_sha256":runtime}),
             "changed_runtime_files":[WORKERS], "delivery_and_acknowledgement_ast_unchanged":True,
             "other_worker_ast_unchanged":True}
    if receipt != exact:
        raise ValueError("Exact immutable candidate receipt required")
    return runtime


def prepare(output):
    output, data, _ = parent.prepare(output)
    source = output / "source"
    worker = source / WORKERS
    original = worker.read_bytes()
    (output / "control-workers.py").write_bytes(original)
    worker.write_bytes(port(original.decode()))
    expected = parent.expected_source_map()
    expected[WORKERS] = parent.base.sha(worker.read_bytes())
    parent.base.verify_tree(source, expected)
    runtime = {**data["runtime_source_sha256"], WORKERS:expected[WORKERS]}
    data.update(runtime_source_sha256=runtime,
                overlay_sha256={**data["overlay_sha256"], WORKERS:expected[WORKERS]})
    receipt = {"decision":"ADR0192", "implementation_decision":"ADR0189", "implementation_revision":REVISION,
               "parent_manifest_sha256":parent.base.sha((parent.ARTIFACTS / "manifest.json").read_bytes()),
               "source_file_sha256":expected, "runtime_source_sha256":runtime,
               "source_manifest_sha256":parent.base.digest({"base_revision":parent.base.REVISION,
                                                             "runtime_source_sha256":runtime}),
               "changed_runtime_files":[WORKERS], "delivery_and_acknowledgement_ast_unchanged":True,
               "other_worker_ast_unchanged":True}
    (output / "atomic-claim-source-receipt.json").write_text(json.dumps(receipt,indent=2)+"\n")
    context=output / "image-context"
    (context / WORKERS).write_bytes(worker.read_bytes())
    (context / "manifest.json").write_text(json.dumps(data,sort_keys=True)+"\n")
    verify(source)
    return output, data, receipt


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",type=Path,required=True)
    args=parser.parse_args()
    output, _, receipt=prepare(args.output)
    print(json.dumps({"decision":"ADR0192", "source":str(output / "source"),
                      "runtime_modules_verified":len(receipt['runtime_source_sha256']),
                      "changed_runtime_files":receipt['changed_runtime_files'], "cloud_calls":0,
                      "cloud_execution_implemented":False}))


if __name__ == "__main__":
    main()
