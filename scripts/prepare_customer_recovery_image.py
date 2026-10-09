"""Prepare one recovery API image from the exact accepted runtime; no cloud calls."""

import ast
import hashlib
import json
import subprocess
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
PARENT = "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing:slot-adr0153-parents-9738a048121e-control"
RECEIPT = "docs/capacity/cce/slot-comparison-images-2026-10-09.json"
CHANGED = {"src/ticketing/api.py", "src/ticketing/config.py", "src/ticketing/application/ports.py",
           "src/ticketing/application/reservations.py", "src/ticketing/infrastructure/reservations.py",
           "src/ticketing/infrastructure/postgres.py"}


def sha(raw):
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def call(args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def function(source, name, owner=None):
    tree = ast.parse(source)
    nodes = tree.body if owner is None else next(node.body for node in tree.body if isinstance(node, ast.ClassDef) and node.name == owner)
    node = next(node for node in nodes if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name)
    start = min([node.lineno, *[d.lineno for d in node.decorator_list]]) - 1
    return start, node.end_lineno, "".join(source.splitlines(True)[start:node.end_lineno])


def once(source, before, after):
    if source.count(before) != 1:
        raise ValueError("Exact accepted source anchor required")
    return source.replace(before, after, 1)


def prepare():
    receipt = json.loads((ROOT / RECEIPT).read_text())["images"]["control"]
    info = json.loads(call(["image", "inspect", PARENT]))[0]
    if receipt["registry_image"] not in info.get("RepoDigests", []):
        raise ValueError("Accepted immutable registry image differs")
    output = ROOT / "tmp" / ("adr0254-image-" + uuid4().hex[:12])
    output.mkdir()
    cid = call(["create", "--network", "none", "--entrypoint", "true", PARENT])
    try:
        call(["cp", cid + ":/app/src", str(output / "src")])
        for name in ("pyproject.toml", "requirements.lock"):
            call(["cp", cid + ":/app/" + name, str(output / name)])
    finally:
        call(["rm", cid])
    parent = {p.relative_to(output).as_posix(): sha(p.read_bytes()) for p in (output/"src/ticketing").rglob("*.py")}
    if parent != receipt["runtime_sources_sha256"]:
        raise ValueError("Complete accepted image source differs")
    for name in CHANGED:
        p = output / name
        source = p.read_text()
        current = (ROOT / name).read_text()
        if name.endswith("config.py"):
            source = once(source, '    order_status_cache_ms:',
                          '    order_status_read_pipeline: bool = os.getenv("ORDER_STATUS_READ_PIPELINE", "0") == "1"\n    order_status_cache_ms:')
        elif name.endswith("api.py"):
            source = once(source, '    with ExitStack() as resources:',
                          '    db.order_status_read_pipeline = settings.order_status_read_pipeline\n    with ExitStack() as resources:')
            endpoint = function(current, "payment_operation")[2]
            source = once(source, '@app.post("/v1/orders/{order_id}/payments",',
                          endpoint + '\n\n@app.post("/v1/orders/{order_id}/payments",')
        elif name.endswith("ports.py"):
            source = once(source, '    def get_order(self, actor, order_id) -> dict: ...',
                          '    def get_order(self, actor, order_id) -> dict: ...\n    def get_payment_operation(self, actor, order_id, key) -> dict: ...')
        elif name.endswith("postgres.py"):
            start, end, _ = function(source, "transaction", "Postgres")
            lines = source.splitlines(True)
            source = ''.join(lines[:start]) + function(current, "transaction", "Postgres")[2] + '\n' + ''.join(lines[end:])
            source = once(source, 'from contextlib import ExitStack, contextmanager',
                          'from contextlib import ExitStack, contextmanager, nullcontext')
        else:
            owner = "Reservations" if "/application/" in name else "PostgresReservations"
            method = function(current, "get_payment_operation", owner)[2]
            source = once(source, '    def get_hold(self, actor, hold_id):',
                          method + '\n\n    def get_hold(self, actor, hold_id):')
            if owner == "PostgresReservations":
                source = once(source, '    def get_order(self, actor, order_id):\n        with self.db.transaction() as conn:',
                    '    def get_order(self, actor, order_id):\n        transaction = (self.db.transaction(pipeline=True)\n'
                    '                       if getattr(self.db, "order_status_read_pipeline", False) else self.db.transaction())\n'
                    '        with transaction as conn:')
        ast.parse(source)
        p.write_bytes(source.encode())
    runtime = {p.relative_to(output).as_posix(): sha(p.read_bytes()) for p in (output/"src/ticketing").rglob("*.py")}
    if {name for name in parent if runtime[name] != parent[name]} != CHANGED or set(runtime) != set(parent):
        raise ValueError("Recovery image escaped the six-module change scope")
    manifest = {"parent_runtime_source_sha256": parent, "runtime_source_sha256": runtime,
                "dependency_input_sha256": {name: sha((output/name).read_bytes()) for name in ("pyproject.toml", "requirements.lock")}}
    (output/"manifest.json").write_bytes((json.dumps(manifest, indent=2)+"\n").encode())
    (output/"install.py").write_bytes((ROOT/"scripts/status_refresh_image_install.py").read_bytes())
    (output/"Dockerfile").write_bytes(('FROM '+PARENT+'\nUSER root\nCOPY . /tmp/recovery-payload\n'
        'RUN python /tmp/recovery-payload/install.py /tmp/recovery-payload\nUSER ticketing\n').encode())
    return {"output": str(output), "parent_image_inspect_id": info["Id"], "modules": len(runtime),
            "changed_modules": sorted(CHANGED), "cloud_calls": 0}


if __name__ == "__main__":
    print(json.dumps(prepare()))
