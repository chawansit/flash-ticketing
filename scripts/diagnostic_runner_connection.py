"""ADR0181 protected target, owned transfer, preflight and mandatory cleanup."""
import hashlib
import ipaddress
import json
import posixpath
import re
from dataclasses import dataclass, field
from types import MappingProxyType

from diagnostic_connection import identity
from kafka_lag_observe import source_sha256
from qualify_two_host_deployment import ROOT

FILES = ("diagnostic.private.json", "diagnostic-ca.pem")
TRANSFER_FILES = ("observe_two_host_pipeline.py", "prepare_two_host_scaling.py",
                  "database_wait_evidence.py", "diagnostic_connection.py")
LEDGER = "bounded_diagnostic_placement"


def validate_target(value):
    keys = {"host", "port", "dbname", "user", "ca_source_path", "ca_sha256"}
    if not isinstance(value, (dict, MappingProxyType)) or set(value) != keys:
        raise ValueError("Exact password-free diagnostic target required")
    if (not isinstance(value["host"], str) or not ipaddress.IPv4Address(value["host"]).is_private
            or type(value["port"]) is not int or value["port"] != 5432 or value["user"] != "root"
            or not isinstance(value["dbname"], str) or not re.fullmatch(r"[a-zA-Z0-9_]{1,63}", value["dbname"])
            or not isinstance(value["ca_source_path"], str) or not value["ca_source_path"].startswith("/root/")
            or posixpath.normpath(value["ca_source_path"]) != value["ca_source_path"]
            or not isinstance(value["ca_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", value["ca_sha256"])):
        raise ValueError("Diagnostic target, role or trusted certificate differs")
    return dict(value)


@dataclass(repr=False)
class ProtectedContext:
    target: dict
    password: str = field(repr=False)

    def __post_init__(self):
        self.target = MappingProxyType(validate_target(self.target))
        if not isinstance(self.password, str) or not self.password:
            raise ValueError("Protected diagnostic credential required")

    def __repr__(self):
        return "ProtectedContext(<protected>)"

    def clear(self):
        self.password = None


def cleanup_program(directory):
    # Only exact filenames in the existing exclusively owned stage are removed.
    return ("import json,os,stat;from pathlib import Path;p=Path(" + repr(directory) + ")\n"
            "if p.is_symlink() or not p.is_dir() or p.stat().st_uid!=os.geteuid() or stat.S_IMODE(p.stat().st_mode)!=0o700:raise ValueError('Diagnostic cleanup ownership differs')\n"
            "for name in " + repr(FILES) + ":\n"
            " f=p/name\n"
            " if f.is_symlink():raise ValueError('Unsafe diagnostic cleanup link')\n"
            " if f.exists():\n"
            "  m=f.stat()\n"
            "  if not stat.S_ISREG(m.st_mode) or m.st_uid!=os.geteuid() or m.st_nlink!=1 or stat.S_IMODE(m.st_mode)!=0o600:raise ValueError('Diagnostic cleanup file ownership differs')\n"
            "  f.unlink()\n"
            "assert all(not (p/name).exists() and not (p/name).is_symlink() for name in " + repr(FILES) + ")\n"
            "print(json.dumps({'diagnostic_credentials_removed':True}))")


def cleanup(session, cid, owner, directory):
    # Attempt both sites even when either site fails; never hide uncertainty.
    failed = []
    for operation in (lambda: session.api(cid, cleanup_program(directory), 45),
                      lambda: session.call("primary", cleanup_program(owner), 45)):
        try:
            if operation() != {"diagnostic_credentials_removed": True}:
                failed.append("InvalidCleanupProof")
        except BaseException as exc:  # noqa: BLE001 - both sites must be attempted during interruption
            failed.append(type(exc).__name__)
    if failed:
        raise RuntimeError("Diagnostic credential cleanup requires recovery")
    return {"diagnostic_credentials_removed": True}


def prepare(session, cid, owner, directory, inventory, upload, context):
    if not isinstance(context, ProtectedContext) or not context.password:
        raise ValueError("Protected diagnostic context missing before staging")
    target = validate_target(context.target)
    # Mark owned cleanup before the first transfer; a partial SFTP write is recoverable.
    expected = {}
    for name in TRANSFER_FILES:
        content = (ROOT / "scripts" / name).read_text(encoding="utf-8")
        expected[name] = source_sha256(content.encode())
        upload(session, cid, owner, directory, name, content)
    proof = session.api(cid, "import hashlib,json;from pathlib import Path;p=Path(" + repr(directory)
                        + ");expected=" + repr(expected)
                        + ";assert {k:hashlib.sha256((p/k).read_bytes().replace(b'\\r\\n',b'\\n')).hexdigest() for k in expected}==expected;print(json.dumps({'diagnostic_helpers_verified':True}))", 45)
    if proof != {"diagnostic_helpers_verified": True}:
        raise ValueError("Diagnostic transferred source differs")
    # Trust the exact already mounted CA, never an arbitrary freshly supplied certificate.
    from qualify_two_host_deployment import INSPECT
    rows = session.call("primary", INSPECT, 45)
    mounts = [m for row in rows for m in row.get("Mounts", [])
              if m.get("Destination") == "/etc/pgbouncer/rds-ca.pem"]
    if len(mounts) != 1 or mounts[0].get("Source") != target["ca_source_path"]:
        raise ValueError("Existing trusted CA mount differs")
    transport = session.clients["primary"].open_sftp()
    try:
        with transport.open(target["ca_source_path"], "rb") as handle:
            certificate = handle.read(262145)
    finally:
        transport.close()
    if (not 0 < len(certificate) <= 262144 or b"BEGIN CERTIFICATE" not in certificate
            or hashlib.sha256(certificate).hexdigest() != target["ca_sha256"]):
        raise ValueError("Existing CA trust binding differs")
    value = {k: target[k] for k in ("host", "port", "dbname", "user")}
    value.update(password=context.password, sslmode="verify-full", sslrootcert=directory + "/diagnostic-ca.pem")
    role_identity = identity(target["user"], target["dbname"])
    # The exact POSIX file specification is checked inside the remote preflight.
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"))
    binding = {"decision": "ADR0180", "database": target["dbname"], "identity_sha256": role_identity,
               "endpoint": [target["host"], target["port"]], "bundle_sha256": hashlib.sha256(raw.encode()).hexdigest(),
               "ca_sha256": target["ca_sha256"]}
    inventory["diagnostic_connection_binding"] = binding
    upload(session, cid, owner, directory, "diagnostic-ca.pem", certificate.decode("ascii"))
    upload(session, cid, owner, directory, "diagnostic.private.json", raw)
    # Existing upload transport is SFTP then docker cp; secrets never enter argv/env.
    from status_refresh_contract import digest
    upload(session, cid, owner, directory, "diagnostic-preflight-inventory.private.json", json.dumps(inventory))
    program = ("import os,sys,json;from pathlib import Path;from types import SimpleNamespace;sys.path.insert(0,"
               + repr(directory) + ")\n"
               "import psycopg,observe_two_host_pipeline as observer,database_wait_evidence as evidence\n"
               "p=Path(" + repr(directory) + ");inventory=json.loads((p/'diagnostic-preflight-inventory.private.json').read_text())\n"
               "os.environ['TEST_DATABASE_URL']=os.environ['DATABASE_URL']\n"
               "args=SimpleNamespace(inventory=p/'diagnostic-preflight-inventory.private.json',diagnostic_connection_bundle=p/'diagnostic.private.json',database_wait_diagnostics=True,approved_inventory_sha256=" + repr(digest(inventory)) + ")\n"
               "module=SimpleNamespace(psycopg=psycopg);adapter=observer.install_diagnostic_connection(module,inventory,args)\n"
               "with module.psycopg.connect(os.environ['DATABASE_URL'],autocommit=True) as conn:\n"
               " collector=evidence.Collector();records=[collector.collect(conn) for _ in range(2)]\n"
               " assert all(row['complete'] for row in records),'Diagnostic preflight incomplete'\n"
               " assert conn.execute('SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()').fetchone()[0] is True\n"
               "assert adapter.connections_created==1\n"
               "print(json.dumps({'pass':True,'diagnostic_helpers_verified':True,'read_only_connection_verified':True,'verified_tls':True,'one_data_connection':True,'identity_sha256':" + repr(role_identity) + ",'max_collection_ms':max(row['collection_ms'] for row in records)}))")
    preflight = session.api(cid, program, 45)
    if (any(preflight.get(k) is not True for k in ("pass", "diagnostic_helpers_verified", "read_only_connection_verified", "verified_tls", "one_data_connection"))
            or preflight.get("identity_sha256") != role_identity):
        raise ValueError("Exact read-only diagnostic preflight proof required")
    return {"diagnostic_connection_preflight": preflight, **proof}
