"""ADR0153: stage immutable image caches only; never recreate services or dispatch customers."""

import argparse
import getpass
import hashlib
import json
import re
import sys
import time
from pathlib import Path

from fetch_status_refresh_parents import (
    MAX_ARCHIVE,
    cleanup_program,
    inventory_program,
    new_owner_name,
    owner_path,
    parents_from_inventory,
    validate_owner,
)
from prepare_status_refresh_artifacts import command, inspect_image, owned_output
from qualify_two_host_deployment import GENERATOR_IDLE, ROOT, Session
from run_status_refresh_comparison import STATE, binding_for, validate_release
from run_two_host_paid_comparison import validate_config
from status_refresh_contract import ROLES, StatusRefreshContract, source_contract


def new_stage_output():
    """Generate the exact directory name accepted by the shared ownership validator."""
    return ROOT / "tmp" / new_owner_name()


def hash_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024**2), b""):
            digest.update(block)
    return digest.hexdigest()


def secondary_inventory():
    return r"""import json,subprocess
ids=subprocess.check_output(['docker','ps','-aq','--no-trunc','--filter','label=com.docker.compose.project=flash-ticketing-api-secondary'],text=True).split()
rows=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
print(json.dumps({'containers':[{'id':r['Id'],'image':r['Image'],'started_at':r['State']['StartedAt'],'running':r['State']['Running']} for r in sorted(rows,key=lambda r:r['Id'])]}))
"""


def create_owner_program(owner, size):
    validate_owner(owner)
    if type(size) is not int or not 0 < size <= MAX_ARCHIVE:
        raise ValueError("Bounded archive required")
    return r"""import json,shutil
from pathlib import Path
owner=Path(OWNER)
repo=owner.parent.parent
if repo.resolve()!=repo or not repo.is_dir():raise ValueError('Exact repository directory required')
if owner.parent.is_symlink():raise ValueError('Symlink tmp rejected')
owner.parent.mkdir(mode=0o700,exist_ok=True)
if owner.parent.resolve()!=owner.parent or owner.exists() or owner.is_symlink():raise ValueError('Fresh owner required')
if shutil.disk_usage(owner.parent).free<SIZE+512*1024**2:raise ValueError('Archive space insufficient')
owner.mkdir(mode=0o700)
print(json.dumps({'owner':str(owner),'created':True}))
""".replace("OWNER", repr(owner)).replace("SIZE", str(size))


def seal_program(owner, size, digest):
    validate_owner(owner)
    if type(size) is not int or not 0 < size <= MAX_ARCHIVE or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("Exact bounded digest required")
    return (
        r"""import hashlib,json,stat
from pathlib import Path
owner=Path(OWNER)
if owner.is_symlink() or owner.resolve()!=owner or sorted(p.name for p in owner.iterdir())!=['images.tar']:raise ValueError('Owned directory differs')
p=owner/'images.tar'
s=p.lstat()
if p.is_symlink() or not stat.S_ISREG(s.st_mode) or s.st_size!=SIZE or stat.S_IMODE(s.st_mode)!=0o600:raise ValueError('Archive identity differs')
h=hashlib.sha256()
with p.open('rb') as f:
 for block in iter(lambda:f.read(1024**2),b''):h.update(block)
if h.hexdigest()!=DIGEST:raise ValueError('Archive digest differs')
seal={'dev':s.st_dev,'ino':s.st_ino,'size':s.st_size,'mtime_ns':s.st_mtime_ns,'uid':s.st_uid,'mode':stat.S_IMODE(s.st_mode)}
print(json.dumps({'owner':str(owner),'sha256':h.hexdigest(),'seal':seal}))
""".replace("OWNER", repr(owner))
        .replace("SIZE", str(size))
        .replace("DIGEST", repr(digest))
    )


def load_program(seal, images):
    validate_owner(seal["owner"])
    if not images or any(not re.fullmatch(r"sha256:[0-9a-f]{64}", i) for i in images):
        raise ValueError("Exact immutable images required")
    # Re-seal immediately before load; the receipt used for cleanup remains the same.
    proof = seal_program(seal["owner"], seal["seal"]["size"], seal["sha256"])
    proof = proof[: proof.rindex("print(json.dumps(")]
    return (
        proof
        + r"""
if seal!=SEAL:raise ValueError('Archive seal changed before load')
import subprocess
subprocess.run(['docker','image','load','--input',str(p)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=180)
rows=json.loads(subprocess.check_output(['docker','image','inspect',*IMAGES],text=True,timeout=30))
if {r['Id'] for r in rows}!=set(IMAGES):raise ValueError('Loaded image identities differ')
print(json.dumps({'loaded_images':IMAGES}))
""".replace("SEAL", repr(seal["seal"])).replace("IMAGES", repr(sorted(set(images))))
    )


def upload(session, role, owner, path, size, digest, *, action_guard=None):
    validate_owner(owner)
    if path.is_symlink() or path.stat().st_size != size or not 0 < size <= MAX_ARCHIVE:
        raise ValueError("Exact local archive required")
    if action_guard is not None:
        action_guard.check(240)
    start, progress, sent, computed = time.monotonic(), time.monotonic(), 0, hashlib.sha256()
    sftp = session.clients[role].open_sftp()
    try:
        sftp.get_channel().settimeout(60)
        remote = owner + "/images.tar"
        with path.open("rb") as source, sftp.open(remote, "wx") as destination:
            sftp.chmod(remote, 0o600)
            destination.set_pipelined(True)
            for block in iter(lambda: source.read(1024**2), b""):
                if action_guard is not None:
                    action_guard.check(60)
                sent += len(block)
                if sent > size or time.monotonic() - start > 240:
                    raise ValueError("Transfer size/time budget exceeded")
                computed.update(block)
                destination.write(block)
                if time.monotonic() - progress >= 20:
                    print(
                        json.dumps(
                            {"phase": "image-upload", "role": role, "bytes": sent, "total_bytes": size}
                        ),
                        flush=True,
                    )
                    progress = time.monotonic()
        if sent != size or computed.hexdigest() != digest:
            raise ValueError("Transferred source changed")
    finally:
        sftp.close()
    return {"bytes": sent, "sha256": computed.hexdigest()}


def stage(config, artifact, output, password):
    validate_config(config)
    sources = source_contract()
    contract = StatusRefreshContract(artifact, "control", sources)
    validate_release(json.loads(STATE.read_text()), binding_for(config, artifact, sources), execute=False)
    output = owned_output(output)
    result = {"pass": False, "customer_dispatches": 0, "service_deployments": 0, "hosts": {}}
    session = None
    try:
        # Verify real local receipt metadata again before creating any remote resource.
        command(
            [sys.executable, "-c", contract.image_program(ROLES)],
            log=output / "local-images.txt",
            timeout=120,
        )
        archives = {}
        for role, images in (
            ("primary", sorted(set(contract.images.values()))),
            ("secondary", sorted({contract.images["api"], contract.parents["api"]})),
        ):
            if sum(inspect_image(i)["Size"] for i in images) > MAX_ARCHIVE:
                raise ValueError("Bounded image archive required")
            path = output / (role + ".tar")
            command(
                ["docker", "image", "save", "--output", str(path), *images],
                log=output / (role + "-save.txt"),
                timeout=180,
            )
            size = path.stat().st_size
            if not 0 < size <= MAX_ARCHIVE:
                raise ValueError("Bounded local archive required")
            archives[role] = (path, images, size, hash_file(path))
        session = Session(config, output, password)
        session.phase("staging-runtime-preflight")
        before = session.call("primary", inventory_program(config["primary"]["repo"]), 45)
        if parents_from_inventory(before) != getattr(contract, "original_parents", contract.parents):
            raise ValueError("Original runtime parents differ")
        secondary_before = session.call("secondary", secondary_inventory(), 30)
        if secondary_before["containers"]:
            raise ValueError("Secondary test project must be absent before staging")
        if session.call("generator", GENERATOR_IDLE, 30).get("generator_idle") is not True:
            raise ValueError("Generator must be idle")
        for role, (path, images, size, digest) in archives.items():
            repo = config[role]["repo"] if role == "primary" else config[role]["prepared_directory"]
            owner = owner_path(repo, output.name)
            host = result["hosts"][role] = {"owner": owner, "images": images, "sha256": digest, "bytes": size}
            session.phase(role + "-fresh-owned-upload")
            created = session.call(role, create_owner_program(owner, size), 30)
            if created != {"owner": owner, "created": True}:
                raise ValueError("Owner receipt differs")
            host["transfer"] = upload(session, role, owner, path, size, digest)
            seal = session.call(role, seal_program(owner, size, digest), 60)
            if (
                seal.get("owner") != owner
                or seal.get("sha256") != digest
                or seal.get("seal", {}).get("size") != size
            ):
                raise ValueError("Archive receipt differs")
            (output / (role + "-seal.private.json")).write_text(json.dumps(seal, indent=2) + "\n")
            session.phase(role + "-verified-image-cache-load")
            loaded = session.call(role, load_program(seal, images), 240)
            if loaded != {"loaded_images": images}:
                raise ValueError("Loaded image receipt differs")
            host["artifact_proof"] = session.call(
                role, contract.image_program(contract.roles if role == "primary" else ("api",)), 120
            )
            host.update(session.call(role, cleanup_program(seal), 30))
        if before != session.call("primary", inventory_program(config["primary"]["repo"]), 45):
            raise ValueError("Primary runtime changed during staging")
        if secondary_before != session.call("secondary", secondary_inventory(), 30):
            raise ValueError("Secondary runtime changed during staging")
        if session.call("generator", GENERATOR_IDLE, 30).get("generator_idle") is not True:
            raise ValueError("Generator no longer idle")
        result.update({"pass": True, "runtime_identities_unchanged": True, "generator_idle": True})
    except (Exception, KeyboardInterrupt) as exc:  # No replay or uncertain cleanup.
        result["failure_type"] = type(exc).__name__
        raise
    finally:
        if session:
            session.close()
        password = None
        (output / "staging-summary.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--ssh-runtime", type=Path, required=True)
    args = parser.parse_args()
    if not sys.stdin.isatty():
        raise ValueError("Protected terminal required")
    sys.path.insert(0, str(args.ssh_runtime.resolve()))
    output = new_stage_output()
    result = stage(
        json.loads(args.config.read_text()),
        json.loads(args.artifact.read_text()),
        output,
        getpass.getpass("ECS password: "),
    )
    print(
        json.dumps(
            {"phase": "images-staged", "pass": result["pass"], "output": output.relative_to(ROOT).as_posix()}
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
