"""ADR0153: explicit owned original-image retrieval, never deployment/load."""

import argparse
import getpass
import hashlib
import json
import re
import shutil
import sys
import time
from pathlib import Path, PurePosixPath
from uuid import uuid4

from prepare_status_refresh_artifacts import command, inspect_image, owned_output, validate_parents
from qualify_two_host_deployment import GENERATOR_IDLE, ROOT, Session
from run_two_host_paid_comparison import REVISION, validate_config
from two_host_topology import NORMAL_COUNTS

MAX_ARCHIVE = 3 * 1024**3


def inventory_program(repo):
    return r"""import json,subprocess
repo=REPO
ids=subprocess.check_output(['docker','ps','-q','--no-trunc','--filter','label=com.docker.compose.project=flash-ticketing'],text=True).split()
rows=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
groups={}
for row in rows:
 role=row['Config']['Labels']['com.docker.compose.service']
 groups.setdefault(role,[]).append({'id':row['Id'],'image':row['Image'],'started_at':row['State']['StartedAt']})
for group in groups.values():group.sort(key=lambda row:row['id'])
print(json.dumps({'revision':subprocess.check_output(['git','-C',repo,'rev-parse','HEAD'],text=True).strip(),'groups':groups}))
""".replace("REPO", repr(repo))


def parents_from_inventory(data):
    if (
        data.get("revision") != REVISION
        or {r: len(v) for r, v in data.get("groups", {}).items()} != NORMAL_COUNTS
    ):
        raise ValueError("Frozen normal primary topology required")
    parents = {}
    for role in ("api", "consumer", "publisher", "simulator", "maintenance", "reconciler"):
        rows = data["groups"][role]
        if len({r["image"] for r in rows}) != 1:
            raise ValueError("Same-role image drift")
        parents[role] = rows[0]["image"]
    parents["reservation-writer"] = parents["api"]
    validate_parents(parents)
    return parents


def owner_path(repo, name):
    base = PurePosixPath(repo)
    if (
        not base.is_absolute()
        or len(base.parts) < 3
        or ".." in base.parts
        or not re.fullmatch(r"adr0153-parents-[0-9a-f]{12}", name)
    ):
        raise ValueError("Owned absolute repository and run required")
    return str(base / "tmp" / name)


def validate_owner(owner):
    path = PurePosixPath(owner)
    if (
        not path.is_absolute()
        or ".." in path.parts
        or len(path.parts) < 5
        or path.parent.name != "tmp"
        or not re.fullmatch(r"adr0153-parents-[0-9a-f]{12}", path.name)
    ):
        raise ValueError("Exact owned remote export directory required")
    return str(path)


def export_program(owner, parents):
    owner = validate_owner(owner)
    validate_parents(parents)
    return (
        r"""import hashlib,json,os,shutil,stat,subprocess
from pathlib import Path
owner=Path(OWNER)
if owner.parent.resolve()!=owner.parent or owner.exists() or owner.is_symlink():raise ValueError('Fresh owned directory required')
images=sorted(set(PARENTS.values()))
rows=json.loads(subprocess.check_output(['docker','image','inspect',*images],text=True,timeout=30))
if {r['Id'] for r in rows}!=set(images):raise ValueError('Original image identities differ')
size=sum(r['Size'] for r in rows)
if size<=0 or size>MAXIMUM or shutil.disk_usage(owner.parent).free<size+512*1024**2:raise ValueError('Bounded archive space required')
owner.mkdir(mode=0o700)
archive=owner/'images.tar'
os.umask(0o077)
subprocess.run(['docker','image','save','--output',str(archive),*images],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=180)
archive.chmod(0o600)
s=archive.lstat()
if archive.is_symlink() or not stat.S_ISREG(s.st_mode) or not 0<s.st_size<=MAXIMUM:raise ValueError('Archive identity/size differs')
digest=hashlib.sha256()
with archive.open('rb') as f:
 for block in iter(lambda:f.read(1024**2),b''):digest.update(block)
seal={'dev':s.st_dev,'ino':s.st_ino,'size':s.st_size,'mtime_ns':s.st_mtime_ns,'uid':s.st_uid,'mode':stat.S_IMODE(s.st_mode)}
print(json.dumps({'owner':str(owner),'sha256':digest.hexdigest(),'seal':seal,'images':images}))
""".replace("OWNER", repr(owner))
        .replace("PARENTS", repr(parents))
        .replace("MAXIMUM", str(MAX_ARCHIVE))
    )


def cleanup_program(export):
    validate_owner(export["owner"])
    return r"""import json,stat
from pathlib import Path
owner=Path(OWNER)
if owner.is_symlink() or owner.resolve()!=owner or sorted(p.name for p in owner.iterdir())!=['images.tar']:raise ValueError('Owned archive directory differs')
archive=owner/'images.tar'
s=archive.lstat()
actual={'dev':s.st_dev,'ino':s.st_ino,'size':s.st_size,'mtime_ns':s.st_mtime_ns,'uid':s.st_uid,'mode':stat.S_IMODE(s.st_mode)}
if archive.is_symlink() or not stat.S_ISREG(s.st_mode) or actual!=SEAL:raise ValueError('Owned archive identity differs')
archive.unlink()
owner.rmdir()
print(json.dumps({'remote_archive_removed':True}))
""".replace("OWNER", repr(export["owner"])).replace("SEAL", repr(export["seal"]))


def transfer(session, export, target):
    size = export["seal"]["size"]
    if (
        type(size) is not int
        or not 0 < size <= MAX_ARCHIVE
        or not re.fullmatch(r"[0-9a-f]{64}", export["sha256"])
    ):
        raise ValueError("Bounded sealed archive required")
    if shutil.disk_usage(target.parent).free < size + 512 * 1024**2:
        raise ValueError("Local archive space insufficient")
    sftp = session.clients["primary"].open_sftp()
    digest, received, last_progress = hashlib.sha256(), 0, time.monotonic()
    try:
        sftp.get_channel().settimeout(90)
        with sftp.open(export["owner"] + "/images.tar", "rb") as source, target.open("xb") as destination:
            if source.stat().st_size != size:
                raise ValueError("Remote archive size changed")
            while True:
                block = source.read(1024**2)
                if not block:
                    break
                received += len(block)
                if received > size:
                    raise ValueError("Archive exceeded seal")
                digest.update(block)
                destination.write(block)
                if time.monotonic() - last_progress >= 20:
                    print(
                        json.dumps(
                            {"phase": "archive-transfer", "received_bytes": received, "total_bytes": size}
                        ),
                        flush=True,
                    )
                    last_progress = time.monotonic()
        if received != size or digest.hexdigest() != export["sha256"]:
            raise ValueError("Transferred archive hash/size differs")
    finally:
        sftp.close()
    return {"bytes": received, "sha256": digest.hexdigest()}


def retrieve(config, output, password):
    validate_config(config)
    state = json.loads((ROOT / "docs/capacity/CURRENT_STATE.json").read_text())
    if state.get("cloud_load_requires_resume") or state.get("current_run"):
        raise ValueError("Paused or active workflow; no artifact retrieval")
    output = owned_output(output)
    owner = owner_path(config["primary"]["repo"], output.name)
    session, result = (
        None,
        {"pass": False, "remote_owner": owner, "service_deployment": False, "customer_dispatches": 0},
    )
    try:
        session = Session(config, output, password)
        session.phase("original-parent-runtime-inspection")
        before = session.call("primary", inventory_program(config["primary"]["repo"]), 45)
        parents = parents_from_inventory(before)
        idle = session.call("generator", GENERATOR_IDLE, 30)
        if idle.get("generator_idle") is not True:
            raise ValueError("Generator must be idle")
        (output / "original-role-parents.json").write_text(json.dumps(parents, indent=2) + "\n")
        session.phase("bounded-original-image-export")
        export = session.call("primary", export_program(owner, parents), 240)
        if export.get("owner") != owner or set(export.get("images", [])) != set(parents.values()):
            raise ValueError("Remote export receipt ownership/images differ")
        (output / "export.private.json").write_text(json.dumps(export, indent=2) + "\n")
        session.phase("original-image-transfer")
        result["archive"] = transfer(session, export, output / "images.tar")
        session.phase("owned-remote-archive-cleanup")
        result.update(session.call("primary", cleanup_program(export), 30))
        after = session.call("primary", inventory_program(config["primary"]["repo"]), 45)
        if before != after:
            raise ValueError("Running container identity/revision changed during retrieval")
        result["primary_runtime_identity_unchanged"] = True
        session.phase("load-verified-originals-locally")
        command(
            ["docker", "load", "--input", str(output / "images.tar")],
            log=output / "docker-load.txt",
            timeout=180,
        )
        for image in set(parents.values()):
            inspect_image(image)
        result.update(pass_=True, distinct_originals=len(set(parents.values())), parent_images=parents)
        result["pass"] = result.pop("pass_")
    except (Exception, KeyboardInterrupt) as exc:  # Retain ambiguity; never replay export/transfer.
        result["failure_type"] = type(exc).__name__
        raise
    finally:
        if session:
            session.close()
        password = None
        (output / "retrieval-summary.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--ssh-runtime", type=Path, required=True)
    parser.add_argument("--private-fallback", action="store_true")
    args = parser.parse_args()
    if not sys.stdin.isatty():
        raise ValueError("Protected password terminal required")
    config = json.loads(args.config.read_text())
    if args.private_fallback:
        config["secondary_ssh_private_fallback"] = True
    sys.path.insert(0, str(args.ssh_runtime.resolve()))
    output = ROOT / "tmp" / ("adr0153-parents-" + uuid4().hex[:12])
    result = retrieve(config, output, getpass.getpass("ECS password: "))
    print(
        json.dumps(
            {
                "phase": "originals-retrieved",
                "pass": result["pass"],
                "distinct_originals": result["distinct_originals"],
                "output": output.relative_to(ROOT).as_posix(),
                "remote_archive_removed": result["remote_archive_removed"],
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
