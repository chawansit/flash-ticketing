"""ADR0364 local-only immutable-parent image preparation; no cloud calls."""
import argparse
import json
import subprocess
from pathlib import Path
from uuid import uuid4

import cce_projection_refresh_pipeline as parent
from persistent_uuid_overlay import MODULES, PARENT_SOURCE, overlay
from prepare_shared_application_image import ROOT, docker, sha

VERIFY = """
import hashlib,inspect,json,os
from pathlib import Path
from ticketing.config import Settings
import ticketing.domain as domain
m=json.loads(Path('/app/shared-image.json').read_text())
assert m['decision']=='ADR0364' and m['parent_source_identity_sha256']==PARENT
assert len(m['runtime_source_sha256'])==22 and len(m['changed_modules'])==5
for name,want in m['runtime_source_sha256'].items():
 assert hashlib.sha256((Path('/app')/name).read_bytes()).hexdigest()==want
assert hashlib.sha256(Path(domain.__file__).read_bytes()).hexdigest()==m['runtime_source_sha256']['src/ticketing/domain.py']
assert not Settings().persistent_uuid_v7 and domain.persistent_uuid().version==4
os.environ['PERSISTENT_UUID_V7']='1'
assert domain.persistent_uuid().version==7
print(json.dumps({'runtime_modules_verified':22,'default_off':True,'enabled_uuid_version':7,'installed_module_matches':True}))
""".replace("PARENT", repr(PARENT_SOURCE))


def prepare(output):
    receipt = parent.image()
    image = parent.MANIFEST
    info = json.loads(docker("image", "inspect", image))[0]
    if (info["Id"] != image or receipt["source_identity_sha256"] != PARENT_SOURCE
            or (info["Os"], info["Architecture"], info["Config"]["User"]) != ("linux", "amd64", "ticketing")):
        raise ValueError("Exact cached unprivileged combined image required")
    output.mkdir(parents=True, exist_ok=False)
    container = docker("create", "--network", "none", "--entrypoint", "true", image)
    try:
        docker("cp", container + ":/app/src", str(output / "src"))
        for name in ("requirements.lock", "pyproject.toml"):
            docker("cp", container + ":/app/" + name, str(output / name))
    finally:
        docker("rm", container)
    files = {p.relative_to(output).as_posix(): p.read_bytes()
             for p in (output / "src/ticketing").rglob("*.py")}
    candidate = overlay(files, receipt["runtime_source_sha256"])
    for name,raw in candidate.items():
        (output / name).write_bytes(raw)
    runtime = {name:sha(raw) for name,raw in candidate.items()}
    manifest = {"decision":"ADR0364", "parent_local_image_id":image,
                "parent_registry_image":receipt["registry_image"],
                "parent_source_identity_sha256":PARENT_SOURCE,
                "parent_runtime_source_sha256":receipt["runtime_source_sha256"],
                "runtime_source_sha256":runtime,
                "source_identity_sha256":sha(json.dumps(runtime,sort_keys=True).encode()),
                "changed_modules":sorted(MODULES),
                "changed_factor":"time_ordered_persistent_identifiers",
                "dependency_input_sha256":receipt["dependency_input_sha256"]}
    (output / "manifest.json").write_bytes((json.dumps(manifest,indent=2)+"\n").encode())
    (output / "install.py").write_bytes((ROOT / "scripts/status_refresh_image_install.py").read_bytes())
    (output / ".dockerignore").write_bytes(b"**/__pycache__\n**/*.pyc\n")
    # Local tag is rechecked against the immutable ID before and after build.
    local_parent = "flash-ticketing-parent:adr0364"
    docker("tag", image, local_parent)
    (output / "Dockerfile").write_text(
        f"FROM {local_parent}\nUSER root\nCOPY . /tmp/claim-payload\n"
        "RUN python /tmp/claim-payload/install.py /tmp/claim-payload "
        "&& cp /tmp/claim-payload/manifest.json /app/shared-image.json "
        "&& rm -r /tmp/claim-payload\n"
        f'LABEL org.flash-ticketing.decision="ADR0364" org.flash-ticketing.source-sha256="{manifest["source_identity_sha256"]}"\n'
        "USER ticketing\n", encoding="utf-8")
    return manifest, local_parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--build", action="store_true")
    args = parser.parse_args()
    if args.receipt.exists() or not args.receipt.resolve().is_relative_to((ROOT / "tmp").resolve()):
        raise ValueError("Fresh owned private receipt required")
    output = ROOT / "tmp" / ("adr0364-image-" + uuid4().hex[:12])
    manifest, local_parent = prepare(output)
    tag = "flash-ticketing-shared:adr0364-" + manifest["source_identity_sha256"][:12]
    receipt = {**manifest, "payload_path":str(output), "local_tag":tag,
               "preparation_recipe_sha256":sha(Path(__file__).read_bytes()),
               "overlay_sha256":sha((ROOT/"scripts/persistent_uuid_overlay.py").read_bytes()),
               "built":False, "registry_published":False, "cloud_deployed":False,
               "cloud_load_started":False, "capacity_improvement_measured":False}
    if args.build:
        assert json.loads(docker("image","inspect",local_parent))[0]["Id"] == parent.MANIFEST
        log = output.with_suffix(".build.log")
        with log.open("w",encoding="utf-8") as stream:
            subprocess.run(["docker","buildx","build","--platform=linux/amd64","--network=none",
                            "--provenance=false","--sbom=false",
                            "--output=type=docker,oci-mediatypes=false","--load","-t",tag,str(output)],
                           check=True,stdout=stream,stderr=stream)
        assert json.loads(docker("image","inspect",local_parent))[0]["Id"] == parent.MANIFEST
        receipt["validation"] = json.loads(subprocess.check_output(
            ["docker","run","--rm","--network=none","--entrypoint","python",tag,"-c",VERIFY],text=True))
        info = json.loads(docker("image","inspect",tag))[0]
        if (info["Os"],info["Architecture"],info["Config"]["User"]) != ("linux","amd64","ticketing"):
            raise ValueError("Unprivileged linux/amd64 result required")
        receipt.update(built=True,local_image_id=info["Id"])
    args.receipt.write_bytes((json.dumps(receipt,indent=2)+"\n").encode())
    print(json.dumps({"built":receipt["built"],"source":manifest["source_identity_sha256"],
                      "validation":receipt.get("validation"),"cloud_calls":0}))


if __name__ == "__main__":
    main()
