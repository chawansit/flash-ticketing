"""ADR0363 authenticated immutable image publication after executed local checks."""
import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

from prepare_projection_refresh_pipeline_image import VERIFY
from prepare_shared_application_image import ROOT

REPOSITORY = "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing"


def run(*args):
    return subprocess.run(["docker",*args],check=True,text=True,capture_output=True,timeout=300)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image-receipt",type=Path,required=True)
    parser.add_argument("--qualification",type=Path,required=True)
    parser.add_argument("--latency-qualification",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--verify-existing",help="Resume only the retained successful push digest")
    args = parser.parse_args()
    if args.output.exists() or not args.output.resolve().is_relative_to((ROOT/"docs/capacity/cce").resolve()):
        raise ValueError("Fresh public publication receipt required")
    image,quality,latency = [json.loads(path.read_text(encoding="utf-8"))
                             for path in (args.image_receipt,args.qualification,args.latency_qualification)]
    if (not image["built"] or image["registry_published"] or not quality["integration_pass"]
            or not quality["cleanup_pass"] or not latency["benchmark_pass"] or not latency["cleanup_pass"]
            or any(v["source_identity_sha256"]!=image["source_identity_sha256"] for v in (quality,latency))
            or any(v["local_image_id"]!=image["local_image_id"] for v in (quality,latency))):
        raise ValueError("Executed source-bound local correctness and comparison evidence required")
    benchmark = latency["benchmark"]
    if (benchmark["candidate"]["median_ms"]>=benchmark["control"]["median_ms"]
            or benchmark["candidate"]["p95_ms"]>=benchmark["control"]["p95_ms"]
            or not benchmark["synthetic_one_way_ms"]):
        raise ValueError("Local latency comparison must preserve both median and tail")
    for name,key in (("prepare_projection_refresh_pipeline_image.py","preparation_recipe_sha256"),
                     ("projection_refresh_pipeline_overlay.py","overlay_sha256")):
        if hashlib.sha256((ROOT/"scripts"/name).read_bytes().replace(b"\r\n",b"\n")).hexdigest()!=image[key]:
            raise ValueError("Qualified image preparation changed")
    local = json.loads(run("image","inspect",image["local_image_id"]).stdout)[0]
    if local["Id"]!=image["local_image_id"]:
        raise ValueError("Local candidate changed")
    tag = REPOSITORY+":adr0363-"+image["source_identity_sha256"][:12]
    push_log = ROOT/"tmp/adr0363-push.private.log"
    if args.verify_existing:
        if not re.fullmatch(r"sha256:[0-9a-f]{64}",args.verify_existing) or not push_log.is_file() or push_log.is_symlink():
            raise ValueError("Exact retained push digest required")
        pushed_output = push_log.read_text(encoding="utf-8")
    else:
        run("tag",image["local_image_id"],tag)
        pushed = run("push",tag)
        pushed_output = pushed.stdout+pushed.stderr
        push_log.write_text(pushed_output,encoding="utf-8")
    matches = re.findall(r"digest:\s+(sha256:[0-9a-f]{64})",pushed_output)
    if not matches:
        raise ValueError("Actual registry manifest digest missing; inspect retained push log")
    digest = matches[-1]
    if args.verify_existing and (digest!=args.verify_existing or tag.rsplit(":",1)[1]+":" not in pushed_output):
        raise ValueError("Retained source-tag push does not match")
    immutable = REPOSITORY+"@"+digest
    manifest = json.loads(run("manifest","inspect",immutable).stdout)
    config = manifest["config"]["digest"]
    run("pull",immutable)
    pulled = json.loads(run("image","inspect",immutable).stdout)[0]
    verified = json.loads(run("run","--rm","--network=none","--entrypoint","python",
                              immutable,"-c",VERIFY).stdout)
    if (immutable not in pulled["RepoDigests"] or pulled["Id"]!=image["local_image_id"]
            or (pulled["Os"],pulled["Architecture"],pulled["Config"]["User"])!=("linux","amd64","ticketing")
            or verified!={"runtime_modules_verified":22,"projection_transaction_pipeline":True,"installed_module_matches":True}):
        raise ValueError("Pulled immutable runtime differs")
    result = {k:image[k] for k in ("parent_source_identity_sha256","parent_runtime_source_sha256",
                                  "runtime_source_sha256","source_identity_sha256","dependency_input_sha256",
                                  "changed_modules","preparation_recipe_sha256","overlay_sha256","local_image_id")}
    result.update(decision="ADR0363",registry_image=immutable,registry_manifest_digest=digest,
                  registry_configuration_digest=config,registry_pulled_image_id=pulled["Id"],
                  registry_published=True,permission_verified=True,registry_pulled_runtime_verified=True,
                  validation=verified,platform={"os":"linux","architecture":"amd64"},runtime_user="ticketing",
                  cloud_load_started=False,capacity_improvement_measured=False)
    args.output.write_bytes((json.dumps(result,indent=2)+"\n").encode())
    print(json.dumps({"published":True,"manifest":digest,"source":image["source_identity_sha256"],
                      "pulled_modules_verified":22,"cloud_traffic_started":False}))


if __name__ == "__main__":
    main()
