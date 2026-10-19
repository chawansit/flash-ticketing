"""ADR0363 owned local integration/benchmark containers with unconditional cleanup."""
import argparse
import json
import re
import shutil
import subprocess
import time
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]


def docker(*args, check=True):
    return subprocess.run(["docker",*args],text=True,capture_output=True,check=check,timeout=90)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image-receipt",type=Path,required=True)
    parser.add_argument("--receipt",type=Path,required=True)
    parser.add_argument("--benchmark-only",action="store_true")
    parser.add_argument("--one-way-latency-ms",type=float,default=0)
    args = parser.parse_args()
    if not 0 <= args.one_way_latency_ms <= 5:
        raise ValueError("Bounded local synthetic latency required")
    if args.receipt.exists() or not args.receipt.resolve().is_relative_to((ROOT/"tmp").resolve()):
        raise ValueError("Fresh owned local receipt required")
    image = json.loads(args.image_receipt.read_text(encoding="utf-8"))
    if not image["built"] or image["decision"] != "ADR0363" or image["registry_published"]:
        raise ValueError("Unpublished qualified local candidate required")
    token = "adr0363-local-" + uuid4().hex[:12]
    output = ROOT/"tmp"/token
    output.mkdir()
    suite = output/"suite"
    (suite/"tests").mkdir(parents=True)
    names = ["conftest.py","test_event_consumer_separation.py","test_order_status_event_refresh.py","test_projection_refresh_pipeline.py"]
    for name in names:
        shutil.copyfile(ROOT/"tests/integration"/name,suite/"tests"/name)
    for name in ["benchmark_projection_refresh_pipeline.py","reservation_claim_latency_proxy.py"]:
        shutil.copyfile(ROOT/"scripts"/name,suite/name)
    owned = []
    result = {"decision":"ADR0363","source_identity_sha256":image["source_identity_sha256"],
              "local_image_id":image["local_image_id"],"local_only":True,
              "cloud_calls":0,"integration_pass":False,"benchmark_pass":False,"cleanup_pass":False}
    network = None

    def create(name, selected, options, command):
        container = docker("create","--name",name,"--network",token,
                           "--label","codex-owner="+token,*options,selected,*command).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{64}",container):
            raise ValueError("Exact local container identity required")
        row = json.loads(docker("inspect",container).stdout)[0]
        if row["Config"]["Labels"].get("codex-owner") != token:
            raise ValueError("Local ownership differs")
        owned.append(container)
        return container

    try:
        network = docker("network","create","--internal","--label","codex-owner="+token,token).stdout.strip()
        pg = create(token+"-postgres","postgres:17.6",
                    ["--network-alias","qualified-postgres","-e","POSTGRES_HOST_AUTH_METHOD=trust"],[])
        redis = create(token+"-redis","redis:7.4.5-alpine",
                       ["--network-alias","qualified-redis"],[])
        for container in (pg,redis):
            docker("start",container)
        deadline = time.monotonic()+45
        while True:
            ready = docker("exec",pg,"pg_isready","-U","postgres",check=False).returncode == 0
            ready = ready and docker("exec",redis,"redis-cli","ping",check=False).stdout.strip() == "PONG"
            if ready:
                break
            if time.monotonic()>deadline:
                raise TimeoutError("Owned local database readiness")
            time.sleep(.25)
        actual = json.loads(docker("image","inspect",image["local_image_id"]).stdout)[0]
        if actual["Id"] != image["local_image_id"]:
            raise ValueError("Candidate image identity changed")
        options = ["--entrypoint","python","--mount","type=bind,source="+str(suite)+",target=/qualified,readonly",
                   "-e","PYTHONPATH=/qualified:/app/src","-e","PYTHONDONTWRITEBYTECODE=1",
                   "-e","TEST_DATABASE_URL=postgresql://postgres@qualified-postgres:5432/postgres",
                   "-e","TEST_REDIS_URL=redis://qualified-redis:6379/0"]
        result["integration_executed"] = not args.benchmark_only
        if not args.benchmark_only:
            test = create(token+"-tests",image["local_image_id"],
                          options,
                          ["-m","pytest","-q","-p","no:cacheprovider","/qualified/tests"])
            print(json.dumps({"phase":"local-database-integration","owned":token}),flush=True)
            with (output/"integration.log").open("w",encoding="utf-8") as log:
                process = subprocess.run(["docker","start","-a",test],text=True,stdout=log,stderr=log,timeout=420,check=False)
            result["integration_pass"] = process.returncode == 0
            result["integration_log"] = str((output/"integration.log").relative_to(ROOT))
            if not result["integration_pass"]:
                raise ValueError("Local integration failed; inspect owned log")
        if args.one_way_latency_ms:
            proxy = create(token+"-proxy",image["local_image_id"],
                           [*options,"--network-alias","qualified-proxy",
                            "-e","QUALIFY_WIRE_ONE_WAY_MS="+str(args.one_way_latency_ms)],
                           ["/qualified/reservation_claim_latency_proxy.py"])
            docker("start",proxy)
            deadline = time.monotonic()+20
            while True:
                probe = docker("exec",proxy,"python","-c",
                               "import socket;socket.create_connection(('127.0.0.1',5432),1).close()",check=False)
                if probe.returncode == 0:
                    break
                if time.monotonic()>deadline:
                    raise TimeoutError("Owned latency proxy readiness")
                time.sleep(.1)
            options = [v.replace("postgres@qualified-postgres:5432","postgres@qualified-proxy:5432") for v in options]
        options += ["-e","QUALIFY_WIRE_ONE_WAY_MS="+str(args.one_way_latency_ms)]
        print(json.dumps({"phase":"local-projection-batch-comparison","owned":token}),flush=True)
        bench = create(token+"-benchmark",image["local_image_id"],options,
                       ["/qualified/benchmark_projection_refresh_pipeline.py"])
        process = subprocess.run(["docker","start","-a",bench],text=True,capture_output=True,timeout=180,check=False)
        (output/"benchmark.log").write_text(process.stderr,encoding="utf-8")
        if process.returncode:
            raise ValueError("Local benchmark failed; inspect owned log")
        result["benchmark"] = json.loads(process.stdout)
        result["benchmark_pass"] = True
    except BaseException as error:
        result["failure_type"] = type(error).__name__
        raise
    finally:
        failures = []
        for container in reversed(owned):
            row = docker("inspect",container,check=False)
            if row.returncode:
                continue
            value = json.loads(row.stdout)[0]
            if value["Id"] != container or value["Config"]["Labels"].get("codex-owner") != token:
                failures.append("identity")
                continue
            if docker("rm","-f","-v",container,check=False).returncode:
                failures.append("remove")
        if network:
            row = json.loads(docker("network","inspect",network).stdout)[0]
            if row["Labels"].get("codex-owner") != token or row.get("Containers"):
                failures.append("network_identity")
            elif docker("network","rm",network,check=False).returncode:
                failures.append("network_remove")
        result["cleanup_pass"] = not failures
        result["cleanup_failures"] = failures
        args.receipt.write_bytes((json.dumps(result,indent=2)+"\n").encode())
    print(json.dumps({"integration_executed":result["integration_executed"],
                      "integration_pass":result["integration_pass"],"benchmark":result["benchmark"],
                      "cleanup_pass":result["cleanup_pass"]}))


if __name__ == "__main__":
    main()
