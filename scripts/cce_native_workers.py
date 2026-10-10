"""ADR0259 owner-bound worker pods using the shared application image."""
import base64
import copy
import json
import time
from urllib.parse import urlsplit, urlunsplit

import cce_api_adapter as cce
import cce_shared_worker_comparison as comparison
import cce_worker_identity as identity
import work_envelope as policy


def environment(role, values, primary_ip):
    if role not in identity.COUNTS or not isinstance(values, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in values.items()):
        raise ValueError("Resolved known worker environment required")
    result = copy.deepcopy(values)
    if any(key in result for key in ("CCE_RUN_ID", "CCE_POD_UID")):
        raise ValueError("Reserved worker identity environment collision")
    db = urlsplit(result["DATABASE_URL"])
    if db.hostname != "pgbouncer" or db.port != 5432 or db.scheme not in {"postgres", "postgresql"} or "@" not in db.netloc:
        raise ValueError("Worker must preserve existing pooler authority/options")
    result["DATABASE_URL"] = urlunsplit(db._replace(netloc=db.netloc.rsplit("@", 1)[0] + "@" + cce.private_ip(primary_ip) + ":6432"))
    if role == "simulator":
        if result.get("API_URL") != "http://load-balancer:8000":
            raise ValueError("Shared callback route required")
        result["API_URL"] = "http://" + cce.private_ip(primary_ip) + ":8000"
    return result


def objects(run, environments, primary_ip):
    if set(environments) != set(identity.COUNTS):
        raise ValueError("Complete worker environments required")
    namespace = cce.dependency.namespace_for(run)
    result = []
    sources = comparison.selected_receipt()["runtime_source_sha256"]
    for role, count in identity.COUNTS.items():
        env = environment(role, environments[role], primary_ip)
        command = ["python", "-m", "ticketing.workers", role]
        program = ("expected=" + repr(sources) + "\nenv_sha=" + repr(policy.digest(env)) + "\ncommand=" + repr(command)
                   + "\nenvironment_keys=" + repr(sorted(env)) + "\n" + cce.STARTUP.replace("CCE_API_STARTUP", "CCE_WORKER_STARTUP"))
        secret = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": role + "-env", "namespace": namespace, "labels": {"codex-owner": run}},
                  "type": "Opaque", "data": {k: base64.b64encode(v.encode()).decode() for k, v in env.items()}}
        result.append(secret)
        for index in range(count):
            result.append({"apiVersion": "v1", "kind": "Pod",
                           "metadata": {"name": f"{role}-{index}", "namespace": namespace, "labels": {"codex-owner": run, "codex-worker-role": role},
                                        "annotations": {"codex-environment-sha256": policy.digest(env), "codex-command-sha256": policy.digest(command)}},
                           "spec": {"restartPolicy": "Never", "activeDeadlineSeconds": 3000,
                                    "automountServiceAccountToken": False, "enableServiceLinks": False,
                                    "imagePullSecrets": [{"name": "default-secret"}, {"name": "swr-pull"}],
                                    "hostAliases": [{"ip": cce.private_ip(primary_ip), "hostnames": ["kafka"]}],
                                    "securityContext": {"runAsUser": 10001, "runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
                                    "containers": [{"name": "worker", "image": comparison.IMAGE, "imagePullPolicy": "Always",
                                                    "command": ["python", "-u", "-c", program], "workingDir": "/app",
                                                    "envFrom": [{"secretRef": {"name": role + "-env"}}],
                                                    "env": [{"name": "CCE_RUN_ID", "value": run}, {"name": "CCE_POD_UID", "valueFrom": {"fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.uid"}}}],
                                                    "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                                                    "resources": copy.deepcopy(identity.RESOURCES), "ports": [{"containerPort": 9101, "protocol": "TCP"}]}]}})
    return result


def receipt(pod, expected, run, uid, log, metrics):
    meta, spec, status = pod.get("metadata", {}), pod.get("spec", {}), pod.get("status", {})
    wanted = expected["spec"]
    states = status.get("containerStatuses", [])
    if (not uid or meta.get("uid") != uid or meta.get("name") != expected["metadata"]["name"]
            or meta.get("deletionTimestamp") or any(meta.get("labels", {}).get(k) != v for k, v in expected["metadata"]["labels"].items())
            or any(meta.get("annotations", {}).get(k) != v for k, v in expected["metadata"]["annotations"].items())
            or any(spec.get(k) != v for k, v in wanted.items() if k != "containers")
            or len(spec.get("containers", [])) != 1
            or any(spec["containers"][0].get(k) != v for k, v in wanted["containers"][0].items())
            or status.get("phase") != "Running" or len(states) != 1
            or states[0].get("name") != "worker" or states[0].get("restartCount") != 0
            or type(states[0].get("restartCount")) is not int or states[0].get("ready") is not True
            or not states[0].get("state", {}).get("running", {}).get("startedAt")
            or states[0].get("imageID", "").split("@")[-1] != identity.MANIFEST):
        raise ValueError("Worker admitted specification/runtime drift")
    if not isinstance(log, str) or len(log.encode()) > 65536:
        raise ValueError("Bounded worker startup log required")
    prefix = "CCE_WORKER_STARTUP "
    proofs = [json.loads(line[len(prefix):]) for line in log.splitlines() if line.startswith(prefix)]
    if len(proofs) != 1:
        raise ValueError("Exactly one worker startup proof required")
    proof = proofs[0]
    if any(proof.get(k) != expected["metadata"]["annotations"][a] for k, a in (("environment_sha256", "codex-environment-sha256"), ("command_sha256", "codex-command-sha256"))):
        raise ValueError("Worker startup environment/command differs")
    result = {"role": meta["labels"]["codex-worker-role"], "pod_name": meta["name"], "pod_uid": uid,
              "private_ipv4": cce.private_ip(status["podIP"]), "image_id": states[0]["imageID"],
              "started_at": states[0]["state"]["running"]["startedAt"], "resources": spec["containers"][0]["resources"],
              "startup_proof": proof, "process_start_time_seconds": metrics.get("process_start_time_seconds")}
    return result


class Workers:
    def __init__(self, deployment, manifests, read_metrics, persist):
        self.deployment, self.manifests, self.read_metrics, self.persist = deployment, manifests, read_metrics, persist
        self.uids = {}
        self.attempted = False
        self.bundle = None

    def create(self):
        self.deployment.verify_owner()
        self.deployment.authorize()
        if self.attempted:
            raise ValueError("Fresh worker creation required")
        self.attempted = True
        self.persist({"worker_creation_attempted": True})
        for item in self.manifests:
            resource = "pods" if item["kind"] == "Pod" else "secrets"
            value = self.deployment.mutation("POST", self.deployment.path() + "/" + resource, item)
            if resource == "pods":
                self.uids[item["metadata"]["name"]] = value["metadata"]["uid"]
                self.persist({"worker_pod_uids": dict(self.uids)})
        return self.observe()

    def observe(self, *, previous=None):
        deadline = time.monotonic() + 180
        pods = [item for item in self.manifests if item["kind"] == "Pod"]
        if set(self.uids) != {item["metadata"]["name"] for item in pods} or len(pods) != 13:
            raise ValueError("Complete captured native workers required")
        while True:
            self.deployment.verify_owner()
            self.deployment.authorize()
            views, pending = [], False
            for expected in pods:
                name = expected["metadata"]["name"]
                path = self.deployment.path() + "/pods/" + name
                pod = self.deployment.request("GET", path, None)
                if pod is None or pod.get("metadata", {}).get("uid") != self.uids[name]:
                    raise ValueError("Worker pod replaced or disappeared")
                states = pod.get("status", {}).get("containerStatuses", [])
                if any(s.get("restartCount", 0) or s.get("state", {}).get("terminated") for s in states):
                    raise ValueError("Worker startup terminated or restarted")
                if not states or pod.get("status", {}).get("phase") != "Running" or not all(s.get("ready") is True for s in states):
                    pending = True
                    continue
                log = self.deployment.request("GET", path + "/log?container=worker&limitBytes=65536", None)
                address = pod["status"]["podIP"]
                try:
                    metrics = self.read_metrics(address)
                except (TimeoutError, OSError):
                    if previous is not None:
                        raise
                    pending = True
                    continue
                views.append(receipt(pod, expected, self.deployment.run, self.uids[name], log, metrics))
            if not pending:
                bundle = {"decision": "ADR0259", "run": self.deployment.run,
                          "sources": comparison.selected_receipt()["runtime_source_sha256"],
                          "manifest_digest": identity.MANIFEST, "resources": identity.RESOURCES, "receipts": views}
                identity.validate_bundle(bundle)
                if previous is not None and bundle != previous:
                    raise ValueError("Native workers changed since admission")
                self.bundle = bundle
                self.persist({"native_worker_receipts": bundle})
                return bundle
            if previous is not None or time.monotonic() >= deadline:
                raise TimeoutError("Worker startup/readiness deadline exceeded")
            time.sleep(2)
