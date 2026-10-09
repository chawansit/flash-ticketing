"""ADR0228 native CCE API adapter with pinned source, image and configuration checks."""

import base64
import copy
import ipaddress
import json
import math
import re
import time
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit, urlunsplit

import cce_dependency_probe as dependency
import work_envelope as policy
from cce_paid_profiles import for_guard
from prepare_two_host_scaling import API_SETTINGS

CONTRACT = policy.ROOT / "docs/capacity/cce/api-adapter-contract-2026-10-08.json"
PROFILE = "cce_paid_comparison"
PROOF_PREFIX = "CCE_API_STARTUP "


def legacy_contract():
    data = policy.read(CONTRACT)
    parent = policy.read(
        policy.ROOT / "docs/capacity/flash-sale-opening/generator-completion-probe-plan-2026-10-08.json"
    )
    if (
        data["decision"] != "ADR0228"
        or data["replicas"] != 4
        or data["api_sources"] != parent["expected_runtime_source_sha256"]
        or data["backend_images"] != parent["artifact_receipt"]["images"]
        or data["backend_images"]["api"] != dependency.INDEX
        or data["pooler_server_connections"] != 24
        or data["resources"] != {k: {"cpu": "1", "memory": "1Gi"} for k in ("requests", "limits")}
        or data["workload"]
        != {
            "offered_journeys_per_second": 84,
            "duration_seconds": 300,
            "concurrency": 500,
            "http_clients_per_shard": 8,
            "shards": 2,
            "poll_seconds": 1,
            "customer_retries": 0,
        }
        or data["api_settings"] != {**API_SETTINGS, **parent["arm_settings"]["candidate"]["api"]}
    ):
        raise ValueError("Exact retained paid baseline contract required")
    return data


def contract():
    import cce_transaction_profile as transaction
    data = legacy_contract()
    goal = transaction.active()
    if goal is not None:
        data = copy.deepcopy(data)
        data["api_sources"] = transaction.image_for(goal)["runtime_sources_sha256"]
        data["api_settings"]["DB_FAILURE_DIAGNOSTICS"] = "1"
        if goal["extension_decision"] == "ADR0245":
            data["api_settings"]["API_PAYMENT_POOL_MAX"] = str(transaction.payment_connections(goal))
    return data


def api_image():
    import cce_transaction_profile as transaction
    goal = transaction.active()
    return (transaction.image_for(goal)["registry_image"]
            if goal is not None else dependency.IMAGE)


def api_manifest():
    return api_image().split("@", 1)[1]


def authorized_creation(envelope, now=None):
    """Require explicit spending and remaining goal scope, or a legacy cleanup window."""
    now = now or datetime.now(UTC)
    dependency.authorized_today(envelope, now)
    exception = envelope["spending"]["temporary_cce_pilot_exception"]
    if exception.get("boundary_mode") == "goal_bounded":
        goal = exception["goal_bounded_authorization"]
        entries = policy.journal(envelope)["experiments"]
        start = goal.get("ledger_start_index")
        if type(start) is not int or not 0 <= start <= len(entries):
            raise ValueError("Exact CCE goal accounting boundary required")
        state = policy.read(policy.STATE)
        consumed = 0
        for entry in entries[start:]:
            if entry.get("profile") != goal["profile"]:
                continue
            counts = (
                entry.get("paid_runs_started", 0),
                state.get(entry["ledger"], {}).get("paid_runs_started", 0),
            )
            if any(type(count) is not int or count < 0 for count in counts):
                raise ValueError("Unknown CCE paid-stage consumption")
            consumed += max(counts)
        if consumed >= goal["maximum_paid_stages"]:
            raise ValueError("CCE goal paid-stage allowance already consumed")
        return
    expiry = datetime.fromisoformat(
        envelope["spending"]["temporary_cce_pilot_exception"]["expires_at_bangkok"]
    )
    if now + timedelta(seconds=3600) >= expiry:
        raise ValueError("Full CCE experiment and cleanup window must precede spending expiry")


def admission_budget():
    """One declared bounded factor; old profiles retain the immutable baseline."""
    exception = policy.envelope()["spending"]["temporary_cce_pilot_exception"]
    goal = exception.get("goal_bounded_authorization", {})
    if goal.get("extension_decision") in {"ADR0242", "ADR0245", "ADR0249"}:
        import cce_transaction_profile as transaction
        transaction.active()
        return 20
    if goal.get("profile") == "cce_hourly_qualification":
        if goal.get("decision") != "ADR0232" or goal.get("acquisition_budget") != 20:
            raise ValueError("Exact passing hourly acquisition configuration required")
        return 20
    if goal.get("extension_decision") != "ADR0234":
        return 12
    if goal.get("candidate_factor") != {
        "name": "api_shared_acquisition_budget",
        "baseline": 12,
        "candidate": 20,
        "database_connections_unchanged": True,
    }:
        raise ValueError("Exact ADR0234 acquisition factor required")
    return 20


def private_ip(value):
    addr = ipaddress.IPv4Address(value)
    if not any(
        addr in ipaddress.IPv4Network(net) for net in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
    ):
        raise ValueError("Routable RFC1918 endpoint required")
    return str(addr)


def api_environment(service, primary_ip, *, acquisition_budget=12):
    """Preserve authority and query options; change only the pooler route."""
    data = contract()
    if service.get("image") != dependency.INDEX or service.get("volumes"):
        raise ValueError("Exact frozen API image and supported mount-free service required")
    env = copy.deepcopy(service["environment"])
    if not isinstance(env, dict) or any(
        not isinstance(k, str) or not isinstance(v, str) for k, v in env.items()
    ):
        raise ValueError("Resolved string API environment required")
    env.update(data["api_settings"])
    if type(acquisition_budget) is not int or acquisition_budget not in {12, 20}:
        raise ValueError("Only baseline12 or bounded candidate20 acquisition slots supported")
    env["DB_POOL_MAX_WAITING"] = str(acquisition_budget)
    db = urlsplit(env["DATABASE_URL"])
    if (
        db.scheme not in {"postgres", "postgresql"}
        or db.hostname != "pgbouncer"
        or db.port != 5432
        or "@" not in db.netloc
    ):
        raise ValueError("Baseline must use the original pooler, never a direct RDS bypass")
    authority = db.netloc.rsplit("@", 1)[0] + "@" + private_ip(primary_ip) + ":6432"
    env["DATABASE_URL"] = urlunsplit(db._replace(netloc=authority))
    redis = urlsplit(env["REDIS_URL"])
    if redis.scheme not in {"redis", "rediss"} or redis.hostname != "10.1.245.61" or redis.port != 6379:
        raise ValueError("Exact live DCS primary endpoint required")
    if any(not env.get(key) for key in ("JWT_SECRET", "WEBHOOK_SECRET")):
        raise ValueError("Unchanged customer authorization secrets required")
    if any(key in env for key in ("CCE_RUN_ID", "CCE_POD_UID")):
        raise ValueError("Reserved pod identity environment collision")
    return env


def startup_program(sources, environment_sha256, command):
    if (
        sources != contract()["api_sources"]
        or not re.fullmatch(r"[0-9a-f]{64}", environment_sha256)
        or command
        != [
            "uvicorn",
            "ticketing.api:app",
            "--host",
            "0.0.0.0",
            "--port",
            "8000",
            "--limit-concurrency",
            "256",
            "--timeout-keep-alive",
            "10",
        ]
    ):
        raise ValueError("Pinned source map and unchanged baseline API command required")
    return (
        "expected="
        + repr(sources)
        + "\nenv_sha="
        + repr(environment_sha256)
        + "\ncommand="
        + repr(command)
        + "\n"
        + STARTUP
    )


STARTUP = r"""
import hashlib,importlib,json,os,types
from pathlib import Path
def code_identity(code):
 return code.replace(co_filename='<frozen>',co_consts=tuple(code_identity(c) if isinstance(c,types.CodeType) else c for c in code.co_consts))
actual_env={k:v for k,v in os.environ.items() if k not in {'CCE_RUN_ID','CCE_POD_UID','HOSTNAME'}}
# Kubernetes may add service variables; hash only explicitly declared baseline keys.
actual_env={k:actual_env.get(k) for k in environment_keys}
if hashlib.sha256(json.dumps(actual_env,sort_keys=True,separators=(',',':')).encode()).hexdigest()!=env_sha:raise ValueError('API environment drift')
for path,want in expected.items():
 raw=(Path('/app')/path).read_bytes().replace(b'\r\n',b'\n')
 if hashlib.sha256(raw).hexdigest()!=want:raise ValueError('Copied API source drift')
 parts=path.removeprefix('src/').removesuffix('.py').split('/')
 module=importlib.import_module('.'.join(parts[:-1] if parts[-1]=='__init__' else parts))
 installed=Path(module.__file__).resolve()
 if not str(installed).startswith('/usr/local/lib/python3.12/site-packages/ticketing/'):raise ValueError('Unexpected installed import path')
 raw=installed.read_bytes().replace(b'\r\n',b'\n')
 if hashlib.sha256(raw).hexdigest()!=want:raise ValueError('Installed API source drift')
 loaded=module.__loader__.get_code(module.__name__)
 if code_identity(loaded)!=code_identity(compile(raw,str(installed),'exec')):raise ValueError('Loaded API bytecode drift')
proof={'run':os.environ['CCE_RUN_ID'],'pod_uid':os.environ['CCE_POD_UID'],
 'sources':expected,'environment_sha256':env_sha,
 'command_sha256':hashlib.sha256(json.dumps(command,sort_keys=True,separators=(',',':')).encode()).hexdigest()}
print('CCE_API_STARTUP '+json.dumps(proof,sort_keys=True),flush=True)
os.execvp(command[0],command)
"""


def objects(run, service, primary_ip, registry_username, registry_password, *, acquisition_budget=12, profile=None):
    from cce_paid_profiles import HOURLY, SHORT
    profile = SHORT if profile is None else profile
    if profile not in (SHORT, HOURLY):
        raise ValueError("Exact native lifetime profile required")
    """Private payloads: never publish returned Secrets."""
    import cce_transaction_profile as transaction
    pull_secrets = ([{"name": "default-secret"}, {"name": "swr-pull"}]
                    if transaction.active() is not None else [{"name": "swr-pull"}])
    namespace = dependency.namespace_for(run)
    env = api_environment(service, primary_ip, acquisition_budget=acquisition_budget)
    program = (
        "environment_keys="
        + repr(sorted(env))
        + "\n"
        + startup_program(contract()["api_sources"], policy.digest(env), service["command"])
    )
    labels = {"codex-owner": run}
    auth = base64.b64encode((registry_username + ":" + registry_password).encode()).decode()
    pull = {"auths": {dependency.REGISTRY: {"auth": auth}}}
    result = [
        {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace, "labels": labels}},
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "api-env", "labels": labels},
            "type": "Opaque",
            "data": {k: base64.b64encode(v.encode()).decode() for k, v in env.items()},
        },
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "swr-pull", "labels": labels},
            "type": "kubernetes.io/dockerconfigjson",
            "data": {".dockerconfigjson": base64.b64encode(json.dumps(pull).encode()).decode()},
        },
    ]
    for index in range(4):
        result.append(
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": f"api-{index}",
                    "labels": labels,
                    "annotations": {
                        "codex-environment-sha256": policy.digest(env),
                        "codex-command-sha256": policy.digest(service["command"]),
                    },
                },
                "spec": {
                    "restartPolicy": "Never",
                    "activeDeadlineSeconds": 5400 if profile == HOURLY else 3000,
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "imagePullSecrets": copy.deepcopy(pull_secrets),
                    "securityContext": {
                        "runAsUser": 10001,
                        "runAsNonRoot": True,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "api",
                            "image": api_image(),
                            "imagePullPolicy": "Always",
                            "command": ["python", "-u", "-c", program],
                            "workingDir": "/app",
                            "envFrom": [{"secretRef": {"name": "api-env"}}],
                            "env": [
                                {"name": "CCE_RUN_ID", "value": run},
                                {
                                    "name": "CCE_POD_UID",
                                    "valueFrom": {
                                        "fieldRef": {"apiVersion": "v1", "fieldPath": "metadata.uid"}
                                    },
                                },
                            ],
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "resources": copy.deepcopy(contract()["resources"]),
                            "readinessProbe": {
                                "httpGet": {"path": "/health/ready", "port": 8000, "scheme": "HTTP"},
                                "periodSeconds": 10,
                                "timeoutSeconds": 5,
                                "failureThreshold": 1,
                                "successThreshold": 1,
                            },
                            "ports": [{"containerPort": 8000, "protocol": "TCP"}],
                        }
                    ],
                },
            }
        )
    return result


def parse_startup(log, expected_env_sha, expected_command):
    if not isinstance(log, str) or len(log.encode()) > 65536:
        raise ValueError("Bounded startup log required")
    proofs = [
        json.loads(line[len(PROOF_PREFIX) :]) for line in log.splitlines() if line.startswith(PROOF_PREFIX)
    ]
    if (
        len(proofs) != 1
        or proofs[0].get("environment_sha256") != expected_env_sha
        or proofs[0].get("command_sha256") != policy.digest(expected_command)
    ):
        raise ValueError("Exactly one unchanged startup environment/command proof required")
    return proofs[0]


def receipt(pod, expected, uid, run, startup_proof):
    meta, spec, status = pod.get("metadata", {}), pod.get("spec", {}), pod.get("status", {})
    expected_spec = expected["spec"]
    statuses, conditions = status.get("containerStatuses", []), status.get("conditions", [])
    container = spec.get("containers", [{}])[0]
    if (
        meta.get("uid") != uid
        or not uid
        or meta.get("name") != expected["metadata"]["name"]
        or meta.get("labels", {}).get("codex-owner") != run
        or meta.get("deletionTimestamp")
        or any(
            meta.get("annotations", {}).get(k) != v for k, v in expected["metadata"]["annotations"].items()
        )
        or len(spec.get("containers", [])) != 1
        or len(statuses) != 1
        or status.get("phase") != "Running"
        or not any(c.get("type") == "Ready" and c.get("status") == "True" for c in conditions)
        or any(container.get(k) != v for k, v in expected_spec["containers"][0].items())
        or any(
            spec.get(k) != expected_spec[k]
            for k in (
                "restartPolicy",
                "activeDeadlineSeconds",
                "automountServiceAccountToken",
                "enableServiceLinks",
                "imagePullSecrets",
                "securityContext",
            )
        )
        or statuses[0].get("name") != "api"
        or type(statuses[0].get("restartCount")) is not int
        or statuses[0]["restartCount"] != 0
        or statuses[0].get("ready") is not True
        or not statuses[0].get("state", {}).get("running", {}).get("startedAt")
        or statuses[0].get("imageID", "").split("@")[-1] != api_manifest()
    ):
        raise ValueError("CCE API identity, readiness or admitted specification drift")
    if (
        startup_proof.get("run") != run
        or startup_proof.get("pod_uid") != uid
        or startup_proof.get("sources") != contract()["api_sources"]
        or startup_proof.get("environment_sha256")
        != expected["metadata"]["annotations"]["codex-environment-sha256"]
        or startup_proof.get("command_sha256") != expected["metadata"]["annotations"]["codex-command-sha256"]
    ):
        raise ValueError("Bound startup source proof required")
    return {
        "pod_uid": uid,
        "pod_name": meta["name"],
        "private_ipv4": private_ip(status["podIP"]),
        "image_id": statuses[0]["imageID"],
        "started_at": statuses[0]["state"]["running"]["startedAt"],
        "resources": container["resources"],
        "startup_proof": startup_proof,
    }


def verification_summary(pod, expected):
    """Bounded nonsecret failed-admission context; never emit environment or logs."""
    spec = pod.get("spec", {})
    status = pod.get("status", {})
    container = spec.get("containers", [{}])[0]
    wanted = expected["spec"]["containers"][0]
    return {
        "pod_name": expected["metadata"]["name"],
        "phase": status.get("phase"),
        "container_count": len(spec.get("containers", [])),
        "different_container_fields": sorted(k for k, v in wanted.items() if container.get(k) != v),
        "different_pod_fields": sorted(
            k
            for k in (
                "restartPolicy",
                "activeDeadlineSeconds",
                "automountServiceAccountToken",
                "enableServiceLinks",
                "imagePullSecrets",
                "securityContext",
            )
            if spec.get(k) != expected["spec"][k]
        ),
        "resources": container.get("resources"),
        "readiness_conditions": [{k: str(row.get(k, ""))[:64] for k in ("type", "status", "reason")}
                                 for row in status.get("conditions", [])[:8]],
        "container_states": [
            {
                "name": row.get("name"),
                "ready": row.get("ready"),
                "restart_count": row.get("restartCount"),
                "image_id": row.get("imageID"),
                "state_kinds": sorted(row.get("state", {})),
                "waiting_reason": str(row.get("state", {}).get("waiting", {}).get("reason", ""))[:64],
                "exit_code": row.get("state", {}).get("terminated", {}).get("exitCode"),
            }
            for row in status.get("containerStatuses", [])[:4]
        ],
    }


def endpoints(receipts):
    if len(receipts) != 4 or {r["pod_name"] for r in receipts} != {f"api-{i}" for i in range(4)}:
        raise ValueError("Every CCE API pod required")
    for key in ("pod_uid", "private_ipv4"):
        if len({r[key] for r in receipts}) != 4:
            raise ValueError("Distinct native pod identities and endpoints required")
    return sorted(private_ip(r["private_ipv4"]) for r in receipts)


def install_observer(module, receipts, process_starts, extra_metrics):
    """Retain native worker discovery; only API discovery/identity changes."""
    addresses = endpoints(receipts)
    if set(process_starts) != set(addresses) or any(
        type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in process_starts.values()
    ):
        raise ValueError("All four process-start identities required")
    discover, metrics = module.api_replicas, module.api_metrics

    def replicas(host="api", port=8000):
        return addresses if host == "api" and port == 8000 else discover(host, port)

    def observed(address):
        result = metrics(address)
        identity = extra_metrics(address)
        if (
            address not in process_starts
            or identity.get("process_start_time_seconds") != process_starts[address]
        ):
            raise ValueError("CCE API process identity changed during observation")
        return {**result, **identity}

    module.api_replicas, module.api_metrics = replicas, observed


class Deployment:
    """UID-guarded lifecycle. It cannot run under a dependency-only scope."""

    def __init__(self, request, guard, persist, *, now=time.monotonic, sleep=time.sleep):
        self.request, self.guard, self.persist, self.now, self.sleep = request, guard, persist, now, sleep
        self.namespace = self.run = self.namespace_uid = None
        self.pod_uids, self.attempted = {}, False

    def authorize(self):
        if self.guard is None:
            raise ValueError("Separate registered CCE paid profile required before application deployment")
        if for_guard(self.guard).name not in policy.PROFILES:
            raise ValueError("CCE paid profile has not been registered and qualified")
        if for_guard(self.guard).name not in policy.envelope()["qualified_profiles"]:
            raise ValueError("CCE paid profile has not been registered and qualified")
        dependency.authorized_today(policy.envelope())
        self.guard.check(15)

    def mutation(self, method, path, body):
        self.authorize()
        return self.request(method, path, body)

    def create(self, manifests):
        self.authorize()
        authorized_creation(policy.envelope())
        if len(manifests) != 7 or [v["kind"] for v in manifests] != [
            "Namespace",
            "Secret",
            "Secret",
            "Pod",
            "Pod",
            "Pod",
            "Pod",
        ]:
            raise ValueError("Exact four-pod deployment required")
        if self.guard.binding.get("cce_manifest_sha256") != policy.digest(manifests):
            raise ValueError("Exact private deployment binding required")
        budget = self.guard.binding.get("cce_acquisition_budget", 12)
        if budget != admission_budget() or type(budget) is not int:
            raise ValueError("Current declared acquisition budget differs from binding")
        secret_budget = base64.b64decode(manifests[1]["data"]["DB_POOL_MAX_WAITING"], validate=True).decode()
        if secret_budget != str(budget):
            raise ValueError("Candidate acquisition Secret differs from bound budget")
        self.namespace = manifests[0]["metadata"]["name"]
        self.run = manifests[0]["metadata"]["labels"]["codex-owner"]
        if self.namespace != dependency.namespace_for(self.run) or self.attempted:
            raise ValueError("Fresh canonical namespace required")
        # Request returns None only for an authenticated Kubernetes 404.
        if self.request("GET", self.path(), None) is not None:
            raise ValueError("Namespace exists; never adopt or replay it")
        self.attempted = True
        self.persist({"namespace": self.namespace, "run": self.run, "namespace_creation_attempted": True})
        ns = self.mutation("POST", "/api/v1/namespaces", manifests[0])
        self.namespace_uid = ns["metadata"]["uid"]
        self.persist({"namespace_uid": self.namespace_uid})
        for item in manifests[1:]:
            resource = "pods" if item["kind"] == "Pod" else "secrets"
            value = self.mutation("POST", self.path() + "/" + resource, item)
            if resource == "pods":
                self.pod_uids[item["metadata"]["name"]] = value["metadata"]["uid"]
                self.persist({"pod_uids": dict(self.pod_uids)})

    def path(self):
        return "/api/v1/namespaces/" + self.namespace

    def verify_owner(self):
        ns = self.request("GET", self.path(), None)
        if (
            ns is None
            or not self.namespace_uid
            or ns["metadata"].get("uid") != self.namespace_uid
            or ns["metadata"].get("labels", {}).get("codex-owner") != self.run
        ):
            raise ValueError("Ambiguous CCE namespace identity")

    def observe(self, manifests, http_ready, http_metrics, *, previous=None):
        self.verify_owner()
        self.authorize()
        if set(self.pod_uids) != {f"api-{i}" for i in range(4)}:
            raise ValueError("Four captured pod creation identities required")
        views = []
        for expected in manifests[3:]:
            name = expected["metadata"]["name"]
            path = self.path() + "/pods/" + name
            pod = self.request("GET", path, None)
            if pod is None:
                raise ValueError("Owned API pod missing")
            log = self.request("GET", path + "/log?container=api&limitBytes=65536", None)
            annotations = expected["metadata"]["annotations"]
            context = verification_summary(pod, expected)
            context["verification_gate"] = "startup_proof"
            try:
                proof = parse_startup(
                    log,
                    annotations["codex-environment-sha256"],
                    [
                        "uvicorn",
                        "ticketing.api:app",
                        "--host",
                        "0.0.0.0",
                        "--port",
                        "8000",
                        "--limit-concurrency",
                        "256",
                        "--timeout-keep-alive",
                        "10",
                    ],
                )
                context["startup_sources_match"] = proof.get("sources") == contract()["api_sources"]
                context["startup_identity_matches"] = (
                    proof.get("run") == self.run and proof.get("pod_uid") == self.pod_uids[name]
                )
                context["verification_gate"] = "admitted_pod_receipt"
                view = receipt(pod, expected, self.pod_uids[name], self.run, proof)
            except (ValueError, KeyError, TypeError):
                self.persist({"failed_pod_verification": context})
                raise
            address = view["private_ipv4"]
            context["verification_gate"] = "application_readiness"
            if http_ready(address) != {"status": "ready"}:
                self.persist({"failed_pod_verification": context})
                raise ValueError("Authenticated database/Redis application readiness failed")
            metrics = http_metrics(address)
            started = metrics.get("process_start_time_seconds")
            cpu = metrics.get("process_cpu_seconds_total")
            context["verification_gate"] = "process_metrics"
            context["process_start_present"] = (
                type(started) in (int, float) and math.isfinite(started) and started > 0
            )
            context["process_cpu_present"] = type(cpu) in (int, float) and math.isfinite(cpu) and cpu >= 0
            if not context["process_start_present"]:
                self.persist({"failed_pod_verification": context})
                raise ValueError("API process-start identity required")
            if not context["process_cpu_present"]:
                self.persist({"failed_pod_verification": context})
                raise ValueError("API CPU counter required")
            view["process_start_time_seconds"] = started
            views.append(view)
        endpoints(views)
        if previous is not None and views != previous:
            raise ValueError("CCE API runtime changed since admission")
        self.persist({"cce_api_receipts": views})
        return views

    def admission_diagnostics(self):
        """Read only exact owned pod logs; available during mandatory cleanup."""
        self.verify_owner()
        if set(self.pod_uids) != {f"api-{i}" for i in range(4)}:
            raise ValueError("Four captured pod identities required for admission evidence")
        records = {}
        errors = {}
        for name, uid in sorted(self.pod_uids.items()):
            path = self.path() + "/pods/" + name
            try:

                def verify(path=path, uid=uid):
                    self.verify_owner()
                    pod = self.request("GET", path, None)
                    if (
                        pod is None
                        or pod.get("metadata", {}).get("uid") != uid
                        or pod["metadata"].get("labels", {}).get("codex-owner") != self.run
                    ):
                        raise ValueError("Admission evidence pod identity changed")

                verify()
                value = self.request(
                    "GET", path + "/log?container=api&sinceSeconds=600&limitBytes=8388608", None
                )
                verify()
                if not isinstance(value, dict) or value.get("schema_version") != 1:
                    raise ValueError("Bounded filtered admission evidence required")
                records[name] = {"pod_uid": uid, **value}
            except Exception as error:  # noqa: BLE001 - Capture failure must not prevent owned cleanup.
                errors[name] = type(error).__name__
        return {
            "schema_version": 1,
            "run": self.run,
            "namespace_uid": self.namespace_uid,
            "pods": records,
            "errors": errors,
            "all_pods_captured": not errors and len(records) == 4,
        }

    def cleanup(self):
        if not self.attempted:
            return {"namespace_removed": True}
        if self.namespace_uid and self.request("GET", self.path(), None) is None:
            return {"namespace_removed": True}
        self.verify_owner()
        # Owned cleanup deliberately remains available after pause/spending expiry.
        self.request(
            "DELETE",
            self.path(),
            {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": self.namespace_uid}},
        )
        deadline = self.now() + 90
        while self.now() < deadline:
            if self.request("GET", self.path(), None) is None:
                return {"namespace_removed": True}
            self.verify_owner()
            self.sleep(2)
        raise TimeoutError("CCE namespace cleanup deadline; recovery remains blocked")


class KubernetesTransport:
    """Verified TLS through the existing SSH session; credentials never reach logs."""

    def __init__(self, session, kubeconfig):
        self.session = session
        self.material = dependency.kube_material(kubeconfig)

    def close(self):
        self.material.clear()

    def request(self, method, path, body=None):
        if not self.material:
            raise ValueError("Closed CCE certificate transport")
        if method not in {"GET", "POST", "DELETE"} or not re.fullmatch(
            r"/api/v1/namespaces(?:/flash-cce-[0-9a-f]{12}(?:/(?:pods|secrets)(?:/api-[0-3](?:/log\?container=api&(?:limitBytes=65536|sinceSeconds=600&limitBytes=8388608))?)?)?)?",
            path,
        ):
            raise ValueError("Bounded owned CCE resource path required")
        if method == "POST" and not (path == "/api/v1/namespaces" or path.endswith(("/pods", "/secrets"))):
            raise ValueError("Creation requires an owned collection path")
        if method == "DELETE" and not re.fullmatch(r"/api/v1/namespaces/flash-cce-[0-9a-f]{12}", path):
            raise ValueError("Cleanup deletes only the exact owned namespace")
        if method == "DELETE" and (
            not isinstance(body, dict) or not body.get("preconditions", {}).get("uid")
        ):
            raise ValueError("Immutable namespace deletion precondition required")
        if getattr(self.session, "cleanup_mode", False) and method == "POST":
            raise ValueError("Cleanup cannot create new workloads")
        if method == "GET" and "/secrets" in path:
            raise ValueError("Secret retrieval is not an observation capability")
        if method != "GET" and body is None:
            raise ValueError("Explicit owned mutation body required")
        if method != "GET" and not getattr(self.session, "cleanup_mode", False):
            guard = getattr(self.session, "action_guard", None)
            if (
                guard is None
                or for_guard(guard).name not in policy.PROFILES
                or for_guard(guard).name not in policy.PROFILES
                or for_guard(guard).name not in policy.envelope()["qualified_profiles"]
            ):
                raise ValueError("Paid CCE mutation requires the qualified registered profile")
            dependency.authorized_today(policy.envelope())
            guard.check(15)
        payload = {
            "material": self.material,
            "server": dependency.SERVER,
            "method": method,
            "path": path,
            "body": body,
        }
        result = self.session.call("primary", "payload=" + repr(payload) + "\n" + REQUEST, timeout=20)
        if result.get("not_found") is True:
            return None
        return result["value"]


DIAGNOSTIC_FILTER = r"""
import hashlib,json,math
def filtered_admission_logs(raw):
 records=[];events=0;malformed=0
 for line in raw.decode('utf-8',errors='replace').splitlines():
  try:row=json.loads(line)
  except (ValueError,TypeError):
   if 'db_acquisition_failure' in line:malformed+=1
   continue
  if not isinstance(row,dict):continue
  fields=row.get('fields',row)
  if not isinstance(fields,dict) or fields.get('event')!='db_acquisition_failure':continue
  events+=1
  if len(records)>=128:continue
  record={}
  for key,allowed in {'role':{'general','payment','callback'},'reason':{'global_limit','role_limit','native_timeout','native_limit'},'capture':{'guard_rejection','native_failure_before_release'}}.items():
   if fields.get(key) in allowed:record[key]=fields[key]
  for key in ('elapsed_ms',):
   value=fields.get(key)
   if type(value) in (int,float) and math.isfinite(value) and value>=0:record[key]=value
  native=fields.get('native_pool')
  if isinstance(native,dict):record['native_pool']={k:v for k,v in native.items() if k in {'pool_size','pool_available','requests_waiting','pool_max'} and type(v) is int and v>=0}
  guard=fields.get('guard')
  if isinstance(guard,dict):
   result={k:v for k,v in guard.items() if k in {'maximum','used','acquiring','retained','callback_reserved'} and type(v) is int and v>=0}
   if type(guard.get('partial_timeout_reclaim')) is bool:result['partial_timeout_reclaim']=guard['partial_timeout_reclaim']
   for key in ('counts','retained_by_role','limits'):
    values=guard.get(key)
    if isinstance(values,dict):result[key]={k:v for k,v in values.items() if k in {'general','payment','callback'} and type(v) is int and v>=0}
   record['guard']=result
  if not all(k in record for k in ('role','reason','capture','guard','native_pool')):malformed+=1
  records.append(record)
 return {'schema_version':1,'logs_sha256':hashlib.sha256(raw).hexdigest(),'logs_bytes':len(raw),'byte_limit_reached':len(raw)>=8388608,'failure_events':events,'overflow_events':max(0,events-len(records)),'malformed_events':malformed,'records':records}
"""

REQUEST = (
    DIAGNOSTIC_FILTER
    + r"""
import json,os,ssl,tempfile,urllib.request,urllib.error
from pathlib import Path
with tempfile.TemporaryDirectory(prefix='codex-cce-') as directory:
 os.chmod(directory,0o700)
 for name,content in payload.pop('material').items():
  path=Path(directory)/name
  with path.open('x') as stream:os.chmod(path,0o600);stream.write(content)
 context=ssl.create_default_context(cafile=directory+'/ca.crt')
 context.load_cert_chain(directory+'/client.crt',directory+'/client.key')
 body=payload['body']
 req=urllib.request.Request(payload['server']+payload['path'],
  data=json.dumps(body).encode() if body is not None else None,
  headers={'Content-Type':'application/json'},method=payload['method'])
 try:
  with urllib.request.urlopen(req,context=context,timeout=10) as response:
   diagnostic=payload['path'].endswith('/log?container=api&sinceSeconds=600&limitBytes=8388608')
   limit=8388608 if diagnostic else 1048576
   raw=response.read(limit+1)
   if len(raw)>limit:raise ValueError('CCE response exceeds bounded payload')
   value=filtered_admission_logs(raw) if diagnostic else (raw.decode() if '/log?' in payload['path'] else json.loads(raw))
   result={'not_found':False,'value':value}
 except urllib.error.HTTPError as error:
  if error.code==404 and payload['method']=='GET':result={'not_found':True}
  else:raise RuntimeError('CCE request failed: HTTP '+str(error.code)) from None
print(json.dumps(result))
"""
)
