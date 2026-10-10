"""ADR0258 offline CCE deployment rendering; all services start at zero replicas."""
import argparse
import hashlib
import json
import re
from pathlib import Path

REGISTRY = "swr.ap-southeast-2.myhuaweicloud.com/chawansit/flash-ticketing"
# (suggested test replicas, client SQL pool per process); physical PgBouncer budget stays 24.
ROLES = {"api": (4, 4), "consumer": (6, 8), "reservation-writer": (3, 12),
         "publisher": (1, 12), "maintenance": (1, 12), "reconciler": (1, 12),
         "simulator": (1, 10), "confirmation": (0, 2)}
API_COMMAND = ["uvicorn", "ticketing.api:app", "--host", "0.0.0.0", "--port", "8000",
               "--limit-concurrency", "256", "--timeout-keep-alive", "5"]


def render(receipt, *, namespace="ticketing-shared-preview"):
    image = receipt.get("registry_image", "")
    if receipt.get("decision") not in {"ADR0258", "ADR0261"} or receipt.get("registry_published") is not True:
        raise ValueError("Published shared image receipt required")
    if not re.fullmatch(re.escape(REGISTRY) + r"@sha256:[0-9a-f]{64}", image):
        raise ValueError("Immutable application registry digest required")
    if receipt.get("registry_manifest_digest") != image.rsplit("@", 1)[1]:
        raise ValueError("Registry digest and receipt differ")
    sources = receipt.get("runtime_source_sha256", {})
    if len(sources) != 22 or any(not re.fullmatch(r"src/ticketing/[a-z_/]+\.py", name)
                               or not re.fullmatch(r"[0-9a-f]{64}", value)
                               for name, value in sources.items()):
        raise ValueError("Complete 22-module source map required")
    identity = receipt.get("source_identity_sha256", "")
    if hashlib.sha256(json.dumps(sources, sort_keys=True).encode()).hexdigest() != identity:
        raise ValueError("Source identity and receipt differ")
    if not re.fullmatch(r"[0-9a-f]{64}", identity):
        raise ValueError("Complete source identity required")
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,61}[a-z0-9]", namespace):
        raise ValueError("Valid namespace required")
    items = []
    for role, (suggested, pool) in ROLES.items():
        labels = {"app.kubernetes.io/name": "ticketing", "app.kubernetes.io/component": role,
                  "app.kubernetes.io/part-of": "ticketing-shared"}
        values = {"DB_POOL_MAX": str(pool),
                  "SIMULATOR_CONCURRENCY": "12" if role == "simulator" else "1",
                  "PAYMENT_CONFIRMATION_ASYNC": "1" if role == "confirmation" else "0",
                  "ORDER_STATUS_READ_PIPELINE": "0", "RESERVATION_WRITE_PIPELINE": "1" if role == "reservation-writer" else "0",
                  "ORDER_STATUS_EVENT_REFRESH": "1" if role in ("consumer", "api") else "0",
                  "ORDER_STATUS_EVENT_REFRESH_DEDUP": "0", "WORKER_METRICS_PORT": "9101",
                  "ORDER_STATUS_CACHE_MS": "1000" if role in ("consumer", "api") else "0",
                  "ORDER_STATUS_POLL_MS": "500"}
        if role == "api":
            values.update(API_PAYMENT_POOL_MAX="2", API_POOL_SHARED_WAITING="1",
                          DB_POOL_MAX_WAITING="20", API_PARTIAL_TIMEOUT_RECLAIM="0",
                          API_CALLBACK_ACQUISITION_RESERVE="0")
        if role == "reservation-writer":
            values["RESERVATION_WRITER_BATCH_SIZE"] = "4"
        if role == "simulator":
            values.update(ENVIRONMENT="development", SIMULATOR_DISPATCH_MODE="refill", API_URL="http://ticketing-api:8000")
        env = [{"name": name, "value": value} for name, value in sorted(values.items())]
        for name, key in {"DATABASE_URL": "DATABASE_URL_POOLER", "REDIS_URL": "REDIS_URL",
                          "JWT_SECRET": "JWT_SECRET", "WEBHOOK_SECRET": "WEBHOOK_SECRET"}.items():
            env.append({"name": name, "valueFrom": {"secretKeyRef": {"name": "ticketing-runtime", "key": key}}})
        resources = {"cpu": "1", "memory": "2Gi"} if role == "api" else {"cpu": "250m", "memory": "512Mi"}
        port = 8000 if role == "api" else 9101
        container = {"name": role, "image": image, "imagePullPolicy": "IfNotPresent",
                     "command": API_COMMAND if role == "api" else ["python", "-m", "ticketing.workers", role],
                     "envFrom": [{"configMapRef": {"name": "ticketing-runtime"}}], "env": env,
                     "resources": {"requests": dict(resources), "limits": dict(resources)},
                     "ports": [{"name": "http" if role == "api" else "metrics", "containerPort": port}],
                     "securityContext": {"runAsNonRoot": True, "runAsUser": 10001,
                                         "allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                     "volumeMounts": [{"name": "rds-ca", "mountPath": "/etc/ticketing/rds-ca", "readOnly": True}]}
        if role == "api":
            container["readinessProbe"] = {"httpGet": {"path": "/health/ready", "port": "http"},
                                           "periodSeconds": 5, "timeoutSeconds": 3, "failureThreshold": 3}
        # Worker metrics is not a business-readiness probe. External progress checks are mandatory.
        pod = {"automountServiceAccountToken": False, "terminationGracePeriodSeconds": 60,
               "imagePullSecrets": [{"name": "swr-pull"}], "containers": [container],
               "volumes": [{"name": "rds-ca", "secret": {"secretName": "ticketing-rds-ca"}}]}
        items.append({"apiVersion": "apps/v1", "kind": "Deployment",
                      "metadata": {"name": "ticketing-" + role, "namespace": namespace, "labels": labels,
                                   "annotations": {"ticketing/source-sha256": identity,
                                                   "ticketing/suggested-test-replicas": str(suggested)}},
                      "spec": {"replicas": 0, "strategy": {"type": "Recreate"},
                               "selector": {"matchLabels": labels},
                               "template": {"metadata": {"labels": labels,
                                                         "annotations": {"ticketing/source-sha256": identity}},
                                            "spec": pod}}})
        items.append({"apiVersion": "v1", "kind": "Service",
                      "metadata": {"name": "ticketing-" + role, "namespace": namespace},
                      "spec": {"type": "ClusterIP", "selector": labels,
                               "ports": [{"name": "http" if role == "api" else "metrics",
                                          "port": port, "targetPort": port}]}})
    return {"apiVersion": "v1", "kind": "List", "items": items}


def resources():
    active = sum(count for count, _ in ROLES.values())
    clients = sum(count * pool for count, pool in ROLES.values())
    return {"suggested_test_pods": active, "requested_vcpu": 4 + (active - 4) * .25,
            "requested_memory_gib": 8 + (active - 4) * .5,
            "application_client_connections_max": clients, "pgbouncer_server_connections_max": 24,
            "initial_replicas": 0, "cloud_deployed": False,
            "configuration_and_network_preflight_required": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--namespace", default="ticketing-shared-preview")
    args = parser.parse_args()
    manifest = render(json.loads(args.receipt.read_text()), namespace=args.namespace)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes((json.dumps(manifest, indent=2) + "\n").encode())
    print(json.dumps(resources()))


if __name__ == "__main__":
    main()
