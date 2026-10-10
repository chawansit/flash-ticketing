"""Run inside the shared image: verify the actual imported application, not host sources."""
import hashlib
import importlib
import importlib.metadata
import json
import os
import subprocess
import sys
from pathlib import Path


def sha(raw):
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def check_deployments(path):
    deployments = [item for item in json.loads(path.read_text())["items"] if item["kind"] == "Deployment"]
    configured = []
    for deployment in deployments:
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        env = {**os.environ, "ENVIRONMENT": "production", "JWT_SECRET": "validation-jwt-no-network",
               "WEBHOOK_SECRET": "validation-webhook-no-network", "RESERVATION_MODE": "redis-first",
               "REDIS_RESERVATION_REPLICA_ACKS": "1"}
        env.update({entry["name"]: entry["value"] for entry in container["env"] if "value" in entry})
        subprocess.run([sys.executable, "-c", "from ticketing.config import Settings; Settings().validate()"],
                       env=env, check=True, capture_output=True)
        configured.append(container["name"])
    return configured


def main():
    manifest = json.loads(Path("/app/shared-image.json").read_text())
    expected = manifest["runtime_source_sha256"]
    dist = importlib.metadata.distribution("flash-ticketing")
    roots = [Path("/app/src/ticketing"), Path(dist.locate_file("ticketing"))]
    for root in roots:
        actual = {"src/ticketing/" + p.relative_to(root).as_posix(): sha(p.read_bytes())
                  for p in root.rglob("*.py")}
        if actual != expected:
            raise ValueError("Installed or copied sources differ from receipt")
    for name, digest in expected.items():
        relative = name.removeprefix("src/").removesuffix(".py").replace("/", ".")
        module = importlib.import_module(relative.removesuffix(".__init__"))
        if sha(Path(module.__file__).read_bytes()) != digest:
            raise ValueError("Imported runtime differs from receipt")
    for name, digest in manifest["dependency_input_sha256"].items():
        if sha((Path("/app") / name).read_bytes()) != digest:
            raise ValueError("Dependency input differs")
    from dataclasses import replace

    from ticketing.api import app
    from ticketing.config import Settings
    for role, pool, concurrency in [("api", 4, 1), ("consumer", 8, 1), ("reservation-writer", 12, 1),
                                    ("publisher", 12, 1), ("maintenance", 12, 1), ("reconciler", 12, 1),
                                    ("simulator", 10, 12), ("confirmation", 2, 1)]:
        settings = replace(Settings(), pool_max=pool, simulator_concurrency=concurrency,
                           payment_confirmation_async=role == "confirmation")
        settings.validate()
    if Settings().reservation_write_pipeline:
        raise ValueError("Writer pipeline must remain default off")
    from ticketing.infrastructure.reservations import PostgresReservations
    if not hasattr(PostgresReservations(None, None, persist_write_pipeline=True), "persist_write_pipeline"):
        raise ValueError("Accepted writer pipeline capability missing")
    for concurrency in (0, 33, True):
        try:
            replace(Settings(), simulator_concurrency=concurrency).validate()
        except RuntimeError:
            pass
        else:
            raise ValueError("Invalid simulator concurrency accepted")
    if not any("payment-operation" in path for path in app.openapi()["paths"]):
        raise ValueError("Customer recovery endpoint missing")
    deployed_roles = check_deployments(Path(sys.argv[1])) if len(sys.argv) == 2 else []
    print(json.dumps({"modules_verified": len(expected), "manifest_role_settings_validated": deployed_roles, "installed_and_copied_sources_match": True,
                      "all_modules_imported": True, "role_settings_validated": 8, "writer_pipeline_capability_verified": True,
                      "invalid_simulator_settings_rejected": True,
                      "payment_recovery_endpoint_present": True,
                      "order_status_read_pipeline_default_off": not Settings().order_status_read_pipeline}))


if __name__ == "__main__":
    main()
