"""ADR0228 private bridge and audit helper, with exact owned Docker cleanup."""

import json
import re

import cce_api_adapter as cce
import cce_paid_stage as paid
import work_envelope as policy
from cce_ecs_transition import execution_identity
from cce_paid_profiles import for_guard
from qualify_two_host_deployment import INSPECT

BRIDGE_CPU_LIMIT = 1

PROXY = r"""
import asyncio
async def copy(src,dst):
 try:
  while True:
   block=await asyncio.wait_for(src.read(65536),300)
   if not block:break
   dst.write(block);await dst.drain()
 finally:dst.close()
async def accept(reader,writer):
 try:
  rr,rw=await asyncio.wait_for(asyncio.open_connection('pgbouncer',5432),5)
  await asyncio.gather(copy(reader,rw),copy(rr,writer))
 finally:writer.close()
async def main():
 async with await asyncio.start_server(accept,'0.0.0.0',6432) as server:await server.serve_forever()
asyncio.run(main())
"""


def owned_receipt(row, name, run, command, *, pool_network, bridge, require_running=True):
    cfg, host = row.get("Config", {}), row.get("HostConfig", {})
    labels = cfg.get("Labels", {})
    expected_ports = {"6432/tcp": [{"HostIp": "10.1.137.69", "HostPort": "6432"}]} if bridge else {}
    if (
        not re.fullmatch(r"[0-9a-f]{64}", row.get("Id", ""))
        or row.get("Name") != "/" + name
        or row.get("Image") != cce.dependency.INDEX
        or cfg.get("Image") != cce.dependency.INDEX
        or labels.get("com.docker.compose.project") != "cce-" + run
        or labels.get("com.docker.compose.service") != ("pooler-bridge" if bridge else "audit-helper")
        or labels.get("codex-owner") != run
        or labels.get("codex-purpose") != ("cce-pooler" if bridge else "cce-audit")
        or cfg.get("Entrypoint") != ["python"]
        or cfg.get("Cmd") != command
        or host.get("PortBindings", {}) != expected_ports
        or (bridge and (host.get("NanoCpus") != BRIDGE_CPU_LIMIT * 1_000_000_000
                        or host.get("Memory") != 64 * 1024 * 1024
                        or host.get("ReadonlyRootfs") is not True))
        or set(row.get("NetworkSettings", {}).get("Networks", {})) != {pool_network}
        or (require_running and not row.get("State", {}).get("Running"))
        or not row.get("State", {}).get("StartedAt")
    ):
        raise ValueError("Owned helper identity or configuration changed")
    return {
        "container_id": row["Id"],
        "started_at": row["State"]["StartedAt"],
        "image_id": row["Image"],
        "execution_sha256": policy.digest(execution_identity(row)),
    }


class Resources:
    def __init__(self, session, guard, run, owner, environment, persist):
        if (
            not re.fullmatch(r"adr0151-[0-9a-f]{12}", run)
            or owner != session.config["primary"]["repo"] + "/tmp/" + run
            or session.config["primary"]["private_ipv4"] != "10.1.137.69"
            or not isinstance(environment, dict)
            or any(
                not re.fullmatch(r"[A-Z][A-Z0-9_]*", k)
                or not isinstance(v, str)
                or any(x in v for x in ("\n", "\r", "\x00"))
                for k, v in environment.items()
            )
        ):
            raise ValueError("Canonical owner and resolved private helper environment required")
        self.session, self.guard, self.run, self.owner = session, guard, run, owner
        self.env = {**environment, **cce.contract()["api_settings"]}
        self.persist = persist
        self.network = None
        self.pool = None
        self.rows = {}
        self.record = {"create_attempted": [], "owned_resources_removed": False}
        self.names = {"bridge": "cce-pooler-" + run, "audit": "cce-audit-" + run}
        self.commands = {"bridge": ["-u", "-c", PROXY], "audit": ["-u", "-c", "import time;time.sleep(" + str(for_guard(guard).experiment_limit) + ")"]}

    def check(self):
        if (
            self.session.action_guard is not self.guard
            or for_guard(self.guard).name not in policy.PROFILES
            or for_guard(self.guard).name not in policy.PROFILES
            or for_guard(self.guard).name not in policy.envelope()["qualified_profiles"]
            or self.guard.binding.get("configuration_sha256") != policy.digest(self.session.config)
            or self.guard.binding.get("cce_bridge_cpu_limit") != BRIDGE_CPU_LIMIT
            or self.guard.binding.get("cce_resource_source_sha256")
            != paid.sha((policy.ROOT / "scripts/cce_paid_resources.py").read_bytes())
        ):
            raise ValueError("Exactly bound qualified native resources required")
        cce.authorized_creation(policy.envelope())
        self.guard.check(120)

    def checkpoint(self):
        self.persist(json.loads(json.dumps(self.record)))
        self.session.checkpoint()

    def inspect_name(self, role):
        name = self.names[role]
        return self.session.call(
            "primary",
            "import json,subprocess;ids=subprocess.check_output(['docker','ps','-aq','--no-trunc','--filter',"
            + repr("name=^/" + name + "$")
            + "],text=True).split();print(json.dumps(json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []))",
            45,
        )

    def create(self):
        self.check()
        if self.record["create_attempted"]:
            raise ValueError("Fresh resource lifecycle required")
        pools = [
            r
            for r in self.session.call("primary", INSPECT, 45)
            if r["Config"]["Labels"].get("com.docker.compose.service") == "pgbouncer"
        ]
        if len(pools) != 1 or not pools[0]["State"]["Running"]:
            raise ValueError("One live unchanged pooler required")
        self.pool = pools[0]
        nets = list(self.pool["NetworkSettings"]["Networks"])
        if len(nets) != 1:
            raise ValueError("Ambiguous pooler network")
        self.network = nets[0]
        for role in self.names:
            if self.inspect_name(role):
                raise ValueError("Owned helper name already exists")
        self.record["env_transfer_attempted"] = True
        self.checkpoint()
        self.session.put(
            "primary",
            self.owner + "/cce-audit.env",
            "\n".join(k + "=" + v for k, v in self.env.items()) + "\n",
            True,
        )
        self.record["environment_sha256"] = policy.digest(self.env)
        for role in ("audit", "bridge"):
            self.record["create_attempted"].append(role)
            self.checkpoint()
            options = [
                "docker",
                "run",
                "-d",
                "--pull",
                "never",
                "--name",
                self.names[role],
                "--label",
                "codex-owner=" + self.run,
                "--label",
                "com.docker.compose.project=cce-" + self.run,
                "--label",
                "com.docker.compose.service=" + ("pooler-bridge" if role == "bridge" else "audit-helper"),
                "--label",
                "codex-purpose=cce-" + ("pooler" if role == "bridge" else "audit"),
                "--network",
                self.network,
                "--cap-drop",
                "ALL",
                "--security-opt",
                "no-new-privileges",
                "--pids-limit",
                "128",
                "--entrypoint",
                "python",
                "--user",
                "10001:10001",
            ]
            if role == "bridge":
                options += [
                    "--read-only",
                    "--cpus",
                    str(BRIDGE_CPU_LIMIT),
                    "--memory",
                    "64m",
                    "-p",
                    "10.1.137.69:6432:6432",
                ]
            else:
                options += [
                    "--memory",
                    "1g",
                    "--env-file",
                    self.owner + "/cce-audit.env",
                ]
            options += [cce.dependency.INDEX, *self.commands[role]]
            try:
                self.session.call(
                    "primary",
                    "import json,socket,subprocess\n"
                    + ("with socket.socket() as s:s.bind(('10.1.137.69',6432))\n" if role == "bridge" else "")
                    + "args="
                    + repr(options)
                    + "\nsubprocess.run(args,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=30);print(json.dumps({'created':True}))",
                    45,
                )
            finally:
                # Recover a lost creation acknowledgement only with full ownership proof.
                rows = self.inspect_name(role)
                if len(rows) != 1:
                    raise ValueError("Ambiguous helper creation requires recovery")
                row = rows[0]
                receipt = owned_receipt(
                    row,
                    self.names[role],
                    self.run,
                    self.commands[role],
                    pool_network=self.network,
                    bridge=role == "bridge",
                )
                if role == "audit":
                    actual = dict(value.split("=", 1) for value in row["Config"]["Env"])
                    if any(actual.get(k) != v for k, v in self.env.items()):
                        raise ValueError("Audit environment changed")
                self.rows[role] = row
                self.record[role] = receipt
                self.checkpoint()
        return self.record["audit"]["container_id"]

    def cleanup(self):
        self.session.begin_cleanup()
        failures = []
        for role in reversed(self.record["create_attempted"]):
            try:
                rows = self.inspect_name(role)
                if not rows:
                    continue
                expected = self.rows.get(role)
                if (
                    expected is None
                    or len(rows) != 1
                    or execution_identity(rows[0]) != execution_identity(expected)
                ):
                    raise ValueError("Unknown helper identity; refusing deletion")
                owned_receipt(
                    rows[0],
                    self.names[role],
                    self.run,
                    self.commands[role],
                    pool_network=self.network,
                    bridge=role == "bridge",
                    require_running=False,
                )
                result = self.session.call(
                    "primary",
                    "expected="
                    + repr(execution_identity(expected))
                    + r"""
import json,subprocess
rows=json.loads(subprocess.check_output(['docker','inspect',expected['Id']],text=True))
if len(rows)!=1 or {k:rows[0][k] for k in expected}!=expected:raise ValueError('Helper changed before removal')
subprocess.run(['docker','rm','-f',expected['Id']],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=20)
print(json.dumps({'owned_helper_removed':True}))
""",
                    45,
                )
                if result.get("owned_helper_removed") is not True or self.inspect_name(role):
                    raise ValueError("Helper removal unverified")
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Clean independently owned resources despite another failure.
                failures.append({"role": role, "type": type(error).__name__})
        if self.record.get("env_transfer_attempted") and not failures:
            try:
                result = self.session.call(
                    "primary",
                    "from pathlib import Path;import json\np=Path("
                    + repr(self.owner)
                    + ")\nf=p/'cce-audit.env'\nif p.is_symlink() or not p.is_dir() or f.is_symlink():raise ValueError('Unknown private helper path')\nif f.exists():\n if not f.is_file():raise ValueError('Unsafe private helper file')\n f.unlink()\nprint(json.dumps({'helper_environment_removed':not f.exists()}))",
                    45,
                )
                if result.get("helper_environment_removed") is not True:
                    raise ValueError("Helper secret cleanup unverified")
                self.record.update(result)
            except (Exception, KeyboardInterrupt) as error:  # noqa: BLE001 - Record private cleanup uncertainty.
                failures.append({"role": "environment", "type": type(error).__name__})
        self.record["resource_cleanup_failures"] = failures
        self.record["owned_resources_removed"] = not failures
        self.checkpoint()
        if failures:
            raise ValueError("Native resource cleanup requires recovery")
        return dict(self.record)
