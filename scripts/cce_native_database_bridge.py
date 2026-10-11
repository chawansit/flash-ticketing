"""ADR0279 explicit fixed-budget native TCP forwarding contract."""
import work_envelope as policy

IMAGE = "sha256:65645c7bb6a0661892a8b03b89d0743208a18dd2f3f17a54ef4b76fb8e2f2a10"
REPOSITORY = "nginx@" + IMAGE
DESTINATION = "/etc/nginx/cce-stream.conf"
TMPFS = "rw,noexec,nosuid,nodev,size=4m,mode=700,uid=10001,gid=10001"
COMMAND = ["-c", DESTINATION, "-g", "daemon off;"]
CONFIG = """worker_processes 1;
pid /tmp/nginx.pid;
lock_file /tmp/nginx.lock;
error_log stderr warn;
events { worker_connections 1024; }
stream {
    server {
        listen 6432;
        proxy_pass pgbouncer:5432;
        proxy_connect_timeout 5s;
        proxy_timeout 300s;
        proxy_buffer_size 64k;
        proxy_next_upstream off;
        proxy_half_close off;
        proxy_protocol off;
        proxy_ssl off;
    }
}
"""


def contract():
    import hashlib
    return {"decision": "ADR0279", "image_id": IMAGE, "repository_digest": REPOSITORY,
            "configuration_sha256": hashlib.sha256(CONFIG.encode()).hexdigest(),
            "entrypoint": ["nginx"], "command": COMMAND, "cpu": 1, "memory_bytes": 67108864,
            "tmpfs": {"/tmp": TMPFS}, "database_connections": 24,
            "connect_timeout_seconds": 5, "inactivity_timeout_seconds": 300,
            "tls_termination": False, "upstream_replay": False}


def selected(goal):
    value = goal.get("native_database_bridge_decision") if goal else None
    if value is None:
        return False
    if (value != "ADR0279" or goal.get("native_database_bridge_sha256") != policy.digest(contract())
            or goal.get("timeout_retention_decision") != "ADR0278"
            or goal.get("extension_decision") != "ADR0271"):
        raise ValueError("Exact explicitly selected native database bridge required")
    return True


def image_program():
    return "image=" + repr(IMAGE) + "\nrepository=" + repr(REPOSITORY) + "\n" + r"""
import json,subprocess
rows=json.loads(subprocess.check_output(['docker','image','inspect',image],text=True))
if len(rows)!=1 or rows[0]['Id']!=image or repository not in rows[0].get('RepoDigests',[]):
 raise ValueError('Exact cached native bridge image required')
r=subprocess.run(['docker','run','--rm','--pull','never','--network','none','--read-only',
 '--cap-drop','ALL','--security-opt','no-new-privileges','--user','10001:10001',
 '--cpus','1','--memory','64m','--entrypoint','nginx',image,'-V'],capture_output=True,text=True,timeout=15)
if r.returncode or 'nginx/1.27.5' not in r.stderr or '--with-stream ' not in r.stderr:
 raise ValueError('Pinned built-in native stream module required')
print(json.dumps({'native_image_verified':True,'image_id':image,'repository_digest':repository}))
"""


def qualify_row(row, owner):
    cfg, host = row.get("Config", {}), row.get("HostConfig", {})
    mounts = row.get("Mounts", [])
    if (row.get("Image") != IMAGE or cfg.get("Image") != IMAGE or cfg.get("Entrypoint") != ["nginx"]
            or cfg.get("Cmd") != COMMAND or cfg.get("User") != "10001:10001"
            or host.get("Tmpfs") != {"/tmp": TMPFS}
            or len(mounts) != 1 or mounts[0].get("Type") != "bind"
            or mounts[0].get("Source") != owner + "/cce-stream.conf"
            or mounts[0].get("Destination") != DESTINATION or mounts[0].get("RW") is not False):
        raise ValueError("Native bridge execution or read-only configuration changed")


def config_program(owner, *, prepare=False):
    return "owner=" + repr(owner) + "\nexpected=" + repr(contract()["configuration_sha256"]) + "\nprepare=" + repr(prepare) + "\n" + r"""
import hashlib,json,os,stat
from pathlib import Path
p=Path(owner);f=p/'cce-stream.conf'
if p.is_symlink() or not p.is_dir() or f.is_symlink() or not f.is_file():
 raise ValueError('Canonical owned native bridge configuration required')
if hashlib.sha256(f.read_bytes()).hexdigest()!=expected:raise ValueError('Native bridge configuration drift')
if prepare:
 os.chown(f,10001,10001);os.chmod(f,0o400)
s=f.stat()
if s.st_uid!=10001 or s.st_gid!=10001 or stat.S_IMODE(s.st_mode)!=0o400:
 raise ValueError('Native configuration must be readable only by the unprivileged owner')
print(json.dumps({'native_configuration_verified':True,'configuration_sha256':expected}))
"""
