"""ADR0228 zero-customer CCE dependency qualification; no paid capacity claim."""
import base64
import hashlib
import json
import re
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import work_envelope as policy

PROFILE = "cce_dependency_probe"
INDEX = "sha256:18ddd5d6faee7673eefa39eab39b3579eb66766f474a82768caa58852e31122c"
MANIFEST = "sha256:2fdad0b429603e01ce28eb0a578e8e0c87d98401868e389c1d92350c8969c38d"
CONFIG = "sha256:56adbbcd473749b8cfe23da9decf7a6d4a0538fb2409fe40eb081218325287cf"
REGISTRY = "swr.ap-southeast-2.myhuaweicloud.com"
IMAGE = REGISTRY + "/chawansit/flash-ticketing@" + MANIFEST
SERVER = "https://dd1a8efd-c2b5-11f1-bd1d-0255ac1001d4.cluster.cce.ap-southeast-2.myhuaweicloud.com:5443"


def plan():
    return {"decision": "ADR0228", "arms": [],
            "common": {"buyer_journeys_per_second": 0, "duration_seconds": 0},
            "kind": "tcp_dependency_qualification_only", "maximum_pods": 1,
            "pod_resources": {"cpu": "1", "memory": "1Gi"}, "customer_writes": 0,
            "probe_deadline_seconds": 240, "cleanup_deadline_seconds": 120, "image": IMAGE,
            "private_bridge_port": 6432, "pooler_upstream_port": 5432}


def authorized_today(envelope, now=None):
    now = now or datetime.now(UTC)
    exception = envelope.get("spending", {}).get("temporary_cce_pilot_exception", {})
    try:
        expiry = datetime.fromisoformat(exception["expires_at_bangkok"])
        start = datetime.fromisoformat(exception["authorized_date_bangkok"] + "T00:00:00+07:00")
        valid = (now.tzinfo is not None and expiry.tzinfo is not None
                 and exception["decision"] == "ADR0220"
                 and exception["no_spending_cap_explicitly_authorized"] is True
                 and start <= now and now + timedelta(seconds=480) < expiry)
    except (KeyError, TypeError, ValueError):
        valid = False
    if not valid:
        raise ValueError("CCE spending authorization expired or cleanup margin insufficient")


def identity():
    return {"scripts/" + name: hashlib.sha256((policy.ROOT / "scripts" / name).read_bytes()).hexdigest()
            for name in ("cce_dependency_probe.py", "work_envelope.py", "run_work_envelope.py")}


def validate_proof(proof):
    if (proof.get("pass") is not True or proof.get("registry_reference") != IMAGE
            or proof.get("original_local_image_index_digest") != INDEX
            or proof.get("docker_configuration_digest") != CONFIG
            or proof.get("manifest_config_verified") is not True
            or proof.get("platform") != "linux/amd64"):
        raise ValueError("Exact frozen Linux platform image proof required")


def namespace_for(run):
    if not re.fullmatch(r"adr0151-[0-9a-f]{12}", run):
        raise ValueError("Canonical experiment identity required")
    return "flash-cce-" + run.split("-")[1]


def kube_material(path):
    import yaml

    kube = yaml.safe_load(Path(path).read_text(encoding="utf-8-sig"))
    context = next(c["context"] for c in kube["contexts"] if c["name"] == kube["current-context"])
    cluster = next(c["cluster"] for c in kube["clusters"] if c["name"] == context["cluster"])
    user = next(c["user"] for c in kube["users"] if c["name"] == context["user"])
    if cluster["server"] != SERVER or cluster.get("insecure-skip-tls-verify", False):
        raise ValueError("Exact verified-TLS CCE endpoint required")
    return {name: base64.b64decode(value, validate=True).decode() for name, value in {
        "ca.crt": cluster["certificate-authority-data"],
        "client.crt": user["client-certificate-data"], "client.key": user["client-key-data"]}.items()}


def outcome(report, scope, binding):
    valid = (report.get("run") == binding.get("run_id") and binding.get("run_id") is not None
             and type(report.get("customer_writes")) is int and type(report.get("capacity_stages_started")) is int
             and scope.get("binding") == binding and scope.get("cce_result_sha256") == policy.digest(report)
             and report.get("customer_writes") == 0 and report.get("capacity_stages_started") == 0
             and isinstance(report.get("runtime_fingerprint_before"), str)
             and re.fullmatch(r"[0-9a-f]{64}", report["runtime_fingerprint_before"]) is not None
             and report.get("runtime_fingerprint_before") == report.get("runtime_fingerprint_after")
             and report.get("runtime_drift") == [])
    restored = valid and all(report.get(k) is True for k in (
        "namespace_removed", "bridge_removed", "existing_runtime_unchanged",
        "generator_idle_after", "temporary_credentials_removed"))
    return restored, valid, restored and report.get("pass") is True


def remote_program(material, run, username, password):
    payload = {"material": material, "run": run, "namespace": namespace_for(run), "server": SERVER,
               "username": username, "password": password, "image": IMAGE, "local_image": INDEX}
    return "payload=" + repr(payload) + "\n" + REMOTE


REMOTE = r"""
import base64,hashlib,json,os,socket,ssl,subprocess,tempfile,time,urllib.request,urllib.error
from pathlib import Path
run=payload['run'];namespace=payload['namespace'];name='cce-pooler-'+run
result={'run':run,'customer_writes':0,'capacity_stages_started':0}
namespace_uid=None;bridge_id=None;namespace_attempted=False;bridge_attempted=False
def runtime():
 ids=subprocess.check_output(['docker','ps','-aq','--no-trunc','--filter','label=com.docker.compose.project=flash-ticketing'],text=True).split()
 rows=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
 return sorted([{'id':r['Id'],'image':r['Image'],'state':{k:r['State'].get(k) for k in ('Running','Paused','Restarting','Dead','StartedAt','FinishedAt')},'restart_count':r.get('RestartCount'),'config':r['Config'],'host_config':r['HostConfig'],'mounts':sorted(r['Mounts'],key=lambda v:json.dumps(v,sort_keys=True))} for r in rows],key=lambda r:r['id'])
before=runtime()
def fingerprint(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
result['runtime_fingerprint_before']=fingerprint(before)
with tempfile.TemporaryDirectory(prefix='codex-cce-') as directory:
 os.chmod(directory,0o700)
 for cert,content in payload.pop('material').items():
  path=Path(directory)/cert
  with path.open('x') as stream:os.chmod(path,0o600);stream.write(content)
 context=ssl.create_default_context(cafile=directory+'/ca.crt');context.load_cert_chain(directory+'/client.crt',directory+'/client.key')
 def request(method,path,body=None):
  req=urllib.request.Request(payload['server']+path,data=json.dumps(body).encode() if body is not None else None,headers={'Content-Type':'application/json'},method=method)
  with urllib.request.urlopen(req,context=context,timeout=10) as response:return json.load(response)
 ns_path='/api/v1/namespaces/'+namespace
 try:
  try:request('GET',ns_path)
  except urllib.error.HTTPError as error:
   if error.code!=404:raise
  else:raise ValueError('Namespace already exists')
  if subprocess.check_output(['docker','ps','-aq','--filter','name=^/'+name+'$'],text=True).strip():raise ValueError('Bridge already exists')
  with socket.socket() as probe:probe.bind(('10.1.137.69',6432))
  pools=subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project=flash-ticketing','--filter','label=com.docker.compose.service=pgbouncer'],text=True).split()
  if len(pools)!=1:raise ValueError('One original pooler required')
  pool=json.loads(subprocess.check_output(['docker','inspect',pools[0]],text=True))[0]
  networks=list(pool['NetworkSettings']['Networks'])
  if len(networks)!=1:raise ValueError('Pooler network ambiguous')
  proxy='''import asyncio
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
'''
  bridge_attempted=True
  bridge_id=subprocess.check_output(['docker','run','-d','--name',name,'--label','codex-owner='+run,'--network',networks[0],'-p','10.1.137.69:6432:6432','--cpus','0.125','--memory','64m','--pids-limit','64','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--entrypoint','python',payload['local_image'],'-u','-c',proxy],text=True,stderr=subprocess.PIPE,timeout=30).strip()
  namespace_attempted=True
  ns=request('POST','/api/v1/namespaces',{'apiVersion':'v1','kind':'Namespace','metadata':{'name':namespace,'labels':{'codex-owner':run}}})
  namespace_uid=ns['metadata']['uid'];result['namespace_uid']=namespace_uid
  auth=base64.b64encode((payload['username']+':'+payload.pop('password')).encode()).decode()
  docker={'auths':{'swr.ap-southeast-2.myhuaweicloud.com':{'auth':auth}}}
  request('POST',ns_path+'/secrets',{'apiVersion':'v1','kind':'Secret','metadata':{'name':'swr-pull','labels':{'codex-owner':run}},'type':'kubernetes.io/dockerconfigjson','data':{'.dockerconfigjson':base64.b64encode(json.dumps(docker).encode()).decode()}})
  auth=docker=None
  probe='''import json,socket,struct
from http.server import BaseHTTPRequestHandler,HTTPServer
checks={}
for role,host,port in [('rds','10.1.228.129',5432),('redis','10.1.245.61',6379),('pooler','10.1.137.69',6432)]:
 try:
  with socket.create_connection((host,port),timeout=5) as conn:
   if role in {'rds','pooler'}:
    conn.sendall(struct.pack('!II',8,80877103))
    if conn.recv(1) not in {b'S',b'N'}:raise OSError('Invalid PostgreSQL SSL negotiation response')
  checks[role]={'tcp_connected':True,'postgresql_protocol_checked':role in {'rds','pooler'}}
 except OSError as error:checks[role]={'tcp_connected':False,'failure_type':type(error).__name__}
result={'checks':checks,'all_tcp_connected':all(x['tcp_connected'] for x in checks.values()),'customer_writes':0}
print(json.dumps(result),flush=True)
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  body=json.dumps(result).encode();self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def log_message(self,*args):pass
HTTPServer(('0.0.0.0',8000),Handler).serve_forever()
'''
  resources={'requests':{'cpu':'1','memory':'1Gi'},'limits':{'cpu':'1','memory':'1Gi'}}
  pod={'apiVersion':'v1','kind':'Pod','metadata':{'name':'dependency-probe','labels':{'codex-owner':run}},'spec':{'restartPolicy':'Never','activeDeadlineSeconds':240,'automountServiceAccountToken':False,'imagePullSecrets':[{'name':'swr-pull'}],'securityContext':{'runAsUser':10001,'runAsNonRoot':True},'containers':[{'name':'probe','image':payload['image'],'imagePullPolicy':'Always','command':['python','-u','-c',probe],'securityContext':{'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}},'resources':resources}]}}
  pod=request('POST',ns_path+'/pods',pod);pod_uid=pod['metadata']['uid'];result['pod_uid']=pod_uid
  deadline=time.monotonic()+150
  while time.monotonic()<deadline:
   pod=request('GET',ns_path+'/pods/dependency-probe')
   if pod['metadata']['uid']!=pod_uid:raise ValueError('Pod identity changed')
   statuses=pod.get('status',{}).get('containerStatuses',[])
   if statuses and any(c.get('restartCount',0) for c in statuses):raise ValueError('Probe restarted')
   if pod.get('status',{}).get('phase')=='Running' and statuses and all('running' in c.get('state',{}) for c in statuses):break
   if pod.get('status',{}).get('phase') in {'Failed','Succeeded'}:raise ValueError('Probe stopped early')
   time.sleep(2)
  else:raise TimeoutError('Probe startup deadline')
  result.update(pod_ip=pod['status']['podIP'],image_id=statuses[0].get('imageID'),resources=pod['spec']['containers'][0]['resources'])
  if 'sha256:2fdad0b429603e01ce28eb0a578e8e0c87d98401868e389c1d92350c8969c38d' not in result['image_id']:raise ValueError('Pulled image differs')
  import ipaddress
  if not ipaddress.ip_address(result['pod_ip']).is_private:raise ValueError('Private CCE pod IP required')
  for attempt in range(12):
   try:
    with urllib.request.urlopen('http://'+result['pod_ip']+':8000',timeout=5) as response:result['network']=json.load(response)
    result['ecs_to_pod_reachable']=True;break
   except OSError:time.sleep(1)
  else:raise TimeoutError('ECS cannot reach CCE pod')
  result['pass']=result['network']['all_tcp_connected']
 except Exception as error:
  result.update({'pass':False,'failure_type':type(error).__name__,'http_status':getattr(error,'code',None)})
  if namespace_uid:
   try:
    pod=request('GET',ns_path+'/pods/dependency-probe')
    result['pod_phase']=pod.get('status',{}).get('phase')
    result['waiting_reasons']=[c.get('state',{}).get('waiting',{}).get('reason') for c in pod.get('status',{}).get('containerStatuses',[])]
   except Exception:pass
 finally:
  result['namespace_removed']=not namespace_attempted
  result['bridge_removed']=not bridge_attempted
  try:
   if namespace_attempted:
    ns=request('GET',ns_path)
    if namespace_uid is None or ns['metadata']['uid']!=namespace_uid or ns['metadata']['labels'].get('codex-owner')!=run:raise ValueError('Ambiguous namespace identity')
    request('DELETE',ns_path,{'apiVersion':'v1','kind':'DeleteOptions','preconditions':{'uid':namespace_uid}})
    deadline=time.monotonic()+90
    while time.monotonic()<deadline:
     try:request('GET',ns_path)
     except urllib.error.HTTPError as error:
      if error.code==404:result['namespace_removed']=True;break
      raise
     time.sleep(2)
  except Exception as error:result['namespace_cleanup_failure_type']=type(error).__name__
  try:
   if bridge_attempted:
    ids=subprocess.check_output(['docker','ps','-aq','--no-trunc','--filter','name=^/'+name+'$'],text=True).split()
    if len(ids)!=1 or bridge_id is None or ids[0]!=bridge_id:raise ValueError('Ambiguous bridge creation')
    row=json.loads(subprocess.check_output(['docker','inspect',bridge_id],text=True))[0]
    if row['Name']!='/'+name or row['Image']!=payload['local_image'] or row['Config']['Labels'].get('codex-owner')!=run:raise ValueError('Bridge owner changed')
    subprocess.run(['docker','rm','-f',bridge_id],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=15)
    result['bridge_removed']=not subprocess.check_output(['docker','ps','-aq','--filter','name=^/'+name+'$'],text=True).strip()
  except Exception as error:result['bridge_cleanup_failure_type']=type(error).__name__
result['temporary_credentials_removed']=not Path(directory).exists()
after=runtime()
result['runtime_fingerprint_after']=fingerprint(after)
result['existing_runtime_unchanged']=before==after
old={r['id']:r for r in before};new={r['id']:r for r in after}
result['runtime_drift']=[{'container_id':cid,'fields':[k for k in sorted(set(old.get(cid,{}))|set(new.get(cid,{}))) if old.get(cid,{}).get(k)!=new.get(cid,{}).get(k)]} for cid in sorted(set(old)|set(new)) if old.get(cid)!=new.get(cid)]
print(json.dumps(result))
"""


def execute(config_path, proof_path, ssh_runtime, kubeconfig, ecs_password, username, password):
    from qualify_two_host_deployment import GENERATOR_IDLE, Session
    from run_status_refresh_comparison import RunLock
    from stage_status_refresh_images import new_stage_output

    config, proof = policy.read(config_path), policy.read(proof_path)
    validate_proof(proof)
    material = kube_material(kubeconfig)
    authorized_today(policy.envelope())
    run = "adr0151-" + uuid4().hex[:12]
    binding = {"run_id": run, "configuration_sha256": policy.digest(config), "cce_sources": identity(),
               "image_proof_sha256": policy.digest(proof)}
    output = new_stage_output()
    output.mkdir()
    lock = RunLock(run)
    report = {"run": run, "pass": False, "customer_writes": 0, "capacity_stages_started": 0}
    started = time.monotonic()
    entry = guard = session = None
    try:
        entry = policy.reserve(binding, plan(), profile=PROFILE)
        guard = policy.ActionGuard(entry["ledger"], binding)
        sys.path.insert(0, str(ssh_runtime.resolve()))
        session = Session(config, output, ecs_password, action_guard=guard)
        state = policy.read(policy.STATE)
        if state[entry["ledger"]]["qualification_protocols_started"] != 0:
            raise ValueError("Fresh CCE qualification required")
        state[entry["ledger"]]["qualification_protocols_started"] = 1
        policy.write(policy.STATE, state)
        session.phase("cce-bounded-dependency-probe")
        code = remote_program(material, run, username, password)
        material.clear()
        password = ecs_password = None
        report = session.call("primary", code, timeout=360)
        code = None
        session.begin_cleanup()
        report["generator_idle_after"] = session.call("generator", GENERATOR_IDLE, timeout=30).get("generator_idle") is True
        restored, _, passed = outcome(report, {"binding": binding,
            "cce_result_sha256": policy.digest(report)}, binding)
        report["pass"] = passed
        if not restored:
            report["recovery_required"] = True
    finally:
        if session is not None:
            session.close()
        material.clear()
        password = ecs_password = None
        policy.write(output / "cce-probe.json", report)
        if entry is not None:
            state = policy.read(policy.STATE)
            state[entry["ledger"]]["cce_result_sha256"] = policy.digest(report)
            policy.write(policy.STATE, state)
            receipt = guard.finish([report], time.monotonic() - started)
        lock.release()
    print(json.dumps({"phase": "cce-dependency-probe-finished", "status": receipt["status"],
                      "evidence": str(output / "cce-probe.json"), "report": report}), flush=True)
    return receipt
