"""ADR0317 owned pause/reload/resume with exact original pooler rollback."""
import base64
import hashlib
import re
import time

import cce_backend_partition as partition
import work_envelope as policy

DISCOVER = r"""
import base64,hashlib,json,os,subprocess
ids=subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project=flash-ticketing','--filter','label=com.docker.compose.service=pgbouncer'],text=True).split()
if len(ids)!=1:raise ValueError('One running owned pooler required')
row=json.loads(subprocess.check_output(['docker','inspect',ids[0]],text=True))[0]
raw=subprocess.check_output(['docker','exec',ids[0],'cat','/etc/pgbouncer/pgbouncer.ini'])
stat=subprocess.check_output(['docker','exec',ids[0],'stat','-c','%u %g %a','/etc/pgbouncer/pgbouncer.ini'],text=True).split()
print(json.dumps({'container_id':ids[0],'image':row['Image'],'started_at':row['State']['StartedAt'],'config_b64':base64.b64encode(raw).decode(),'config_sha256':hashlib.sha256(raw).hexdigest(),'uid':int(stat[0]),'gid':int(stat[1]),'mode':int(stat[2],8)}))
"""
INSPECT = r"""
import json,subprocess
row=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0]
if row['Image']!=image or row['State']['StartedAt']!=started or not row['State']['Running']:raise ValueError('Owned pooler identity changed')
"""
ADMIN = r"""
import json,os,psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row
params=conninfo_to_dict(os.environ['DATABASE_URL']);params.update(dbname='pgbouncer',autocommit=True,connect_timeout=5)
with psycopg.connect(**params,row_factory=dict_row,prepare_threshold=None) as conn:
 result={name:conn.execute('SHOW '+name).fetchall() for name in names}
print(json.dumps(result,default=str))
"""


class Partition:
    def __init__(self, session, guard, run, audit_cid, owner, persist):
        if (not re.fullmatch(r"adr0151-[0-9a-f]{12}",run)
                or owner!=session.config["primary"]["repo"]+"/tmp/"+run
                or guard.binding.get("cce_backend_partition_contract")!=partition.contract()):
            raise ValueError("Exactly owned backend partition required")
        self.session,self.guard,self.run,self.cid,self.owner,self.persist=session,guard,run,audit_cid,owner,persist
        self.info=None
        self.record={"attempted":False,"restored":False,"paused":False}
        self.original=self.prepared=self.active=None

    def checkpoint(self):
        self.persist(dict(self.record))

    def check(self):
        if self.session.action_guard is not self.guard:
            raise ValueError("Current bounded action guard required")
        self.guard.check(15)

    def admin(self, names):
        self.check()
        return self.session.api(self.cid,"names="+repr(names)+"\n"+ADMIN,20)

    def prefix(self):
        return ("cid="+repr(self.info["container_id"])+"\nimage="+repr(self.info["image"])
                +"\nstarted="+repr(self.info["started_at"])+"\n"+INSPECT)

    def signal(self, value):
        self.check()
        if value not in {"USR1","USR2","HUP"}:
            raise ValueError("Only declared pooler signals allowed")
        self.record["signal_"+value+"_attempted"]=True;self.checkpoint()
        self.session.call("primary",self.prefix()+"\nsubprocess.run(['docker','kill','--signal="+value+"',cid],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=10);print(json.dumps({'signaled':True}))",20)

    def pause(self):
        self.record["paused"]=True;self.checkpoint()
        self.signal("USR1")
        deadline=time.monotonic()+20
        while self.admin(["SERVERS"])["SERVERS"]:
            if time.monotonic()>deadline:raise TimeoutError("Pooler pause did not release all backends")
            time.sleep(.25)
        self.record["all_backends_released"]=True;self.checkpoint()

    def write(self, raw):
        self.check()
        temporary="/etc/pgbouncer/."+self.run+"."+hashlib.sha256(raw).hexdigest()[:12]+".partition.ini"
        script=("created=0; trap 'if [ \"$created\" = 1 ]; then rm -f -- "+temporary+"; fi' EXIT; "
                +"test ! -e "+temporary+" && umask 077 && created=1 && cat > "+temporary
                +" && chown "+str(self.info["uid"])+":"+str(self.info["gid"])+" "+temporary
                +" && chmod "+format(self.info["mode"],"o")+" "+temporary
                +" && mv "+temporary+" /etc/pgbouncer/pgbouncer.ini")
        body=("raw=base64.b64decode("+repr(base64.b64encode(raw).decode())+")\n"
              +"subprocess.run(['docker','exec','--user','0','-i',cid,'sh','-c',"+repr(script)+"],input=raw,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=10)\n"
              +"actual=subprocess.check_output(['docker','exec',cid,'cat','/etc/pgbouncer/pgbouncer.ini'])\n"
              +"if actual!=raw:raise ValueError('Written pooler configuration mismatch')\n"
              +"print(json.dumps({'configuration_sha256':hashlib.sha256(actual).hexdigest()}))")
        self.record["write_attempted_sha256"]=hashlib.sha256(raw).hexdigest();self.checkpoint()
        value=self.session.call("primary","import base64,hashlib\n"+self.prefix()+"\n"+body,20)
        if value["configuration_sha256"]!=hashlib.sha256(raw).hexdigest():raise ValueError("Written pooler config receipt mismatch")
        self.record["configuration_sha256"]=value["configuration_sha256"];self.checkpoint()
        self.signal("HUP")

    def resume(self):
        self.signal("USR2")
        self.record["paused"]=False;self.checkpoint()

    def prepare(self):
        self.check()
        if self.record["attempted"]:raise ValueError("Fresh partition only")
        info=self.session.call("primary",DISCOVER,25)
        if info["image"]!=partition.POOLER_IMAGE or info["config_sha256"]!=partition.ORIGINAL_CONFIG:
            raise ValueError("Exactly pinned pooler image and original configuration required")
        backup=policy.ROOT/"tmp"/self.run/"pgbouncer-original.private.json"
        if backup.exists() or backup.is_symlink():
            raise ValueError("Fresh owned durable pooler snapshot required")
        policy.write(backup,info)
        self.info=info;self.original=base64.b64decode(info["config_b64"])
        self.record.update(backup_path=backup.relative_to(policy.ROOT).as_posix(),
                           backup_sha256=policy.digest(info))
        self.prepared=partition.render(self.original,phase="prepare");self.active=partition.render(self.original)
        self.admin(["DATABASES","CONFIG","SERVERS"])
        self.record.update(attempted=True,container_id=info["container_id"],original_config_sha256=info["config_sha256"])
        self.checkpoint()
        self.pause();self.write(self.prepared);self.resume()
        self.verify(phase="prepare")
        return True

    def activate(self):
        if not self.record["attempted"] or self.record["restored"]:raise ValueError("Prepared partition required")
        self.pause();self.write(self.active);self.resume()
        self.verify()
        self.record["active"]=True;self.checkpoint()
        return True

    def verify(self, *, phase="active"):
        data=self.admin(["DATABASES","SERVERS","CONFIG"])
        rows={r["name"]:r for r in data["DATABASES"] if r["name"]!="pgbouncer"}
        caps={**partition.ALIASES,"ticketing":24} if phase=="prepare" else partition.ALIASES
        settings={r["key"]:r["value"] for r in data["CONFIG"]}
        if (set(rows)!=set(caps) or settings.get("max_user_connections")!="24"
                or settings.get("pool_mode")!="transaction" or settings.get("reserve_pool_size")!="0"
                or settings.get("server_tls_sslmode")!="verify-full"):
            raise ValueError("Exact live backend partition settings required")
        for alias,cap in caps.items():
            r=rows[alias]
            if r["database"]!="ticketing" or r["pool_size"]!=cap or r["max_connections"]!=cap:
                raise ValueError("Declared alias/backend limits changed")
        if len(data["SERVERS"])>24 or any(r["user"]!="ticketing" for r in data["SERVERS"]):
            raise ValueError("Aggregate single-user backend budget exceeded")
        self.record["live_phase"]=phase;self.record["live_physical_backends"]=len(data["SERVERS"]);self.checkpoint()
        return True

    def restore(self):
        if not self.record["attempted"] or self.record["restored"]:return True
        self.session.begin_cleanup()
        self.pause();self.write(self.original);self.resume()
        data=self.admin(["DATABASES","SERVERS"])
        names={r["name"] for r in data["DATABASES"]}
        if names!={"ticketing","pgbouncer"} or any(r["database"]!="ticketing" for r in data["SERVERS"]):
            raise ValueError("Removed alias/backends still present")
        check=self.session.call("primary",DISCOVER,25)
        if any(check[k]!=self.info[k] for k in ("container_id","image","started_at","config_sha256","uid","gid","mode")):
            raise ValueError("Exact original pooler bytes/mode/identity not restored")
        self.record.update(restored=True,active=False);self.checkpoint()
        return True
