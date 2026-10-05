"""Bounded, self-contained source/import/bytecode proof for container preflight."""

import re

PATH = re.compile(r"src/ticketing(?:/[A-Za-z_][A-Za-z0-9_]*)+\.py")


def source_identity_program(expected, *, readiness=False):
    if not isinstance(expected, dict) or not 1 <= len(expected) <= 64 or type(readiness) is not bool:
        raise ValueError("Invalid bounded source identity map")
    if any(not isinstance(path, str) or not PATH.fullmatch(path)
           or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
           for path, digest in expected.items()):
        raise ValueError("Unsafe source identity path/digest")
    return r"""import hashlib,importlib.util,importlib.machinery,json,sys
from pathlib import Path
sys.dont_write_bytecode=True
expected=EXPECTED
imports={}
app={}
resolved={}
code_match=True
for relative,digest in expected.items():
 app[relative]=hashlib.sha256((Path('/app')/relative).read_bytes().replace(bytes([13,10]),bytes([10]))).hexdigest()
 module=relative.removeprefix('src/').removesuffix('.py').replace('/','.')
 spec=importlib.util.find_spec(module)
 if spec is None or not isinstance(spec.loader,importlib.machinery.SourceFileLoader):raise ValueError('Unsupported source loader')
 raw=Path(spec.origin).read_bytes()
 resolved[relative]=hashlib.sha256(raw.replace(bytes([13,10]),bytes([10]))).hexdigest()
 match=spec.loader.get_code(module)==compile(raw,spec.origin,'exec',dont_inherit=True,optimize=sys.flags.optimize)
 code_match=code_match and match
 imports[relative]={'module':module,'origin':spec.origin,'sha256':resolved[relative],'code_matches_source':match}
proof={'app_source_hashes_match':app==expected,'import_source_hashes_match':resolved==expected,
       'import_code_matches_source':code_match,'resolved_imports':imports}
proof['source_hashes_match']=proof['app_source_hashes_match'] and proof['import_source_hashes_match'] and code_match
if READY:
 import urllib.request
 proof['ready']=json.loads(urllib.request.urlopen('http://127.0.0.1:8000/health/ready',timeout=3).read())['status']=='ready'
print(json.dumps(proof))
""".replace("EXPECTED", repr(expected)).replace("READY", repr(readiness))


def container_identity_program(row, role, expected, *, readiness=False):
    """Verify inspected image/start identity around the import proof, without RPC replay."""
    cid, image = row.get("Id"), row.get("Image")
    started = row.get("State", {}).get("StartedAt")
    if (not isinstance(cid, str) or not re.fullmatch(r"[0-9a-f]{64}", cid)
            or not isinstance(image, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", image)
            or not isinstance(started, str) or not 1 <= len(started) <= 64
            or role not in {"api", "consumer", "reservation-writer", "maintenance", "publisher", "reconciler", "simulator"}):
        raise ValueError("Inspected immutable container/image/start identity required")
    proof = source_identity_program(expected, readiness=readiness)
    return r"""import json,subprocess
cid=CID
image=IMAGE
started=STARTED
role=ROLE
def inspect():
 current=json.loads(subprocess.check_output(['docker','inspect',cid],text=True,timeout=10))[0]
 if current['Id']!=cid or current['Image']!=image or current['State']['StartedAt']!=started or current['State']['Running'] is not True or current['Config']['Labels']['com.docker.compose.service']!=role:raise ValueError('Container image/start identity differs')
 return current
inspect()
proof=json.loads(subprocess.check_output(['docker','exec',cid,'python','-c',PROOF],text=True,timeout=15))
inspect()
if proof.get('source_hashes_match') is not True:raise ValueError('Import/source/bytecode proof differs')
print(json.dumps({'container_id':cid,'image_id':image,'started_at':started,'role':role,'source_identity':proof}))
""".replace("CID", repr(cid)).replace("IMAGE", repr(image)).replace("STARTED", repr(started)).replace("ROLE", repr(role)).replace("PROOF", repr(proof))
