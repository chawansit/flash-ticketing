"""ADR0194 private configuration only; no connection factory, deployment or load CLI."""
import copy
import hashlib
import json
import re

import work_envelope as policy
from fetch_status_refresh_parents import owner_path, validate_owner
from two_host_topology import literal_model
from worker_separation_preload import binding_for
from worker_separation_runtime import RuntimeActions
from worker_separation_topology import validate_pair

MAX_FILE = 1024 * 1024
HASH = re.compile(r'[0-9a-f]{64}$')


def bundle(pair, saved, arm, host, scope_sha256):
    validate_pair(pair)
    if (arm not in {'control', 'candidate'} or host not in {'primary', 'secondary'}
            or not isinstance(scope_sha256, str) or not HASH.fullmatch(scope_sha256) or saved.get('decision') != 'ADR0186'
            or saved.get('schema') != 1):
        raise ValueError('Exact worker pair, snapshot, arm, host and scope required')
    model = pair[arm][host]
    if model is None:
        raise ValueError('No secondary configuration in the control arm')
    documents = {'arm.compose.json': model}
    if host == 'primary':
        documents['restore.compose.json'] = saved['model']
    files = {name: json.dumps(literal_model(value), sort_keys=True, separators=(',', ':'), allow_nan=False) + '\n'
             for name, value in documents.items()}
    if any(not 0 < len(value.encode('utf-8')) <= MAX_FILE for value in files.values()):
        raise ValueError('Bounded nonempty configuration required')
    manifest = {'decision': 'ADR0194', 'arm': arm, 'host': host,
                'pair_sha256': policy.digest(pair), 'saved_runtime_sha256': policy.digest(saved),
                'scope_binding_sha256': scope_sha256,
                'files': {name: {'sha256': hashlib.sha256(value.encode('utf-8')).hexdigest(),
                                 'size': len(value.encode('utf-8'))} for name, value in files.items()}}
    return {'manifest': manifest, 'files': files}


# Used verbatim in both installation and verification/cleanup programs.
POSIX = r'''
import hashlib,json,os,stat
from pathlib import Path

def identity(value):
 return {'device':value.st_dev,'inode':value.st_ino,'uid':value.st_uid,'mode':stat.S_IMODE(value.st_mode)}

def open_parent(owner):
 path=Path(owner)
 if path.parent.resolve()!=path.parent:raise ValueError('Canonical existing parent required')
 parent=os.open(str(path.parent),os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
 before=os.fstat(parent);current=os.stat(str(path.parent),follow_symlinks=False)
 if before.st_uid!=os.geteuid() or stat.S_IMODE(before.st_mode)&0o022 or (before.st_dev,before.st_ino)!=(current.st_dev,current.st_ino):
  os.close(parent);raise ValueError('Parent identity changed')
 return parent,path.name

def observe(parent,name,manifest):
 fd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent)
 try:
  directory=os.fstat(fd)
  if directory.st_uid!=os.geteuid() or stat.S_IMODE(directory.st_mode)!=0o700:raise ValueError('Owner-only directory required')
  if set(os.listdir(fd))!=set(manifest['files']):raise ValueError('Exact configuration file set required')
  files={}
  for filename,expected in manifest['files'].items():
   child=os.open(filename,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=fd)
   try:
    before=os.fstat(child)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_uid!=os.geteuid() or stat.S_IMODE(before.st_mode)!=0o600 or before.st_size!=expected['size']:raise ValueError('Exact owner-only regular file required')
    content=b''
    while len(content)<=expected['size']:
     block=os.read(child,min(65536,expected['size']+1-len(content)))
     if not block:break
     content+=block
    after=os.fstat(child);current=os.stat(filename,dir_fd=fd,follow_symlinks=False)
    if (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns) or (current.st_dev,current.st_ino)!=(before.st_dev,before.st_ino):raise ValueError('Configuration changed during observation')
    if len(content)!=expected['size'] or hashlib.sha256(content).hexdigest()!=expected['sha256']:raise ValueError('Configuration content differs')
    files[filename]={**identity(before),**expected}
   finally:os.close(child)
  current=os.stat(name,dir_fd=parent,follow_symlinks=False)
  if (current.st_dev,current.st_ino)!=(directory.st_dev,directory.st_ino):raise ValueError('Owner directory replaced')
  return {'directory':identity(directory),'files':files}
 finally:os.close(fd)
'''


def validate_manifest(manifest):
    keys = {'decision', 'arm', 'host', 'pair_sha256', 'saved_runtime_sha256', 'scope_binding_sha256', 'files'}
    if (not isinstance(manifest, dict) or set(manifest) != keys or manifest['decision'] != 'ADR0194'
            or manifest['arm'] not in {'control', 'candidate'} or manifest['host'] not in {'primary', 'secondary'}
            or any(not isinstance(manifest[k], str) or not HASH.fullmatch(manifest[k])
                   for k in ('pair_sha256', 'saved_runtime_sha256', 'scope_binding_sha256'))
            or not isinstance(manifest['files'], dict)
            or set(manifest['files']) != ({'arm.compose.json', 'restore.compose.json'}
                                         if manifest['host'] == 'primary' else {'arm.compose.json'})
            or (manifest['host'] == 'secondary' and manifest['arm'] != 'candidate')):
        raise ValueError('Exact bounded configuration manifest required')
    for record in manifest['files'].values():
        if (not isinstance(record, dict) or set(record) != {'size', 'sha256'}
                or type(record['size']) is not int or not 0 < record['size'] <= MAX_FILE
                or not isinstance(record['sha256'], str) or not HASH.fullmatch(record['sha256'])):
            raise ValueError('Bounded manifest file identity required')


def install_program(owner, prepared):
    owner = validate_owner(owner)
    if not isinstance(prepared, dict) or set(prepared) != {'files', 'manifest'}:
        raise ValueError('Exact generated bundle required')
    files, manifest = prepared['files'], prepared['manifest']
    validate_manifest(manifest)
    # Verify content locally before transmitting it; never accept caller-chosen filenames.
    if (set(files) not in ({'arm.compose.json'}, {'arm.compose.json', 'restore.compose.json'})
            or set(files) != set(manifest['files'])
            or any(not isinstance(value, str) or not 0 < len(value.encode()) <= MAX_FILE
                   or manifest['files'][name] != {'size': len(value.encode()),
                                                  'sha256': hashlib.sha256(value.encode()).hexdigest()}
                   for name, value in files.items())):
        raise ValueError('Exact bounded generated documents required')
    return POSIX + '\nowner=' + repr(owner) + '\nmanifest=' + repr(manifest) + '\npayload=' + repr(files) + r'''
parent,name=open_parent(owner)
try:
 os.mkdir(name,mode=0o700,dir_fd=parent) # Exclusive creation; never adopt an existing owner.
 os.fsync(parent)
 fd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent)
 try:
  for filename,content in payload.items():
   child=os.open(filename,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=fd)
   try:
    encoded=content.encode('utf-8');offset=0
    while offset<len(encoded):
     count=os.write(child,encoded[offset:])
     if count<=0:raise OSError('Incomplete configuration write')
     offset+=count
    os.fsync(child)
   finally:os.close(child)
  os.fsync(fd)
 finally:os.close(fd)
 sealed=observe(parent,name,manifest)
 print(json.dumps({'owner':owner,'manifest':manifest,**sealed}))
finally:os.close(parent)
'''


def validate_seal(seal, owner, manifest):
    validate_manifest(manifest)
    if (not isinstance(seal, dict) or set(seal) != {'owner', 'manifest', 'directory', 'files'}
            or seal['owner'] != validate_owner(owner) or seal['manifest'] != manifest
            or not isinstance(seal['files'], dict) or set(seal['files']) != set(manifest['files'])):
        raise ValueError('Exact configuration seal required')
    records = [seal['directory'], *seal['files'].values()]
    for index, record in enumerate(records):
        keys = {'device', 'inode', 'uid', 'mode'} | (set() if index == 0 else {'sha256', 'size'})
        if (not isinstance(record, dict) or set(record) != keys
                or any(type(record[k]) is not int or record[k] < 0 for k in ('device', 'inode', 'uid', 'mode'))
                or record['inode'] == 0 or record['mode'] != (0o700 if index == 0 else 0o600)):
            raise ValueError('Exact POSIX configuration identities required')
    for filename, record in seal['files'].items():
        if {k: record[k] for k in ('sha256', 'size')} != manifest['files'][filename]:
            raise ValueError('Sealed file differs from bound manifest')
    return copy.deepcopy(seal)


def sealed_program(seal, *, cleanup=False):
    if type(cleanup) is not bool:
        raise TypeError('Explicit cleanup boolean required')
    seal = validate_seal(seal, seal['owner'], seal['manifest'])
    return POSIX + '\nsealed=' + repr(seal) + '\ncleanup=' + repr(cleanup) + r'''
parent,name=open_parent(sealed['owner'])
try:
 observed=observe(parent,name,sealed['manifest'])
 if observed!={'directory':sealed['directory'],'files':sealed['files']}:raise ValueError('Sealed configuration identity changed')
 if cleanup:
  fd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent)
  try:
   if identity(os.fstat(fd))!=sealed['directory'] or set(os.listdir(fd))!=set(sealed['files']):raise ValueError('Owned configuration changed before cleanup')
   for filename,expected in sealed['files'].items():
    current=os.stat(filename,dir_fd=fd,follow_symlinks=False)
    if not stat.S_ISREG(current.st_mode) or identity(current)!={k:expected[k] for k in ('device','inode','uid','mode')}:raise ValueError('File replaced before cleanup')
    os.unlink(filename,dir_fd=fd)
   os.fsync(fd)
   if os.listdir(fd):raise ValueError('Unowned files prevent directory cleanup')
   if identity(os.stat(name,dir_fd=parent,follow_symlinks=False))!=sealed['directory']:raise ValueError('Directory replaced before cleanup')
   os.rmdir(name,dir_fd=parent);os.fsync(parent)
  finally:os.close(fd)
 print(json.dumps({'owned_configuration_removed':True} if cleanup else {'configuration_matches':True}))
finally:os.close(parent)
'''


class ConfigurationActions:
    """Guarded configuration component; ADR0201 supplies the registered worker profile."""
    def __init__(self, session, guard, inputs, saved, arm, output, *, handover=None):
        self.runtime = RuntimeActions(session, guard, inputs, saved, arm, output, handover=handover)
        self.session = session
        self.arm, self.scope_key = arm, guard.key
        self.used, self.seals, self.failed, self.cleanup_used = set(), {}, False, set()

    def install(self, host):
        if (self.failed or host in self.used or host not in {'primary', 'secondary'}
                or self.runtime.guard.key != self.scope_key):
            raise ValueError('Single-use configuration installation; no ambiguous replay')
        self.runtime._authorize(45)
        if (binding_for(self.runtime.inputs) != self.runtime.expected
                or policy.digest(self.runtime.saved) != self.runtime.expected['worker_saved_runtime_sha256']):
            raise ValueError('Prepared configuration binding changed')
        prepared = bundle(self.runtime.inputs['pair'], self.runtime.saved, self.arm, host,
                          self.runtime.scope_binding_sha256)
        owner = owner_path(self.session.config[host]['repo'], self.runtime.output.name)
        self.used.add(host)
        try:
            self.runtime._write('configuration-intent', {'host': host, 'manifest_sha256': policy.digest(prepared['manifest'])})
            received = self.session.call(host, install_program(owner, prepared), 45)
            seal = validate_seal(received, owner, prepared['manifest'])
            self.seals[host] = seal  # Preserve known ownership even if local acknowledgement persistence fails.
            self.runtime._write('configuration-seal', {'host': host, 'seal': seal})
            self.runtime._write('configuration-ack', {'host': host, 'manifest_sha256': policy.digest(prepared['manifest'])})
            return {'configuration_installed': True, 'manifest_sha256': policy.digest(prepared['manifest'])}
        except BaseException:
            self.failed = True
            raise

    def verify(self, host):
        if (self.failed or host not in self.seals or host in self.cleanup_used
                or self.runtime.guard.key != self.scope_key):
            raise ValueError('Successful sealed installation required')
        self.runtime._authorize(30)
        try:
            if self.session.call(host, sealed_program(self.seals[host]), 30) != {'configuration_matches': True}:
                raise ValueError('Exact configuration verification acknowledgement required')
            return {'configuration_matches': True}
        except BaseException:
            self.failed = True
            raise

    def cleanup(self, host):
        if host not in self.seals or host in self.cleanup_used:
            raise ValueError('Known sealed ownership required; cleanup is single-use')
        if (getattr(self.session, 'action_guard', None) is not self.runtime.guard
                or policy.digest(self.runtime.guard.binding) != self.runtime.scope_binding_sha256
                or self.runtime.guard.key != self.scope_key):
            raise ValueError('Original guarded transport required for cleanup')
        self.cleanup_used.add(host)
        previous = self.session.cleanup_mode
        self.session.cleanup_mode = True
        try:
            receipt = self.session.call(host, sealed_program(self.seals[host], cleanup=True), 45)
            if receipt != {'owned_configuration_removed': True}:
                raise ValueError('Exact owned configuration cleanup acknowledgement required')
            self.runtime._write('configuration-cleanup', {'host': host, 'seal_sha256': policy.digest(self.seals[host])})
            return receipt
        except BaseException:
            self.failed = True
            raise
        finally:
            self.session.cleanup_mode = previous
