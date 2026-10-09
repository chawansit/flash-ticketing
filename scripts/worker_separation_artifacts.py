"""ADR0198 sealed owner-only paid artifacts; no recursive or inferred cleanup."""
import copy
import re
from pathlib import PurePosixPath

IDENTITY_KEYS = {'device', 'inode', 'uid', 'mode'}


def validate_seal(value, directory):
    path = PurePosixPath(directory)
    if (not path.is_absolute() or '..' in path.parts or str(path) != directory
            or path.name not in {'control', 'candidate'}
            or not isinstance(value, dict) or set(value) != {'fresh_arm_directory', 'root', 'directory'}
            or value['fresh_arm_directory'] is not True):
        raise ValueError('Exact fresh owned artifact directory seal required')
    for key in ('root', 'directory'):
        row = value[key]
        if (not isinstance(row, dict) or set(row) != IDENTITY_KEYS
                or any(type(row[k]) is not int or row[k] < 0 for k in IDENTITY_KEYS)
                or row['inode'] < 1 or row['mode'] != 0o700):
            raise ValueError('Exact owner-only directory identity required')
    if value['root']['uid'] != value['directory']['uid']:
        raise ValueError('Same owned root and arm required')
    return copy.deepcopy(value)


POSIX = r'''import json,os,stat
from pathlib import PurePosixPath

def identity(s):
 return {'device':s.st_dev,'inode':s.st_ino,'uid':s.st_uid,'mode':stat.S_IMODE(s.st_mode)}

def walk(path):
 fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY)
 try:
  for part in PurePosixPath(path).parts[1:]:
   other=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd)
   os.close(fd);fd=other
  return fd
 except BaseException:
  os.close(fd);raise

def owned(fd):
 s=os.fstat(fd)
 if s.st_uid!=os.geteuid() or stat.S_IMODE(s.st_mode)!=0o700:raise ValueError('Owner-only artifact directory required')
 return identity(s)
'''


def directories_program(root, arm):
    path = PurePosixPath(root)
    if (not path.is_absolute() or '..' in path.parts or str(path) != root
            or arm not in {'control', 'candidate'}):
        raise ValueError('Canonical owned artifact root and exact arm required')
    return POSIX + '\nroot=' + repr(root) + '\narm=' + repr(arm) + r'''
p=PurePosixPath(root);parent=walk(str(p.parent))
try:
 try:os.mkdir(p.name,0o700,dir_fd=parent)
 except FileExistsError:pass
 fd=os.open(p.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent)
 try:
  root_identity=owned(fd)
  if os.listdir(fd):raise ValueError('Fresh empty artifact root required')
  os.mkdir(arm,0o700,dir_fd=fd)
  child=os.open(arm,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd)
  try:directory_identity=owned(child)
  finally:os.close(child)
  if identity(os.stat(p.name,dir_fd=parent,follow_symlinks=False))!=root_identity:raise ValueError('Artifact root replaced')
  os.fsync(fd);os.fsync(parent)
 finally:os.close(fd)
finally:os.close(parent)
print(json.dumps({'fresh_arm_directory':True,'root':root_identity,'directory':directory_identity}))
'''


def cleanup_program(directory, seal, filenames, *, remove=True):
    if type(remove) is not bool:raise ValueError('Explicit artifact cleanup mode required')
    seal = validate_seal(seal, directory)
    if (not isinstance(filenames, (set, list, tuple)) or not 0 < len(filenames) <= 250
            or len(set(filenames)) != len(filenames)
            or any(not isinstance(n, str) or not re.fullmatch(r'[a-zA-Z0-9_.-]+', n)
                   or n in {'.', '..'} for n in filenames)):
        raise ValueError('Explicit bounded artifact file set required')
    return (POSIX + '\ndirectory=' + repr(directory) + '\nseal=' + repr(seal)
            + '\nremove=' + repr(remove) + '\nallowed=' + repr(sorted(filenames)) + r'''
p=PurePosixPath(directory);parent=walk(str(p.parent.parent))
try:
 root=os.open(p.parent.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent)
 try:
  if owned(root)!=seal['root'] or set(os.listdir(root))!={p.name}:raise ValueError('Sealed artifact root changed')
  fd=os.open(p.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=root)
  try:
   if owned(fd)!=seal['directory']:raise ValueError('Sealed artifact arm replaced')
   names=set(os.listdir(fd))
   if not names<=set(allowed):raise ValueError('Unexpected artifact: retain for recovery')
   before={}
   for name in names:
    s=os.stat(name,dir_fd=fd,follow_symlinks=False)
    if not stat.S_ISREG(s.st_mode) or s.st_nlink!=1 or s.st_uid!=os.geteuid() or stat.S_IMODE(s.st_mode)!=0o600 or s.st_size>16777216:raise ValueError('Unsafe artifact: retain for recovery')
    before[name]=(identity(s),s.st_size,s.st_mtime_ns,s.st_ctime_ns)
   if identity(os.stat(p.name,dir_fd=root,follow_symlinks=False))!=seal['directory'] or identity(os.stat(p.parent.name,dir_fd=parent,follow_symlinks=False))!=seal['root']:raise ValueError('Artifact directory changed before removal')
   for name in sorted(names) if remove else ():
    s=os.stat(name,dir_fd=fd,follow_symlinks=False)
    if (identity(s),s.st_size,s.st_mtime_ns,s.st_ctime_ns)!=before[name]:raise ValueError('Artifact changed before removal')
    os.unlink(name,dir_fd=fd)
   if remove and os.listdir(fd):raise ValueError('Artifact cleanup incomplete')
   os.fsync(fd)
  finally:os.close(fd)
  if remove:os.rmdir(p.name,dir_fd=root);os.fsync(root)
 finally:os.close(root)
 if remove:os.rmdir(p.parent.name,dir_fd=parent);os.fsync(parent)
finally:os.close(parent)
print(json.dumps({'owned_artifacts_removed' if remove else 'owned_artifacts_verified':True}))
''')
