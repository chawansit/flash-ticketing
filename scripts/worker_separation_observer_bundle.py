"""ADR0198 exact source closure for staging the worker observer, with no cloud actions."""
import ast
import hashlib
import re
from pathlib import Path

from worker_separation_inventory import digest

ROOT = Path(__file__).resolve().parents[1]
ENTRY = 'observe_worker_pipeline.py'
MAX_FILES = 160
MAX_SOURCE_BYTES = 1024 * 1024
MAX_TOTAL_BYTES = 8 * 1024 * 1024


def prepare(directory=None):
    """Collect script dependencies explicitly so an old image cannot supply stale helpers."""
    directory = Path(directory or ROOT / 'scripts')
    pending, files = [ENTRY], {}
    while pending:
        name = pending.pop()
        if name in files:continue
        if not re.fullmatch(r'[a-z][a-z0-9_]*\.py', name):
            raise ValueError('Exact script basename required')
        path = directory / name
        if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= MAX_SOURCE_BYTES:
            raise ValueError('Bounded regular source file required')
        source = path.read_text(encoding='utf-8').replace('\r\n', '\n')
        tree = ast.parse(source, filename=name)
        dependencies = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                dependencies.update(alias.name.split('.')[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
                dependencies.add(node.module.split('.')[0])
        for module in dependencies:
            dependency = directory / (module + '.py')
            if dependency.exists() or dependency.is_symlink():pending.append(dependency.name)
        files[name] = source
        if len(files) > MAX_FILES or sum(len(value.encode()) for value in files.values()) > MAX_TOTAL_BYTES:
            raise ValueError('Observer source closure exceeds bound')
    manifest = {name: {'sha256': hashlib.sha256(source.encode()).hexdigest(), 'bytes': len(source.encode())}
                for name, source in sorted(files.items())}
    return {'decision': 'ADR0198', 'entry': ENTRY, 'files': dict(sorted(files.items())),
            'manifest': manifest, 'manifest_sha256': digest(manifest)}


def validate(value, directory=None, *, check_closure=True):
    if (not isinstance(value, dict) or set(value) != {'decision', 'entry', 'files', 'manifest', 'manifest_sha256'}
            or value['decision'] != 'ADR0198' or value['entry'] != ENTRY
            or not isinstance(value['files'], dict) or not 1 <= len(value['files']) <= MAX_FILES
            or ENTRY not in value['files'] or set(value['files']) != set(value['manifest'])
            or digest(value['manifest']) != value['manifest_sha256']):
        raise ValueError('Exact observer bundle manifest required')
    total = 0
    for name, source in value['files'].items():
        if not re.fullmatch(r'[a-z][a-z0-9_]*\.py', name) or not isinstance(source, str):
            raise ValueError('Exact text script source required')
        raw = source.encode()
        if not 0 < len(raw) <= MAX_SOURCE_BYTES or b'\r\n' in raw:
            raise ValueError('Bounded normalized observer source required')
        if value['manifest'][name] != {'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}:
            raise ValueError('Observer source differs from sealed manifest')
        total += len(raw)
    if total > MAX_TOTAL_BYTES:raise ValueError('Observer source closure exceeds byte bound')
    if not check_closure:return value['manifest_sha256']
    # Resolve local script imports against the preparation directory, not an image fallback.
    directory = Path(directory or ROOT / 'scripts')
    visited, pending = set(), [ENTRY]
    while pending:
        name = pending.pop()
        if name in visited:continue
        if name not in value['files']:raise ValueError('Local observer dependency omitted from bundle')
        visited.add(name)
        for node in ast.walk(ast.parse(value['files'][name])):
            modules = []
            if isinstance(node, ast.Import):modules = [alias.name.split('.')[0] for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module and not node.level:modules = [node.module.split('.')[0]]
            for module in modules:
                dependency = directory / (module + '.py')
                if dependency.exists() or dependency.is_symlink():pending.append(dependency.name)
    if visited != set(value['files']):raise ValueError('Unexpected source outside observer closure')
    return value['manifest_sha256']


def verification_program(value, directory):
    """Read-only remote proof for every declared file, before any observer launch."""
    validate(value)
    if not isinstance(directory, str) or not directory.startswith('/tmp/') or '..' in directory.split('/'):
        raise ValueError('Owned absolute observer directory required')
    expected = value['manifest']
    return ("import hashlib,json,os,stat;from pathlib import Path;p=Path(" + repr(directory) + ")\n"
            "assert not p.is_symlink() and p.is_dir() and stat.S_IMODE(p.stat().st_mode)==0o700 and p.stat().st_uid==os.geteuid()\n"
            "expected=" + repr(expected) + "\n"
            "for name,identity in expected.items():\n"
            " f=p/name;assert not f.is_symlink() and f.is_file()\n"
            " m=f.stat();assert m.st_nlink==1 and stat.S_IMODE(m.st_mode)==0o600 and m.st_uid==os.geteuid()\n"
            " raw=f.read_bytes();assert {'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}==identity\n"
            "print(json.dumps({'observer_sources_verified':True,'manifest_sha256':" + repr(value['manifest_sha256']) + "}))")
