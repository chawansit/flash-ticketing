"""ADR0363 exact single-function patch over the clean combined parent."""
import ast
from reservation_claim_lookup_overlay import PARENT_SOURCE, sha

MODULES = {'src/ticketing/workers.py'}


def overlay(parent, expected):
    if len(expected) != 22 or {n:sha(v) for n,v in parent.items()} != expected:
        raise ValueError('Complete verified immutable parent required')
    name = 'src/ticketing/workers.py'
    raw = parent[name]
    module = ast.parse(raw)
    node = next(n for n in module.body if isinstance(n,ast.FunctionDef) and n.name=='consume_refresh_batch')
    lines = raw.splitlines(keepends=True)
    body = b''.join(lines[node.lineno-1:node.end_lineno])
    old = b'    with db.transaction() as conn:\n'
    if body.count(old) != 1:
        raise ValueError('Exact projection refresh transaction required')
    changed = body.replace(old,b'    with db.transaction(pipeline=True) as conn:\n')
    result = {**parent,name:b''.join(lines[:node.lineno-1])+changed+b''.join(lines[node.end_lineno:])}
    for n,v in result.items():
        ast.parse(v,filename=n)
    if {n for n in parent if result[n]!=parent[n]} != MODULES:
        raise ValueError('Only projection refresh transaction may change')
    return result
