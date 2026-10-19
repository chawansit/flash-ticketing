"""ADR0364 opt-in durable identifier locality over the verified projection image."""
import ast

from reservation_claim_lookup_overlay import replace, sha

PARENT_SOURCE = '72fabf5016df66fd73cfdb0faf05e4c1f80fa41868afa044c266609ffed665f6'
MODULES = {'src/ticketing/config.py', 'src/ticketing/domain.py', 'src/ticketing/workers.py',
           'src/ticketing/infrastructure/reservations.py',
           'src/ticketing/infrastructure/redis_reservations.py'}
FACTORY = b'''

def persistent_uuid():
    """Opaque durable IDs; timestamps never drive authorization, sale clocks or TTL."""
    if os.getenv("PERSISTENT_UUID_V7", "0") != "1":
        return uuid4()
    timestamp = time_ns() // 1_000_000
    if not 0 <= timestamp < (1 << 48):
        raise RuntimeError("Persistent UUID timestamp outside48-bit Unix milliseconds")
    random = randbits(74)
    return UUID(int=(timestamp << 80) | (7 << 76) | ((random >> 62) << 64)
                | (2 << 62) | (random & ((1 << 62) - 1)))
'''


def overlay(parent, expected):
    if len(expected) != 22 or {n:sha(v) for n,v in parent.items()} != expected:
        raise ValueError('Complete verified immutable projection parent required')
    result = dict(parent)
    n = 'src/ticketing/config.py'
    result[n] = replace(parent[n],b'class Settings:\n',b'class Settings:\n    persistent_uuid_v7: bool = os.getenv("PERSISTENT_UUID_V7", "0") == "1"\n')
    n = 'src/ticketing/domain.py'
    result[n] = replace(parent[n],b'from dataclasses import dataclass\n',
                        b'import os\nfrom dataclasses import dataclass\nfrom secrets import randbits\nfrom time import time_ns\nfrom uuid import UUID, uuid4\n') + FACTORY
    for n in ('src/ticketing/infrastructure/reservations.py','src/ticketing/infrastructure/redis_reservations.py'):
        raw = replace(parent[n],b'from uuid import uuid4\n',b'from ticketing.domain import persistent_uuid as uuid4\n')
        result[n] = raw
    n = 'src/ticketing/workers.py'
    raw = replace(parent[n],b'from ticketing.config import Settings\n',
                  b'from ticketing.config import Settings\nfrom ticketing.domain import persistent_uuid\n')
    result[n] = replace(raw,b'(uuid4(), booking["id"]),',b'(persistent_uuid(), booking["id"]),')
    if {n for n in parent if result[n] != parent[n]} != MODULES:
        raise ValueError('Exactly five reviewed runtime modules may change')
    for n,v in result.items():
        ast.parse(v,filename=n)
    return result
