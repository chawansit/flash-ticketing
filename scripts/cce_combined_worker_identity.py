"""ADR0349 immutable identities for hash-sealed transferred observer bundles.

No local authority, file reads, registry access or goal selection belongs here.
"""
PINS = {
    'sha256:4efa10e301666db5aebc58a3a218df8987307c310c55936ba1c4e3b13de921b2':
        '0087ce3078532a8fd05b668c4d286988a3165d95295c51ebbf2c60d4a702ce0d',
    'sha256:b114f6b15e4e07f11da19e98a82a21d910c39879b7aadf1bc96e8e11a74595ba':
        '19da6fb554d1e592e3eba59a52427b10510c213ab62c2a80181b8d5d27123925',
}


ALLOCATION_COUNTS = {'consumer': 5, 'projection-consumer': 1, 'reservation-writer': 5,
                     'publisher': 1, 'maintenance': 1, 'reconciler': 1, 'simulator': 1}
ALLOCATION_CPU = {'simulator': '500m', 'maintenance': '125m', 'reconciler': '125m'}
ALLOCATION_MANIFEST = 'sha256:b114f6b15e4e07f11da19e98a82a21d910c39879b7aadf1bc96e8e11a74595ba'


def allocation_specification(base):
    original = {**ALLOCATION_COUNTS, 'consumer': 6, 'reservation-writer': 4}
    uniform = {kind: {'cpu': '250m', 'memory': '512Mi'} for kind in ('requests', 'limits')}
    if base['counts'] != original or base['resources'] != uniform:
        raise ValueError('Exact historical worker allocation required')
    resources = {role: {kind: {'cpu': ALLOCATION_CPU.get(role, '250m'), 'memory': '512Mi'}
                        for kind in ('requests', 'limits')} for role in ALLOCATION_COUNTS}
    if (sum(ALLOCATION_COUNTS.values()) != 15
            or sum(int(resources[r]['limits']['cpu'][:-1])*n for r,n in ALLOCATION_COUNTS.items()) != 3750):
        raise ValueError('Fixed aggregate worker budget changed')
    return {**base, 'decision': 'ADR0357', 'counts': dict(ALLOCATION_COUNTS), 'resources': resources}


def specification(value):
    from cce_worker_identity import specification as local_specification
    manifest = value.get('manifest_digest')
    decision = value.get('decision')
    if decision not in {'ADR0345', 'ADR0357'} or manifest not in PINS:
        raise ValueError('Exact reviewed transferred combined worker identity required')
    base = local_specification('ADR0324')
    if decision == 'ADR0357':
        if manifest != ALLOCATION_MANIFEST:
            raise ValueError('Allocated workers require exact combined candidate image')
        base = allocation_specification(base)
    return {**base, 'decision': decision,
            'manifest': manifest, 'image_id': manifest, 'source': PINS[manifest]}
