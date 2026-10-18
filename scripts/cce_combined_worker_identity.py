"""ADR0349 immutable identities for hash-sealed transferred observer bundles.

No local authority, file reads, registry access or goal selection belongs here.
"""
PINS = {
    'sha256:34ac0dacef1e7f9676ee327ae5603ee83ebdd5eb7422455270400e4d755ce0bb':
        'a96fbf6593bb805be6c7c250bc24bc56ec25791c3e11803d59c676db69d1b709',
    'sha256:4efa10e301666db5aebc58a3a218df8987307c310c55936ba1c4e3b13de921b2':
        '0087ce3078532a8fd05b668c4d286988a3165d95295c51ebbf2c60d4a702ce0d',
    'sha256:b114f6b15e4e07f11da19e98a82a21d910c39879b7aadf1bc96e8e11a74595ba':
        '19da6fb554d1e592e3eba59a52427b10510c213ab62c2a80181b8d5d27123925',
}


ALLOCATION_COUNTS = {'consumer': 5, 'projection-consumer': 1, 'reservation-writer': 5,
                     'publisher': 1, 'maintenance': 1, 'reconciler': 1, 'simulator': 1}
ALLOCATION_CPU = {'simulator': '500m', 'maintenance': '125m', 'reconciler': '125m'}
ALLOCATION_MANIFEST = 'sha256:b114f6b15e4e07f11da19e98a82a21d910c39879b7aadf1bc96e8e11a74595ba'


def allocation_specification(base, decision="ADR0357"):
    if decision not in {"ADR0357", "ADR0359"}:
        raise ValueError("Exact fixed allocation decision required")
    original = {**ALLOCATION_COUNTS, 'consumer': 6, 'reservation-writer': 4}
    uniform = {kind: {'cpu': '250m', 'memory': '512Mi'} for kind in ('requests', 'limits')}
    if base['counts'] != original or base['resources'] != uniform:
        raise ValueError('Exact historical worker allocation required')
    counts = ALLOCATION_COUNTS if decision == "ADR0357" else original
    resources = {role: {kind: {'cpu': ALLOCATION_CPU.get(role, '250m'), 'memory': '512Mi'}
                        for kind in ('requests', 'limits')} for role in counts}
    if (sum(counts.values()) != 15
            or sum(int(resources[r]['limits']['cpu'][:-1])*n for r,n in counts.items()) != 3750):
        raise ValueError('Fixed aggregate worker budget changed')
    return {**base, 'decision': decision, 'counts': dict(counts), 'resources': resources}


def specification(value):
    from cce_worker_identity import specification as local_specification
    manifest = value.get('manifest_digest')
    decision = value.get('decision')
    if decision not in {'ADR0345', 'ADR0357', 'ADR0359'} or manifest not in PINS:
        raise ValueError('Exact reviewed transferred combined worker identity required')
    if (manifest == 'sha256:34ac0dacef1e7f9676ee327ae5603ee83ebdd5eb7422455270400e4d755ce0bb'
            and decision != 'ADR0359'):
        raise ValueError('Lookup/claim image requires exact six-consumer allocation')
    base = local_specification('ADR0324')
    if decision in {'ADR0357', 'ADR0359'}:
        allowed = {ALLOCATION_MANIFEST}
        if decision == 'ADR0359':
            allowed.add('sha256:34ac0dacef1e7f9676ee327ae5603ee83ebdd5eb7422455270400e4d755ce0bb')
        if manifest not in allowed:
            raise ValueError('Allocated workers require exact combined candidate image')
        base = allocation_specification(base, decision)
    return {**base, 'decision': decision,
            'manifest': manifest, 'image_id': manifest, 'source': PINS[manifest]}
