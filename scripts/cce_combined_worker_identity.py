"""ADR0349 immutable identities for hash-sealed transferred observer bundles.

No local authority, file reads, registry access or goal selection belongs here.
"""
PINS = {
    'sha256:4efa10e301666db5aebc58a3a218df8987307c310c55936ba1c4e3b13de921b2':
        '0087ce3078532a8fd05b668c4d286988a3165d95295c51ebbf2c60d4a702ce0d',
    'sha256:b114f6b15e4e07f11da19e98a82a21d910c39879b7aadf1bc96e8e11a74595ba':
        '19da6fb554d1e592e3eba59a52427b10510c213ab62c2a80181b8d5d27123925',
}


def specification(value):
    from cce_worker_identity import specification as local_specification
    manifest = value.get('manifest_digest')
    if value.get('decision') != 'ADR0345' or manifest not in PINS:
        raise ValueError('Exact reviewed transferred combined worker identity required')
    return {**local_specification('ADR0324'), 'decision': 'ADR0345',
            'manifest': manifest, 'image_id': manifest, 'source': PINS[manifest]}
