"""Synthetic development gateway timings; these are not provider measurements."""

import hashlib
import json
import random

RANGES = {"initiation": (.05, .15), "callback_network": (.02, .1)}


def sample_delay(profile, phase):
    if profile not in {"bank-like", "none"}:
        raise ValueError("Unknown simulator latency profile")
    lower, upper = RANGES[phase]
    return random.uniform(lower, upper) if profile == "bank-like" else 0.0


def confirmation_delay(profile, actor, order_id, key, explicit=None):
    if profile not in {"bank-like", "none"}:
        raise ValueError("Unknown simulator latency profile")
    if explicit is not None:
        return explicit
    if profile == "none":
        return 1
    # Stable financial request fingerprint on repeated calls with the same key.
    identity = json.dumps([str(actor), str(order_id), str(key)], separators=(",", ":"))
    value = int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8], "big")
    return 1.0 + 2.0 * value / ((1 << 64) - 1)
