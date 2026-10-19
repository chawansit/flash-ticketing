"""ADR0370 instrumentation correction over the frozen ADR0368 candidate overlay."""
import ast

from reservation_claim_lookup_overlay import replace
from simulator_claim_batch_overlay import MODULES, PARENT_SOURCE
from simulator_claim_batch_overlay import overlay as base_overlay

__all__ = ['MODULES','PARENT_SOURCE','overlay']

PROGRESS = b'''

@measured_work("simulate_one")
def simulate_claimed_one(db, settings, row, token, transport=None):
    """Preserve observed delivery progress without double-counting legacy work."""
    return deliver_claimed_callback(db, settings, row, token, transport)
'''


def overlay(parent,expected):
    result=base_overlay(parent,expected)
    name='src/ticketing/workers.py'
    result[name]=replace(result[name],b'\ndef claim_due_callbacks(db, limit):',
                         PROGRESS+b'\n\ndef claim_due_callbacks(db, limit):')
    result[name]=replace(result[name],b'executor.submit(deliver_claimed_callback, db, settings, row, token, transport)',
                         b'executor.submit(simulate_claimed_one, db, settings, row, token, transport)')
    ast.parse(result[name],filename=name)
    return result
