"""ADR0368 exact immutable UUID parent overlay; no unrelated checkout source."""
import ast

from reservation_claim_lookup_overlay import replace, sha

PARENT_SOURCE = '2c59942318c306c925e5dba3ae26e59c7af30260b4de4560fed823e73ecd6afe'
MODULES = {'src/ticketing/config.py','src/ticketing/workers.py'}

BATCH = b'''

def claim_due_callbacks(db, limit):
    """Claim only free executor slots; commit fenced leases before HTTP work."""
    if type(limit) is not int or not 1 <= limit <= 16:
        raise ValueError("Callback claim batch requires1..16 free slots")
    token = uuid4()
    with simulator_phase("claim"), db.transaction(pipeline=True) as conn:
        rows = conn.execute("""WITH candidate AS (
            SELECT p.id,o.total,o.currency,clock_timestamp() AS claim_observed_at
            FROM payment_attempts p JOIN orders o ON o.id=p.order_id
            WHERE p.deliveries<p.target_deliveries AND p.due_at<=statement_timestamp()
            AND (p.lease_until IS NULL OR p.lease_until<clock_timestamp())
            ORDER BY p.due_at LIMIT %s FOR UPDATE OF p SKIP LOCKED
            ) UPDATE payment_attempts p
            SET lease_until=clock_timestamp()+interval '15 seconds',lease_token=%s
            FROM candidate c WHERE p.id=c.id
            RETURNING p.*,c.total,c.currency,c.claim_observed_at""", (limit, token)).fetchall()
    return rows, token


def simulate_refill_claim_batch(db, settings, executor, should_run=None, transport=None):
    """Bound claims by free slots; refill without a delivery-group barrier."""
    maximum = settings.simulator_concurrency
    if type(maximum) is not int or not 1 <= maximum <= 16:
        raise ValueError("Batch claim preserves at most16 independent delivery slots")
    should_run = should_run or (lambda: running)
    pending = set()
    next_poll = 0.0

    def complete(future):
        try:
            return future.result()
        except Exception:
            WORKER_ERRORS.labels("simulator").inc()
            log.exception("callback_dispatch_failed")
            return False

    try:
        while should_run():
            free = maximum - len(pending)
            now = time.monotonic()
            if free and now >= next_poll:
                try:
                    rows, token = claim_due_callbacks(db, free)
                except Exception:
                    WORKER_ERRORS.labels("simulator").inc()
                    log.exception("callback_claim_failed")
                    rows = []
                next_poll = time.monotonic() + (.1 if len(rows) < free else 0)
                for row in rows:
                    pending.add(executor.submit(deliver_claimed_callback, db, settings, row, token, transport))
            if not pending:
                time.sleep(max(0, min(.1, next_poll-time.monotonic())))
                continue
            done, _ = wait(pending, timeout=.1, return_when=FIRST_COMPLETED)
            for future in done:
                pending.remove(future)
                complete(future)
    finally:
        for future in as_completed(pending):
            complete(future)
'''


def overlay(parent, expected):
    if len(expected)!=22 or {n:sha(v) for n,v in parent.items()}!=expected:
        raise ValueError('Complete verified immutable UUID parent required')
    result=dict(parent)
    name='src/ticketing/config.py'
    raw=replace(parent[name],b'class Settings:\n',b'class Settings:\n    simulator_batch_claim: bool = os.getenv("SIMULATOR_BATCH_CLAIM", "0") == "1"\n')
    anchor=b'        if self.simulator_dispatch_mode not in {"batch", "refill"}:\n'
    result[name]=replace(raw,anchor,b'        if self.simulator_batch_claim and (self.simulator_dispatch_mode != "refill" or not 1 <= self.simulator_concurrency <= 16):\n            raise ValueError("Batch callback claim requires refill and at most16 delivery slots")\n'+anchor)
    name='src/ticketing/workers.py'
    raw=parent[name]
    begin=raw.index(b'    SIMULATOR_DUE_TO_CLAIM_SECONDS.observe(',raw.index(b'def simulate_one('))
    end=raw.index(b'\n\ndef simulate_batch(',begin)
    body=raw[begin:end]
    raw=raw[:begin]+b'    return deliver_claimed_callback(db, settings, row, token, transport)\n\n\ndef deliver_claimed_callback(db, settings, row, token, transport=None):\n'+body+BATCH+raw[end:]
    anchor=b'    """Keep bounded slots occupied independently; drain outstanding work on stop."""\n'
    result[name]=replace(raw,anchor,anchor+b'    if getattr(settings, "simulator_batch_claim", False):\n        return simulate_refill_claim_batch(db, settings, executor, should_run, transport)\n')
    if {n for n in parent if result[n]!=parent[n]}!=MODULES:
        raise ValueError('Exactly reviewed worker/config modules may change')
    for n,v in result.items():ast.parse(v,filename=n)
    return result
