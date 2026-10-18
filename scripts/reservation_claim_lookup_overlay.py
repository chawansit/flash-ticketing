"""ADR0362 exact three-module patch over the verified combined image."""
import ast
import hashlib

PARENT_SOURCE = '19da6fb554d1e592e3eba59a52427b10510c213ab62c2a80181b8d5d27123925'
MODULES = {'src/ticketing/config.py', 'src/ticketing/workers.py',
           'src/ticketing/infrastructure/reservations.py'}

CLAIM = b'''def reservation_command_replay_claim(conn, payload):
    row = conn.execute(
        """WITH previous_command AS MATERIALIZED (
            SELECT request_hash,response FROM reservation_commands WHERE command_id=%s
        ), claimed AS (
            INSERT INTO idempotency_records(actor,operation,key,request_hash)
            SELECT %s,%s,%s,%s
            WHERE NOT EXISTS (SELECT 1 FROM previous_command)
            ON CONFLICT DO NOTHING RETURNING key
        )
        SELECT EXISTS(SELECT 1 FROM previous_command) AS previous_found,
               (SELECT request_hash FROM previous_command) AS previous_request_hash,
               (SELECT response FROM previous_command) AS previous_response,
               EXISTS(SELECT 1 FROM claimed) AS idempotency_claimed""",
        (payload["command_id"], payload["actor"], "hold",
         payload["idempotency_key"], payload["request_hash"]),
    ).fetchone()
    if row["previous_found"]:
        return {"request_hash": row["previous_request_hash"], "response": row["previous_response"]}, None
    if row["idempotency_claimed"]:
        return None, None
    # A concurrent conflict may commit after this statement's snapshot. Read next.
    return None, idempotency_replay(conn, payload["actor"], "hold",
                                    payload["idempotency_key"], payload["request_hash"])


'''


def sha(raw):
    return hashlib.sha256(raw.replace(b'\r\n', b'\n')).hexdigest()


def replace(raw, before, after):
    if raw.count(before) != 1:
        raise ValueError('Exact immutable-parent insertion point required')
    return raw.replace(before, after)


def overlay(parent, expected):
    if {name:sha(raw) for name,raw in parent.items()} != expected or len(expected) != 22:
        raise ValueError('Complete verified combined parent required')
    result = dict(parent)
    name = 'src/ticketing/config.py'
    old = b'    reservation_write_pipeline: bool ='
    result[name] = replace(parent[name], old,
        b'    reservation_lookup_claim: bool = os.getenv("RESERVATION_LOOKUP_CLAIM", "0") == "1"\n' + old)
    name = 'src/ticketing/workers.py'
    old = b'        persist_write_pipeline=role == "reservation-writer" and settings.reservation_write_pipeline,\n'
    result[name] = replace(parent[name], old, old +
        b'        persist_lookup_claim=role == "reservation-writer" and settings.reservation_lookup_claim,\n')
    name = 'src/ticketing/infrastructure/reservations.py'
    raw = replace(parent[name], b'*, persist_write_pipeline=False):',
                  b'*, persist_write_pipeline=False, persist_lookup_claim=False):')
    old = b'        self.persist_write_pipeline = persist_write_pipeline\n'
    raw = replace(raw, old, old + b'        if type(persist_lookup_claim) is not bool:\n            raise TypeError("Reservation lookup/claim selection must be boolean")\n        self.persist_lookup_claim = persist_lookup_claim\n')
    old = b'    if row:\n        return None\n    previous = conn.execute('
    raw = replace(raw, old, b'    if row:\n        return None\n    return idempotency_replay(conn, actor, operation, key, hashed)\n\n\ndef idempotency_replay(conn, actor, operation, key, hashed):\n    previous = conn.execute(')
    raw = replace(raw, b'def remember(', CLAIM + b'def remember(')
    old = b'''        previous_command = conn.execute(
            "SELECT request_hash,response FROM reservation_commands WHERE command_id=%s",
            (payload["command_id"],),
        ).fetchone()
        if previous_command:
            if previous_command["request_hash"] != payload["request_hash"]:
                raise Failure("RESERVATION_COMMAND_MISMATCH", 409)
            return previous_command["response"]

        replay = idem(
            conn,
            payload["actor"],
            "hold",
            payload["idempotency_key"],
            request,
        )
'''
    new = b'''        if self.persist_lookup_claim:
            previous_command, replay = reservation_command_replay_claim(conn, payload)
        else:
            previous_command = conn.execute(
                "SELECT request_hash,response FROM reservation_commands WHERE command_id=%s",
                (payload["command_id"],),
            ).fetchone()
            replay = None
        if previous_command:
            if previous_command["request_hash"] != payload["request_hash"]:
                raise Failure("RESERVATION_COMMAND_MISMATCH", 409)
            return previous_command["response"]

        if not self.persist_lookup_claim:
            replay = idem(
                conn, payload["actor"], "hold", payload["idempotency_key"], request,
            )
'''
    result[name] = replace(raw, old, new)
    if {name for name in result if result[name] != parent[name]} != MODULES:
        raise ValueError('Exactly three reviewed modules must change')
    for name,raw in result.items():
        ast.parse(raw, filename=name)
    return result
