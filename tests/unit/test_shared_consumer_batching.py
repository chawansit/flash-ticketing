import ast
from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from cce_historical_sources import blob
from prepare_shared_application_image import CONSUMER_SOURCE, WRITER_SOURCES, restore_consumer

WORKERS = "src/ticketing/workers.py"


def corrected():
    original = {WORKERS: blob(WRITER_SOURCES[WORKERS]), "other.py": b"untouched\n"}
    return original, restore_consumer(original)


def test_restores_only_accepted_consumer_functions_and_keeps_writer():
    before, after = corrected()
    assert after["other.py"] == before["other.py"]
    def functions(raw):
        return {n.name: ast.dump(n) for n in ast.parse(raw).body if isinstance(n, ast.FunctionDef)}
    old, new = functions(before[WORKERS]), functions(after[WORKERS])
    assert {k for k in old if old[k] != new[k]} == {"consume_events", "consume_refresh_batch"}
    accepted = functions(blob(CONSUMER_SOURCE))
    for name in ("consume_events", "consume_refresh_batch"):
        assert new[name] == accepted[name]
    assert b'persist_write_pipeline=role == "reservation-writer" and settings.reservation_write_pipeline' in after[WORKERS]


def test_unknown_worker_source_is_rejected():
    with pytest.raises(ValueError, match="Exact shared writer"):
        restore_consumer({WORKERS: b"pass"})


@pytest.mark.parametrize("reverse", [False, True])
def test_mixed_business_events_coalesce_refreshes_lock_stably_and_replay_once(reverse):
    _, after = corrected()
    tree = ast.parse(after[WORKERS])
    selected = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in {"consume_events", "consume_refresh_batch"}]
    namespace = {"measured_work": lambda _: lambda fn: fn, "consumer_phase": lambda _: nullcontext()}
    actions, inserted = [], set()
    class Connection:
        def execute(self, query, params):
            assert "ON CONFLICT DO NOTHING" in query
            ids = []
            for value in params[0]:
                if value not in inserted:
                    ids.append({"event_id": value}); inserted.add(value)
            actions.append(("refresh_transaction", len(params[0])))
            return SimpleNamespace(fetchall=lambda: ids)
    db = SimpleNamespace(transaction=lambda: nullcontext(Connection()))
    namespace["request_refresh"] = lambda conn, show, seats: actions.append(("refresh", show, seats))
    namespace["consume_event"] = lambda db, cache, envelope, **kw: actions.append(("business", envelope["event_type"]))
    exec(compile(ast.Module(body=selected, type_ignores=[]), "verified-consumer-methods", "exec"), namespace)  # noqa: S102 - Execute only verified repository-owned methods.
    def seat(identity, show, seats):
        return {"event_id": identity, "schema_version": 1, "event_type": "SeatsChanged", "payload": {"event_id": show, "seats": seats}}
    refreshes = [seat("one", "B", [2]), seat("two", "A", [1])]
    if reverse: refreshes.reverse()
    messages = [refreshes[0], {"event_type": "OrderPaid"}, refreshes[1], {"event_type": "TicketsIssued"}, seat("three", "B", [3]), refreshes[0]]
    namespace["consume_events"](db, None, messages)
    assert actions == [("business", "OrderPaid"), ("business", "TicketsIssued"), ("refresh_transaction", 4), ("refresh", "A", [1]), ("refresh", "B", [2, 3])]
    actions.clear()
    namespace["consume_refresh_batch"](db, [refreshes[0], refreshes[1], seat("three", "B", [3])])
    assert actions == [("refresh_transaction", 3)]  # Replayed refreshes produce no publication or lock work.
