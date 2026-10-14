"""ADR0323 default-off adapter wiring and unchanged connection budgets."""
import pytest

from ticketing.config import Settings
from ticketing.infrastructure import postgres as module


@pytest.mark.parametrize("explicit", [False, True])
def test_both_api_roles_select_one_driver_mode_without_changing_budget(monkeypatch, explicit):
    captured = []
    class Pool:
        def __init__(self, url, **kwargs): captured.append(kwargs)
        def close(self): pass
    monkeypatch.setattr(module, "ConnectionPool", Pool)
    general, payment = module.create_api_databases("postgresql://test", 4, 150, 20, 2, explicit_start=explicit)
    assert general is not payment
    assert [p["max_size"] for p in captured] == [2, 2]
    assert all(p["kwargs"]["autocommit"] is explicit for p in captured)
    assert all(p["timeout"] == .15 for p in captured)
    general.close(); payment.close()


@pytest.mark.parametrize("explicit", [1, "1", None])
def test_driver_mode_rejects_non_boolean_selection(explicit):
    with pytest.raises(TypeError): module.Postgres("postgresql://test", explicit_start=explicit)


def test_setting_remains_default_off():
    assert Settings().db_explicit_transaction_start is False
