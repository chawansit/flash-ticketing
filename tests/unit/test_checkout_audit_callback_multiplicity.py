"""Audit delivery targets match the declared payment workload."""

from contextlib import nullcontext

from scripts.audit_checkout_smoke import audit


class FakeConnection:
    def __init__(self, callbacks):
        self.values = (
            2,
            2,
            0,
            0,
            2,
            0,
            2,
            2,
            2,
            2,
            0,
            callbacks,
            callbacks,
            0,
            0,
            0,
            0,
        )

    def transaction(self):
        return nullcontext()

    def execute(self, statement, params=None):
        if statement.startswith("SELECT"):
            return self
        return None

    def fetchone(self):
        return self.values


def test_one_callback_per_paid_order_passes_only_one_callback_profile():
    conn = FakeConnection(callbacks=2)
    assert audit(conn, ["fixture"], 2, callback_duplicates=1)["pass"]
    assert not audit(conn, ["fixture"], 2, callback_duplicates=3)["pass"]


def test_duplicate_callback_profile_still_requires_all_deliveries():
    conn = FakeConnection(callbacks=6)
    result = audit(conn, ["fixture"], 2, callback_duplicates=3)
    assert result["pass"]
    assert result["expected_callback_deliveries_per_payment"] == 3
