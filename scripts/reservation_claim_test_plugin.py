"""Local qualification only: run retained integration cases with the new writer path."""
import os

import pytest


@pytest.fixture(autouse=True)
def qualified_claim_default(monkeypatch):
    if os.getenv("QUALIFY_RESERVATION_LOOKUP_CLAIM") != "1":
        return
    from ticketing.infrastructure.reservations import PostgresReservations
    original = PostgresReservations.__init__

    def initialize(self, *args, **kwargs):
        kwargs.setdefault("persist_lookup_claim", True)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(PostgresReservations, "__init__", initialize)
