from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import contextmanager
from multiprocessing import get_context
from uuid import uuid4

import pytest
from psycopg.errors import LockNotAvailable

from ticketing.application.reservations import Reservations
from ticketing.domain import Failure
from ticketing.infrastructure.postgres import Postgres
from ticketing.infrastructure.reservations import PostgresReservations

pytestmark = pytest.mark.integration


class NoShield:
    @contextmanager
    def shield(self, *_):
        yield


def process_batch(args):
    url, event_id = args
    db = Postgres(url, maximum=4)
    db.pool.wait(timeout=10)
    service = Reservations(PostgresReservations(db, NoShield()))
    try:

        def attempt(_):
            try:
                service.reserve(str(uuid4()), event_id, ["A"], str(uuid4()))
                return 1
            except (Failure, LockNotAvailable):
                return 0

        with ThreadPoolExecutor(max_workers=4) as threads:
            return sum(threads.map(attempt, range(25)))
    finally:
        db.close()


def test_one_owner_across_four_processes(system):
    _svc, db, event_id = system
    with ProcessPoolExecutor(max_workers=4, mp_context=get_context("spawn")) as processes:
        wins = list(processes.map(process_batch, [(db.pool.conninfo, event_id)] * 4))
    assert sum(wins) == 1
