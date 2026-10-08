"""Delayed callback cleanup must not create fake capacity exhaustion."""
import asyncio
import sys
from pathlib import Path
from types import ModuleType

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import prepare_generator_completion as patch
from run_two_host_paid_comparison import frozen_bundle
from test_paid_ticket_load_generator import FakeClient, args, fulfilled, manifest


@pytest.fixture(scope="module")
def frozen():
    return frozen_bundle()


def module(raw):
    result = ModuleType("qualified_generator")
    exec(compile(raw, patch.PATH, "exec"), result.__dict__)  # noqa: S102 - execute only locally verified frozen test source
    return result


def delay_cleanup(monkeypatch):
    create = asyncio.create_task
    class DelayedCallbacks(asyncio.Task):
        def add_done_callback(self, callback, *, context=None):
            return super().add_done_callback(
                lambda task: self.get_loop().call_later(0.35, callback, task), context=context)
    def delayed(coro, **kwargs):
        if coro.cr_code.co_name == "one":
            return DelayedCallbacks(coro, **kwargs)
        return create(coro, **kwargs)
    monkeypatch.setattr(asyncio, "create_task", delayed)


def test_delayed_callbacks_reproduce_parent_drops_and_corrected_dispatch(monkeypatch, tmp_path, frozen):
    delay_cleanup(monkeypatch)
    monkeypatch.setattr(__import__("httpx"), "AsyncClient", FakeClient)
    async def quick(_client, _manifest, index, *_):
        await asyncio.sleep(0.005)
        return fulfilled(index)
    candidate = args(tmp_path, rate=10, concurrency=1)
    candidate.lifecycle_diagnostics = True
    before = asyncio.run(module(frozen[patch.PATH]).scheduled_journeys(candidate, manifest(), quick))
    after = asyncio.run(module(patch.corrected_source(frozen[patch.PATH])).scheduled_journeys(candidate, manifest(), quick))
    assert before["generator_drops"] > 0
    assert any(d["completed_tasks_still_counted"] for d in before["lifecycle"]["drop_snapshots"])
    assert after["scheduled"] == after["dispatched"] == after["completed"] == after["distinct_tickets"] == 10
    assert after["generator_drops"] == 0 and after["pass"]
    assert after["active_journeys_peak"] == 1
    assert 0 <= after["active_journeys_time_weighted_mean"] <= 1
    assert after["http_max_connections"] == before["http_max_connections"] == 1


def test_true_saturation_still_drops_with_delayed_cleanup(monkeypatch, tmp_path, frozen):
    delay_cleanup(monkeypatch)
    monkeypatch.setattr(__import__("httpx"), "AsyncClient", FakeClient)
    active = peak = 0
    async def slow(_client, _manifest, index, *_):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.4)
        active -= 1
        return fulfilled(index)
    candidate = args(tmp_path, rate=10, concurrency=1)
    candidate.lifecycle_diagnostics = True
    result = asyncio.run(module(patch.corrected_source(frozen[patch.PATH])).scheduled_journeys(candidate, manifest(), slow))
    assert result["generator_drops"] > 0 and not result["pass"]
    assert result["dispatched"] + result["generator_drops"] == result["scheduled"] == 10
    assert result["completed"] == result["dispatched"] == result["distinct_orders"] == result["distinct_tickets"]
    assert result["active_journeys_peak"] == peak == 1
    assert all(d["completed_tasks_still_counted"] == 0 for d in result["lifecycle"]["drop_snapshots"])


def test_exact_frozen_bundle_changes_only_generator(frozen):
    candidate = patch.corrected_bundle(frozen)
    assert set(candidate) == set(frozen)
    assert {p for p in candidate if candidate[p] != frozen[p]} == {patch.PATH}
    assert patch.sha(candidate[patch.PATH]) == "dd84d979bfd993c2833bb7c3cfbb01366774f17fdf591e98cb9cdfa1fecbb7f1"


def test_wrong_parent_and_patch_block_before_execution(monkeypatch, tmp_path, frozen):
    with pytest.raises(ValueError): patch.corrected_source(frozen[patch.PATH] + b"\n")
    monkeypatch.setattr(patch, "ARTIFACTS", tmp_path)
    for name in ["manifest.json", "adr0226.patch"]:
        source = Path(__file__).resolve().parents[2] / "artifacts/generator-completion" / name
        (tmp_path / name).write_bytes(source.read_bytes())
    (tmp_path / "adr0226.patch").write_bytes(b"drift")
    with pytest.raises(ValueError): patch.corrected_bundle(frozen)
