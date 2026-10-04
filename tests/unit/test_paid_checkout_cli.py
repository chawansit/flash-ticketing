"""Exercise paid-stage validation without accessing cloud services."""

import runpy
import sys
from pathlib import Path

import pytest


class ReachedTransport(RuntimeError):
    pass


@pytest.mark.parametrize("cache_ms", [0, 1000, 3000])
@pytest.mark.parametrize("shards", [1, 2])
@pytest.mark.parametrize("delivery_slots", [8, 12])
def test_paid_diagnostics_accept_supported_layout_before_transport(monkeypatch, tmp_path, shards, delivery_slots, cache_ms, payment_pool=0):
    scripts = Path(__file__).resolve().parents[2] / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    import unattended_capacity_stage

    def transport_boundary(*args, **kwargs):
        raise ReachedTransport("Validated configuration reached transport")

    monkeypatch.setattr(unattended_capacity_stage, "Transport", transport_boundary)
    copied = tmp_path / "scripts" / "run_huawei_checkout_smoke.py"
    copied.parent.mkdir()
    copied.write_text((scripts / copied.name).read_text(encoding="utf-8"), encoding="utf-8")
    (tmp_path / "tmp").mkdir()
    monkeypatch.setattr(sys, "argv", [str(copied), "--backend-host", "root@example.invalid",
        "--generator-host", "root@generator.invalid", "--backend-dir", "/isolated/backend",
        "--generator-dir", "/isolated/generator", "--origin", "http://192.0.2.1:8000",
        "--identity-file", str(tmp_path / "absent-key"), "--admission-candidate", "4",
        "--admission-rollback", "4", "--paid-rate", "60", "--paid-seconds", "300",
        "--paid-concurrency", "500", "--paid-generator-shards", str(shards),
        "--paid-http-client-count", str(16 // shards), "--shows", "60", "--viewers", "18000",
        "--paid-lifecycle-diagnostics", "--simulator-concurrency-candidate", str(delivery_slots),
        "--simulator-dispatch-mode-candidate", "refill",
        "--order-status-cache-ms-candidate", str(cache_ms),
        "--api-payment-pool-max-candidate", str(payment_pool),
        "--api-pool-shared-waiting-candidate", "1" if payment_pool else "0",
        "--api-pool-per-instance-candidate", "4", "--api-pool-waiters-candidate", "12"])
    with pytest.raises(ReachedTransport):
        runpy.run_path(str(copied), run_name="__main__")


def test_nonpaid_diagnostics_rejected_before_transport(monkeypatch, tmp_path):
    scripts = Path(__file__).resolve().parents[2] / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    monkeypatch.setattr(sys, "argv", ["checkout", "--backend-host", "root@example.invalid",
        "--generator-host", "root@generator.invalid", "--backend-dir", "/isolated/backend",
        "--generator-dir", "/isolated/generator", "--origin", "http://192.0.2.1:8000",
        "--identity-file", str(tmp_path / "absent-key"), "--admission-candidate", "4",
        "--admission-rollback", "4", "--paid-lifecycle-diagnostics"])
    with pytest.raises(SystemExit) as rejected:
        runpy.run_path(str(scripts / "run_huawei_checkout_smoke.py"), run_name="__main__")
    assert rejected.value.code == 2

@pytest.mark.parametrize("cache_ms", [-1, 500, 1001, 3001])
def test_unapproved_cache_age_rejected_before_transport(monkeypatch, cache_ms):
    scripts = Path(__file__).resolve().parents[2] / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    import unattended_capacity_stage

    def forbidden_transport(*args, **kwargs):
        pytest.fail("Rejected cache age reached cloud transport")

    monkeypatch.setattr(unattended_capacity_stage, "Transport", forbidden_transport)
    monkeypatch.setattr(sys, "argv", ["checkout", "--backend-host", "root@example.invalid",
        "--generator-host", "root@generator.invalid", "--backend-dir", "/isolated/backend",
        "--generator-dir", "/isolated/generator", "--origin", "http://192.0.2.1:8000",
        "--identity-file", "/absent-key", "--admission-candidate", "4",
        "--admission-rollback", "4", "--order-status-cache-ms-candidate", str(cache_ms)])
    with pytest.raises(SystemExit) as rejected:
        runpy.run_path(str(scripts / "run_huawei_checkout_smoke.py"), run_name="__main__")
    assert rejected.value.code == 2


def test_payment_partition_candidate_reaches_existing_bounded_transport(monkeypatch, tmp_path):
    test_paid_diagnostics_accept_supported_layout_before_transport(monkeypatch,tmp_path,2,8,0,payment_pool=2)


@pytest.mark.parametrize('allocation',[1,3,-1])
def test_unqualified_payment_allocation_rejected_before_transport(monkeypatch,allocation):
    scripts=Path(__file__).resolve().parents[2]/'scripts'
    monkeypatch.syspath_prepend(str(scripts))
    import unattended_capacity_stage
    def forbidden_transport(*args,**kwargs):
        pytest.fail('Rejected allocation reached cloud transport')
    monkeypatch.setattr(unattended_capacity_stage,'Transport',forbidden_transport)
    monkeypatch.setattr(sys,'argv',['checkout','--backend-host','root@example.invalid',
        '--generator-host','root@generator.invalid','--backend-dir','/isolated/backend',
        '--generator-dir','/isolated/generator','--origin','http://192.0.2.1:8000',
        '--identity-file','/absent-key','--admission-candidate','4','--admission-rollback','4',
        '--api-payment-pool-max-candidate',str(allocation)])
    with pytest.raises(SystemExit) as rejected:
        runpy.run_path(str(scripts/'run_huawei_checkout_smoke.py'),run_name='__main__')
    assert rejected.value.code==2

@pytest.mark.parametrize("payment,shared,total,waiting",[(0,1,4,12),(2,0,4,12),(2,1,3,12),(2,1,4,3),(2,2,4,12)])
def test_invalid_shared_acquisition_candidate_rejected_before_transport(monkeypatch,payment,shared,total,waiting):
    scripts=Path(__file__).resolve().parents[2]/"scripts"
    monkeypatch.syspath_prepend(str(scripts))
    import unattended_capacity_stage
    monkeypatch.setattr(unattended_capacity_stage,"Transport",lambda *a,**k:pytest.fail("Rejected shared budget reached cloud transport"))
    monkeypatch.setattr(sys,"argv",["checkout","--backend-host","root@example.invalid",
        "--generator-host","root@generator.invalid","--backend-dir","/isolated/backend",
        "--generator-dir","/isolated/generator","--origin","http://192.0.2.1:8000",
        "--identity-file","/absent-key","--admission-candidate","4","--admission-rollback","4",
        "--api-payment-pool-max-candidate",str(payment),"--api-pool-shared-waiting-candidate",str(shared),
        "--api-pool-per-instance-candidate",str(total),"--api-pool-waiters-candidate",str(waiting)])
    with pytest.raises(SystemExit) as rejected:
        runpy.run_path(str(scripts/"run_huawei_checkout_smoke.py"),run_name="__main__")
    assert rejected.value.code==2
