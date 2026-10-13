"""Native admission is renewed only after current identity/background checks."""
import copy
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"scripts"))
import cce_inventory_renewal as renewal


def material():
    now=datetime.now(UTC)
    inventory={"captured_at":(now-timedelta(seconds=600)).isoformat(),"hosts":{"primary":"old"},
               "diagnostic_connection_binding":{"bundle_sha256":"same"}}
    native={"run":"adr0151-123456abcdef","receipts":[{"pod_uid":str(i)} for i in range(4)]}
    workers={"run":native["run"],"receipts":[{"pod_uid":str(i)} for i in range(14)]}
    return inventory,native,workers,now


def arguments():
    inventory,native,workers,now=material()
    return inventory, {"run": native["run"], "binding": {"cce_inventory_renewal_contract": renewal.contract()},
        "native_before": native, "workers_before": workers, "observe_native": lambda: native,
        "observe_workers": lambda: workers, "verify_background": lambda: None, "now": now}


def test_original_retained_and_current_receipts_bound():
    inventory,args=arguments()
    original=copy.deepcopy(inventory)
    value,evidence=renewal.renew(inventory,**args)
    assert inventory==original
    assert value["captured_at"]==args["now"].isoformat()
    assert value["diagnostic_connection_binding"]==original["diagnostic_connection_binding"]
    assert evidence["original_captured_at_utc"]==original["captured_at"]
    assert evidence["background_unchanged"]


@pytest.mark.parametrize("kind",["native","workers","background"])
def test_failed_live_check_never_renews(kind):
    inventory,args=arguments()
    original=copy.deepcopy(inventory)
    def failed():
        raise ValueError("changed live identity")
    args[{"native":"observe_native","workers":"observe_workers","background":"verify_background"}[kind]]=failed
    with pytest.raises(ValueError):
        renewal.renew(inventory,**args)
    assert inventory==original


@pytest.mark.parametrize("kind",["native","workers"])
def test_changed_uid_bundle_rejected(kind):
    inventory,args=arguments()
    previous=args[kind+"_before"] if kind=="native" else args["workers_before"]
    changed=copy.deepcopy(previous);changed["receipts"][0]["pod_uid"]="different"
    args["observe_"+kind]=lambda:changed
    with pytest.raises(ValueError,match="identities changed"):
        renewal.renew(inventory,**args)


def test_mismatched_scope_or_missing_members_rejected():
    inventory,args=arguments()
    args["workers_before"]["receipts"].pop()
    with pytest.raises(ValueError,match="Complete"):
        renewal.renew(inventory,**args)


def test_future_original_timestamp_rejected():
    inventory,args=arguments()
    inventory["captured_at"]=(args["now"]+timedelta(seconds=1)).isoformat()
    with pytest.raises(ValueError,match="chronology"):
        renewal.renew(inventory,**args)


def test_unbound_or_changed_helper_rejected():
    inventory,args=arguments()
    args["binding"]={}
    with pytest.raises(ValueError,match="Reserved"):
        renewal.renew(inventory,**args)
