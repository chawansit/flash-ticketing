"""Primary-only CCE bootstrap: actual host evidence, exact budgets, no retired-host access."""
import copy
import json
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from types import SimpleNamespace

import cce_fulfillment_batch_profile as fulfillment
import cce_primary_bootstrap as topology
import pytest
import qualify_two_host_deployment as driver
from cce_committed_status_contract import CommittedStatusContract
from run_status_refresh_comparison import restoration_complete
from test_cce_ecs_transition import area as transition_area  # noqa: F401 - pytest fixture
from test_cce_event_lane_correction import inventory as worker_inventory
from test_cce_paid_lifecycle import area as lifecycle_area  # noqa: F401 - pytest fixture
from test_two_host_scaling_preparation import inventory as host_inventory


def config():
    return {topology.MARKER:topology.VALUE,"primary":{"repo":"/root/app","private_ipv4":"10.0.0.1"},
            "generator":{"repo":"/root/load","private_ipv4":"10.0.0.3"}}


@lru_cache(maxsize=1)
def prepared_inventory():
    contract=topology.configure(CommittedStatusContract(arm="candidate",identity=fulfillment.pins),config())
    data=host_inventory("control")
    data["arm"]="candidate"
    data["captured_at"]=datetime.now(UTC).isoformat()
    data["hosts"].pop("secondary")
    workers=worker_inventory()
    data["background"]=copy.deepcopy(contract.background)
    data["worker_sources"]=workers["worker_sources"]
    sources=fulfillment.image()["runtime_source_sha256"]
    for row in data["worker_sources"]:
        row["image_id"]=fulfillment.pins.IMAGE_ID
        row["settings"]=contract.settings(row["role"])
        row["source_identity"]["resolved_imports"]={k:{"sha256":v,"code_matches_source":True} for k,v in sources.items()}
    data["status_refresh_contract"]=contract.inventory_marker()
    data["primary_bootstrap_api_settings"]=dict(contract.api_settings)
    data["cce_primary_bootstrap_contract"]=topology.marker(contract.api_settings)
    for api in data["apis"]:
        api["image_id"]=contract.images["api"]
        api["settings"]=dict(contract.api_settings)
        api["ORDER_STATUS_EVENT_REFRESH"]="0"
        api["ORDER_STATUS_EVENT_REFRESH_DEDUP"]="0"
    return data,contract


def inventory():
    data,contract=prepared_inventory()
    return copy.deepcopy(data),contract


def test_session_opens_only_the_two_real_hosts(monkeypatch,tmp_path):
    opened=[]
    def connect(self,role):
        opened.append(role)
        return SimpleNamespace(close=lambda:None)
    monkeypatch.setattr(driver.Session,"_open",connect)
    session=driver.Session(config(),tmp_path,"unused-test-password")
    assert opened==["primary","generator"] and "secondary" not in session.clients
    assert topology.restoration_verified(session.state)
    for operation in (lambda:session.call("secondary","print(1)"),lambda:session.put("secondary","/tmp/test","test")):
        with pytest.raises(ValueError,match="forbidden"):
            operation()
    assert session.state["secondary_access_attempts"]==2
    assert not topology.restoration_verified(session.state)
    session.close()


def test_legacy_sessions_still_open_three_distinct_roles(monkeypatch,tmp_path):
    opened=[]
    monkeypatch.setattr(driver.Session,"_open",lambda self,role:(opened.append(role) or SimpleNamespace(close=lambda:None)))
    old=config();old.pop(topology.MARKER);old["secondary"]={"private_ipv4":"10.0.0.2"}
    session=driver.Session(old,tmp_path,"unused-test-password")
    assert opened==["primary","secondary","generator"]
    session.close()


@pytest.mark.parametrize("fault",["mixed","marker","missing"])
def test_malformed_primary_only_config_cannot_connect(fault):
    value=config()
    if fault=="mixed":value["secondary"]={}
    elif fault=="marker":value[topology.MARKER]="primary-only"
    else:value.pop("generator")
    with pytest.raises(ValueError):topology.connection_roles(value)


def test_real_inventory_preserves_all_source_and_database_gates():
    from observe_slot_paid_pipeline import correction_inventory
    from prepare_two_host_scaling import validate_inventory
    data,contract=inventory()
    retained=copy.deepcopy(data)
    view=validate_inventory(data,image_id=contract.images["api"],contract=contract)
    assert view["api_counts"]=={"primary":4,"secondary":0} and view["inventory_contract_pass"]
    assert topology.validate_observer_inventory(data,topology.digest(data),image_id=contract.images["api"])
    actual,sealed=correction_inventory(data,topology.digest(data))
    assert actual==retained==data and sealed==topology.digest(data)
    assert "secondary" not in actual["hosts"]


@pytest.mark.parametrize("fault",["host","source","image","flags","pool","duplicate","stale","authority","secondary_route","missing","seal"])
def test_retired_host_or_incomplete_identity_never_qualifies(fault):
    data,contract=inventory()
    if fault=="host":data["hosts"]["secondary"]=copy.deepcopy(data["hosts"]["primary"])
    elif fault=="source":data["worker_sources"][0]["source_identity"]["source_hashes_match"]=False
    elif fault=="image":data["worker_sources"][0]["image_id"]="sha256:"+"0"*64
    elif fault=="flags":data["apis"][0]["settings"]["API_PAYMENT_POOL_MAX"]="9"
    elif fault=="pool":data["pgbouncer"]["server_pool"]=25
    elif fault=="duplicate":data["apis"][1]["container_id"]=data["apis"][0]["container_id"]
    elif fault=="stale":data["captured_at"]=(datetime.now(UTC)-timedelta(seconds=400)).isoformat()
    elif fault=="authority":data["apis"][0]["auth_secrets_sha256"]="0"*64
    elif fault=="secondary_route":data["apis"][0]["host_role"]="secondary"
    elif fault=="missing":data["worker_sources"].pop()
    with pytest.raises(ValueError):
        topology.validate_observer_inventory(data,"0"*64 if fault=="seal" else topology.digest(data),image_id=contract.images["api"])


def test_primary_cpu_window_is_observed_and_secondary_is_not_invented():
    start=datetime.now(UTC);end=start+timedelta(seconds=300)
    primary={"host_role":"primary","placement":"cce-worker-isolation","arm":"candidate","decision":"ADR0259","inventory_sha256":"a"*64,"pass":True,
             "start_utc":start.isoformat(),"end_utc":end.isoformat(),"host_cpu_percent":12.0}
    value=topology.cpu_window(primary,offered_start_utc=start.isoformat(),offered_end_utc=end.isoformat())
    assert value["observed_host_roles"]==["primary"] and value["retired_secondary_cpu"]=="not_applicable"
    assert "both_hosts_cpu_observed" not in value and "secondary" not in value["host_cpu_percent"]
    primary["end_utc"]=(end-timedelta(seconds=2)).isoformat()
    with pytest.raises(ValueError):topology.cpu_window(primary,offered_start_utc=start.isoformat(),offered_end_utc=end.isoformat())


def test_retirement_restoration_still_requires_every_real_cleanup_gate():
    value={topology.MARKER:topology.VALUE,"connected_host_roles":["primary","generator"],
           "secondary_access_attempts":0,"retired_secondary_cleanup":"not_applicable",
           "restore_pass":True,"primary_runtime_semantics_restored":True,"generator_idle_after":True,
           "credential_snapshots_removed":True,"restored_global_queues":{"pass":True}}
    assert restoration_complete(value) and "secondary_resources_removed" not in value
    value["generator_idle_after"]=False
    assert not restoration_complete(value)


def test_binding_allows_only_removing_the_declared_host(tmp_path,monkeypatch):
    monkeypatch.setattr(topology,"ROOT",tmp_path)
    (tmp_path/"tmp").mkdir()
    old=config();old.pop(topology.MARKER);old["secondary"]={"private_ipv4":"10.0.0.2"}
    for name,value in (("old",old),("new",config())):
        (tmp_path/"tmp"/(name+".json")).write_text(json.dumps(value))
    monkeypatch.setattr(topology,"contract",lambda:{"decision":"ADR0330"})
    g={"primary_bootstrap_decision":"ADR0330","primary_bootstrap_contract_sha256":topology.digest(topology.contract()),
       "primary_bootstrap_config_path":"tmp/new.json","primary_bootstrap_legacy_config_path":"tmp/old.json"}
    original={"configuration_sha256":topology.digest(old)}
    binding={"configuration_sha256":topology.digest(config()),"cce_primary_bootstrap_contract":topology.contract()}
    assert topology.validate_binding(g,binding,original)
    changed=config();changed["primary"]["private_ipv4"]="10.0.0.9"
    (tmp_path/"tmp/new.json").write_text(json.dumps(changed))
    binding["configuration_sha256"]=topology.digest(changed)
    with pytest.raises(ValueError):topology.validate_binding(g,binding,original)



def test_configuration_preparation_preserves_primary_generator_and_credentials():
    legacy=config();legacy.pop(topology.MARKER);legacy["secondary"]={"host":"retired-host"};legacy["known_hosts"]="/owned/known_hosts"
    before=copy.deepcopy(legacy)
    after=topology.prepare_config(legacy)
    assert legacy==before and "secondary" not in after
    assert after["primary"]==before["primary"] and after["generator"]==before["generator"]
    assert after["known_hosts"]==before["known_hosts"]



def test_real_cce_transition_and_restore_never_contact_secondary(transition_area):  # noqa: F811 - imported pytest fixture
    import cce_ecs_transition as component
    import work_envelope as policy
    from test_cce_ecs_transition import RUN, pod_receipts
    old,rows,route,calls,persisted=transition_area
    cfg=old.session.config
    cfg.pop("secondary");cfg["generator"]={"repo":"/root/load","private_ipv4":"10.0.0.3"};cfg[topology.MARKER]=topology.VALUE
    rows["primary"] += rows.pop("secondary")
    address=cfg["primary"]["private_ipv4"]
    apis=[row for row in rows["primary"] if row["Config"]["Labels"].get("com.docker.compose.service")=="api"]
    routes=[]
    for i,row in enumerate(apis):
        row["NetworkSettings"]["Ports"]["8000/tcp"][0]["HostIp"]=address
        routes.append({"container_id":row["Id"],"host_role":"primary","private_ipv4":address,"port":8101+i})
    old.guard.binding["configuration_sha256"]=policy.digest(cfg)
    route["text"]=component.nginx_config(routes)
    current=component.Transition(old.session,old.guard,RUN,routes,cfg["primary"]["repo"]+"/tmp/"+RUN,persisted.append)
    current.capture()
    current.activate(pod_receipts())
    result=current.restore()
    assert result["candidate_restored"] and set(current.rows)=={"primary"}
    assert all(role!="secondary" for role,_ in calls)


def test_composed_cce_lifecycle_keeps_all_safety_and_cleanup_without_secondary(lifecycle_area):  # noqa: F811 - imported pytest fixture
    value,calls,_fail,_saved=lifecycle_area
    value.session.config=config()
    original=value.session.call
    def call(role,*args):
        assert role=="primary","Retired secondary was contacted"
        return original(role,*args)
    value.session.call=call
    result=value.run(None,None,"global audit")
    assert result["pass"] and result["cleanup_complete"]
    assert all(result["measurement_gates"].values())
    assert calls.index("safety")<calls.index("dispatch")<calls.index("audit")
