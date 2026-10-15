"""ADR0330 bounded primary-only bootstrap/safety qualification; no paid capacity stage."""
import argparse
import getpass
import json
import sys
import time
from pathlib import Path
from uuid import uuid4

import cce_fulfillment_batch_identity as pins
import cce_primary_bootstrap as topology
import qualify_two_host_deployment as driver
import work_envelope as policy
from cce_committed_status_contract import CommittedStatusContract
from run_status_refresh_comparison import RunLock, restoration_complete
from run_two_host_paid_comparison import validate_config


class SafetyBudget:
    def __init__(self):
        self.started=time.monotonic()
        self.key="bounded_primary_bootstrap_safety__"+uuid4().hex[:12]
    def check(self,seconds=0):
        if time.monotonic()-self.started+seconds>1800:
            raise TimeoutError("Primary-only safety qualification deadline")


def execute(config, output, *, password):
    if not topology.enabled(config):
        raise ValueError("Explicit primary-only configuration required")
    validate_config(config)
    if output.exists() or output.is_symlink() or not output.resolve().is_relative_to((policy.ROOT/"tmp").resolve()):
        raise ValueError("Fresh owned qualification directory required")
    policy.check_available(policy.envelope(),policy.journal(policy.envelope()),policy.read(policy.STATE))
    output.mkdir(mode=0o700)
    budget=SafetyBudget()
    lock=RunLock(output.name)
    prompt=getpass.getpass
    stages=[]
    try:
        getpass.getpass=lambda _prompt:password
        contract=topology.configure(CommittedStatusContract(arm="candidate",identity=pins),config)
        def safety_stage(_session, arm, routes, *_args):
            if arm not in {"control","candidate"} or len(routes)!=4 or any(r["host_role"]!="primary" for r in routes):
                raise ValueError("Four real primary API endpoints required")
            stages.append(arm)
        result=driver.run(config,output,stage_hook=safety_stage,runtime_policy=contract,action_guard=budget)
        policy.write(output/"bootstrap-safety.private.json",result)
        summary={"decision":"ADR0330","run":output.name,"configuration_sha256":policy.digest(config),
                 "source_contract_sha256":policy.digest(topology.contract()),"bounded_seconds":1800,
                 "no_paid_customer_load":True,"paid_capacity_stages_started":result.get("capacity_stages_started"),
                 "safety_only":"100 concurrent holds and one simulated paid ticket, with replay and post-TTL audit",
                 "bootstrap_api_counts":{"primary":4},"connected_host_roles":result.get("connected_host_roles"),
                 "secondary_access_attempts":result.get("secondary_access_attempts"),
                 "historical_cross_host_safety_executed":False,"native_cce_paid_lifecycle_executed":False,
                 "safety_pass":result.get("safety",{}).get("pass"),
                 "post_ttl_financial_pass":result.get("post_ttl_financial",{}).get("pass"),
                 "queues_drained":result.get("restored_global_queues",{}).get("pass"),
                 "normal_runtime_restored":restoration_complete(result),
                 "qualification_pass":result.get("pass") is True and stages==["control","candidate"]
                    and result.get("capacity_stages_started")==0 and topology.restoration_verified(result)
                    and restoration_complete(result),
                 "original_failure_preserved":True,"capacity_improvement_measured":False,"vm_deleted":False}
        policy.write(output/"qualification.json",summary)
        return summary
    finally:
        getpass.getpass=prompt
        password=None
        lock.release()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config",type=Path,required=True)
    parser.add_argument("--ssh-runtime",type=Path,required=True)
    parser.add_argument("--prepare-from-legacy",type=Path)
    parser.add_argument("--execute",action="store_true")
    args=parser.parse_args()
    if args.prepare_from_legacy:
        if args.config.exists() or args.config.is_symlink():parser.error("Fresh private configuration output required")
        if not args.config.resolve().is_relative_to((policy.ROOT/"tmp").resolve()):parser.error("Private configuration must remain under repository tmp")
        policy.write(args.config,topology.prepare_config(policy.read(args.prepare_from_legacy)))
    config=policy.read(args.config)
    validate_config(config)
    if not topology.enabled(config):parser.error("Primary-only config required")
    if not args.execute:
        print(json.dumps({"decision":"ADR0330","preparation_only":True,"host_roles":topology.connection_roles(config),"paid_load_authorized":False}))
        return
    sys.path.insert(0,str(args.ssh_runtime.resolve()))
    password=bytes.fromhex(getpass.getpass("ECS password hex (hidden): ")).decode()
    try:
        result=execute(config,policy.ROOT/"tmp"/("adr0151-"+uuid4().hex[:12]),password=password)
        print(json.dumps(result),flush=True)
        if not result["qualification_pass"]:raise SystemExit(1)
    finally:password=None


if __name__=="__main__":main()
