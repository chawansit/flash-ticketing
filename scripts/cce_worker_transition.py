"""ADR0259 stop-before-start native workers and verified restoration."""
import copy

import cce_api_adapter as cce
import cce_native_workers as workers
import cce_shared_worker_comparison as comparison
import cce_transaction_profile as profile
import cce_worker_identity as identity
import work_envelope as policy
from cce_ecs_transition import Transition, execution_identity
from qualify_two_host_deployment import INSPECT


def change_program(rows, verb):
    if verb not in {"stop", "start"} or len(rows) != 13:
        raise ValueError("Complete explicit owned worker transition required")
    expected = {r["Id"]: execution_identity(r) for r in rows}
    if len(expected) != 13:
        raise ValueError("Distinct captured worker identities required")
    return "expected=" + repr(expected) + "\nverb=" + repr(verb) + "\n" + r"""
import json,subprocess
ids=list(expected)
rows=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True,timeout=10))
if len(rows)!=13 or {r['Id'] for r in rows}!=set(ids):raise ValueError('Worker identity missing')
for row in rows:
 if {k:row[k] for k in expected[row['Id']]}!=expected[row['Id']]:raise ValueError('Captured worker specification changed')
 if row['Config']['Labels'].get('com.docker.compose.project')!='flash-ticketing' or row['Config']['Labels'].get('com.docker.compose.service') not in {'consumer','reservation-writer','publisher','maintenance','reconciler','simulator'}:raise ValueError('Unknown worker ownership')
subprocess.run(['docker',verb]+(['--time','30'] if verb=='stop' else [])+ids,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=90)
after=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True,timeout=10))
if {r['Id'] for r in after}!=set(ids) or any({k:r[k] for k in expected[r['Id']]}!=expected[r['Id']] or r['State']['Running']!=(verb=='start') for r in after):raise ValueError('Worker transition unverified')
print(json.dumps({'owned_worker_transition_verified':True,'running':verb=='start'}))
"""


class WorkerTransition(Transition):
    def __init__(self, *args, deployment, service, read_metrics, comparison_persist, **kwargs):
        super().__init__(*args, **kwargs)
        self.deployment, self.service, self.read_metrics = deployment, service, read_metrics
        self.comparison_persist = comparison_persist
        self.worker_rows = []
        self.worker_runtime = None
        self.worker_stop_attempted = False
        self.goal = profile.active()
        self.arm = self.goal["comparison_arm"]

    def capture(self):
        super().capture()
        rows = self.session.call("primary", INSPECT, 45)
        grouped = {role: [] for role in identity.COUNTS}
        for row in rows:
            role = row["Config"].get("Labels", {}).get("com.docker.compose.service")
            if role in grouped:
                grouped[role].append(row)
        envs = {}
        for role, values in grouped.items():
            if len(values) != identity.COUNTS[role] or any(not r["State"]["Running"] or r["Image"] != identity.ECS_IMAGE_ID for r in values):
                raise ValueError("Complete running shared-image ECS workers required")
            environments = [dict(v.split("=", 1) for v in r["Config"]["Env"]) for r in values]
            if any(env != environments[0] for env in environments):
                raise ValueError("Replica worker environments differ")
            envs[role] = environments[0]
            self.worker_rows.extend(values)
        ip = self.session.config["primary"]["private_ipv4"]
        hashes = {role: policy.digest(workers.environment(role, values, ip)) for role, values in envs.items()}
        hashes["api"] = policy.digest(cce.api_environment(self.service, ip, acquisition_budget=20))
        binding = comparison.arm_binding(self.arm, hashes)
        if self.arm == "candidate":
            _, report = profile.control_receipt(self.goal)
            comparison.match_bindings(report["comparison_binding"], binding)
        self.comparison_persist(binding)
        self.record["captured_worker_execution_sha256"] = policy.digest([execution_identity(r) for r in self.worker_rows])
        self.checkpoint()
        if self.arm == "candidate":
            self.worker_runtime = workers.Workers(self.deployment, workers.objects(self.run, envs, ip), self.read_metrics,
                                                  lambda value: self.persist({"workers": copy.deepcopy(value)}))

    def activate(self, receipts):
        super().activate(receipts)
        if self.worker_runtime is None:
            return
        self.worker_stop_attempted = True
        self.record["worker_stop_attempted"] = True
        self.checkpoint()
        proof = self.session.call("primary", change_program(self.worker_rows, "stop"), 120)
        if proof != {"owned_worker_transition_verified": True, "running": False}:
            raise ValueError("ECS workers not verified stopped before CCE activation")
        self.worker_runtime.create()

    def verify_workers(self):
        if self.worker_runtime is not None:
            self.worker_runtime.observe(previous=self.worker_runtime.bundle)
        return True

    def restore(self):
        self.session.begin_cleanup()
        # A missing deletion acknowledgement never permits duplicate workers.
        if self.worker_stop_attempted:
            if self.worker_runtime.attempted:
                removed = self.deployment.cleanup()
                if removed.get("namespace_removed") is not True:
                    raise ValueError("Native worker absence not proven; refusing overlapping ECS restart")
            proof = self.session.call("primary", change_program(self.worker_rows, "start"), 120)
            if proof != {"owned_worker_transition_verified": True, "running": True}:
                raise ValueError("Captured ECS worker restoration unverified")
            self.record["captured_workers_restored"] = True
            self.checkpoint()
        return super().restore()
