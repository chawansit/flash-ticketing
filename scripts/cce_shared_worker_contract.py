"""ADR0259 shared measured workers; retain frozen, unmeasured ECS bootstrap APIs."""
import copy

import cce_shared_worker_comparison as comparison
import cce_simulator_dispatch_profile as parent
from runtime_source_identity import source_identity_program


class SharedWorkerContract(parent.SimulatorDispatchContract):
    def __init__(self):
        data = parent.baseline.plan()
        super().__init__(data["artifact_receipt"], "candidate", parent.legacy_contract()["api_sources"])
        self.shared = comparison.selected_receipt()
        for role in self.background:
            self.images[role] = self.shared["local_image_id"]

    def source_map(self, role):
        if role in self.background:
            return copy.deepcopy(self.shared["runtime_source_sha256"])
        return super().source_map(role)

    def image_manifest(self, role):
        if role in self.background:
            return self.shared["source_identity_sha256"]
        return super().image_manifest(role)

    def image_program(self, roles):
        if not set(roles) <= set(self.roles):
            raise ValueError("Unknown artifact role")
        if not any(role in self.background for role in roles):
            return super().image_program(roles)
        # Source/import/bytecode proof is performed in the exact cached image,
        # without credentials, networking, application writes or host mounts.
        program = source_identity_program(self.shared["runtime_source_sha256"])
        bootstrap = (super().image_program(("api",)).replace("print(json.dumps({'artifact_roles_verified':list(values)}))", "bootstrap_verified=True") if "api" in roles else "")
        return "image=" + repr(self.shared["local_image_id"]) + "\nproof_program=" + repr(program) + "\n" + r"""
import json,subprocess
row=json.loads(subprocess.check_output(['docker','image','inspect',image],text=True,timeout=10))[0]
labels=row['Config'].get('Labels',{})
if row['Id']!=image or labels.get('org.flash-ticketing.decision')!=DECISION or labels.get('org.flash-ticketing.source-sha256')!=SOURCE:
 raise ValueError('Shared image identity/decision/source label differs')
if row.get('Os')!='linux' or row.get('Architecture')!='amd64' or row['Config'].get('User')!='ticketing':
 raise ValueError('Shared image platform/user differs')
proof=json.loads(subprocess.check_output(['docker','run','--rm','--pull','never','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--entrypoint','python',image,'-c',proof_program],text=True,stderr=subprocess.PIPE,timeout=60))
if proof.get('source_hashes_match') is not True:raise ValueError('Shared imported sources differ')
""".replace("SOURCE", repr(comparison.SOURCE)).replace("DECISION", repr(self.shared["decision"])) + bootstrap + "\nprint(json.dumps({'shared_worker_image_verified':True}))\n"

    def inventory_marker(self):
        return {**super().inventory_marker(), "shared_image_decision": "ADR0259",
                "shared_image_source_sha256": comparison.SOURCE,
                "shared_image_configuration_digest": self.shared["registry_configuration_digest"],
                "shared_image_receipt_sha256": comparison.digest(self.shared)}


def worker_contract():
    return SharedWorkerContract()
