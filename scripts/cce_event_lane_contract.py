"""ADR0266 preserve the proven lifecycle while isolating event-group progress."""
import copy
import json
import time

import cce_event_lane_identity as pins
import cce_event_lane_profile as profile
from cce_shared_worker_contract import SharedWorkerContract

PROJECTION_AUDIT = r"""
# A six-member fulfillment group is the measured split topology. Normal restored
# mixed mode has one member; its retired projection group is not an active lane.
result['event_lane_mode'] = 'split' if result['kafka_members']==6 else 'normal'
if result['kafka_members']==6:
 observer=python_observer(os.environ.get('KAFKA_BOOTSTRAP','kafka:9092'),'ticketing.events')
 try:projection=observer.sample('ticketing-seat-projection-v1')
 finally:observer.close()
 result.update(projection_kafka_total_lag=projection['total_lag'],projection_kafka_members=projection['members'])
 result['pass']=result['pass'] and projection['total_lag']==0 and projection['members']==1 and projection['assigned_partitions']==6
"""


class EventLaneContract(SharedWorkerContract):
    def __init__(self):
        super().__init__(dispatch_slots=16)
        self.shared = profile.image()
        self.background = copy.deepcopy(self.background)
        self.background['consumer']['pool_per_replica'] = 7
        self.background['projection-consumer'] = {'replicas': 1, 'pool_per_replica': 6}
        self.roles = (*self.roles, 'projection-consumer')
        self.candidate_counts = {**self.candidate_counts, 'projection-consumer': 1}
        self.changed_services = (*self.changed_services, 'projection-consumer')
        for role in self.background:
            self.images[role] = pins.IMAGE_ID
        self.parents['projection-consumer'] = self.parents['consumer']
        self.global_audit = self.global_audit.replace('print(json.dumps(result))', PROJECTION_AUDIT + '\nprint(json.dumps(result))')

    def settings(self, role):
        values = super().settings(role)
        values['RESERVATION_WRITE_PIPELINE'] = '1' if role == 'reservation-writer' else '0'
        values['EVENT_CONSUMER_SEPARATION'] = '1' if role in {'consumer', 'projection-consumer'} else '0'
        if role in {'consumer', 'projection-consumer'}:
            values['DB_POOL_MAX'] = '7' if role == 'consumer' else '6'
        return values

    def primary_model(self, model):
        model = copy.deepcopy(model)
        projection = copy.deepcopy(model['services']['consumer'])
        projection['command'] = ['python', '-m', 'ticketing.workers', 'projection-consumer']
        projection.pop('profiles', None)
        projection.pop('ports', None)
        model['services']['projection-consumer'] = projection
        return super().primary_model(model)

    def inventory_background(self, groups, background):
        background = super().inventory_background(groups, background)
        from two_host_topology import environment
        background['projection-consumer']['pool_per_replica'] = int(environment(groups['projection-consumer'][0])['DB_POOL_MAX'])
        return background

    def image_program(self, roles):
        # Parent's proof uses its historical pin. Substitute only that pin after
        # the new immutable receipt was independently checked during construction.
        from cce_shared_worker_comparison import SOURCE
        return super().image_program(roles).replace(repr(SOURCE), repr(pins.SOURCE))

    def inventory_marker(self):
        return {**super().inventory_marker(), 'event_lane_decision': 'ADR0266',
                'shared_image_decision': 'ADR0267', 'shared_image_source_sha256': pins.SOURCE,
                'shared_image_configuration_digest': pins.CONFIG,
                'shared_image_receipt_sha256': pins.RECEIPT_SHA256, 'event_consumer_pool_budget': 48}

    def verify_inventory(self, data):
        from observe_two_host_pipeline import verify_admission_factor_evidence
        from status_refresh_contract import StatusRefreshContract
        StatusRefreshContract.verify_inventory(self, data)
        if data.get("status_refresh_contract") != self.inventory_marker():
            raise ValueError("Event-lane marker differs")
        legacy, _ = pins.historical_view(data, pins.digest(data))
        verify_admission_factor_evidence(legacy)

    def retain_pre_safety_observation(self, session, inventory, queues):
        # Preserve the observed evidence before a contract rejection can discard it.
        path = session.output / "event-lane-readiness.private.json"
        path.write_text(json.dumps({"inventory": inventory, "queues": queues}, indent=2) + "\n", encoding="utf-8")

    def pre_safety(self, session, routes, saved):
        try:
            return super().pre_safety(session, routes, saved)
        except ValueError as exc:
            session.state["event_lane_readiness_rejection"] = str(exc)[:256]
            session.checkpoint()
            raise

    def pre_mutation(self, session, saved):
        # Added role has no normal snapshot service. All existing parent-image,
        # migration and receipt preflights still execute unchanged.
        roles = self.roles
        self.roles = tuple(role for role in roles if role != 'projection-consumer')
        try:
            super().pre_mutation(session, saved)
        finally:
            self.roles = roles

    def before_restore(self, session, cid, live_path):
        super().before_restore(session, cid, live_path)
        deadline = time.monotonic() + 60
        while True:
            queues = session.api(cid, self.global_audit, 45)
            if queues.get("pass") is True:
                session.state["event_lanes_before_worker_removal"] = queues
                session.checkpoint()
                return
            if time.monotonic() >= deadline:
                raise ValueError("Both event lanes must drain before worker removal")
            time.sleep(2)

    def after_restore(self, session, live_path):
        super().after_restore(session, live_path)
        code = "import json,subprocess;subprocess.run(['docker','compose','-f'," + repr(live_path) + ", 'stop','--timeout','30','projection-consumer'],check=True,capture_output=True,timeout=45);subprocess.run(['docker','compose','-f'," + repr(live_path) + ", 'rm','-f','projection-consumer'],check=True,capture_output=True,timeout=30);print(json.dumps({'projection_worker_removed_after_drain':True}))"
        session.state.update(session.call('primary', code, 90))
        session.checkpoint()
