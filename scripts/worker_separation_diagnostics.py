"""ADR0198 worker observer setup on the original guarded transport; no load CLI."""
import copy
import hashlib
import json
import re

from diagnostic_runner_connection import FILES, ProtectedContext, cleanup_program, validate_target
from fetch_status_refresh_parents import owner_path
from run_two_host_paid_comparison import api_upload
from stage_status_refresh_images import ROOT
from worker_separation_execution import observation_identity, verify_phase
from worker_separation_inventory import digest
from worker_separation_observer_bundle import validate, verification_program
from worker_separation_snapshot import verify_restored


def preflight_program(inventory, directory):
    """Use the existing diagnostic connection/collector; never an application fallback."""
    binding = inventory['diagnostic_connection_binding']
    return ("import json,os,sys;from pathlib import Path;from types import SimpleNamespace\n"
            "sys.dont_write_bytecode=True;sys.path.insert(0," + repr(directory) + ")\n"
            "import psycopg,observe_worker_pipeline as observer,diagnostic_connection,database_wait_evidence\n"
            "p=Path(" + repr(directory) + ")\n"
            "inventory=json.loads((p/'diagnostic-preflight-inventory.private.json').read_text())\n"
            "spec=observer.diagnostic_spec(inventory,inventory_path=p/'diagnostic-preflight-inventory.private.json',"
            "bundle_path=p/'diagnostic.private.json',approved_sha256=" + repr(digest(inventory)) + ","
            "source_dsn=os.environ['DATABASE_URL'])\n"
            "module=SimpleNamespace(psycopg=psycopg)\n"
            "adapter=diagnostic_connection.install(module,os.environ['DATABASE_URL'],spec)\n"
            "with module.psycopg.connect(os.environ['DATABASE_URL'],autocommit=True) as conn:\n"
            " collector=database_wait_evidence.Collector();records=[collector.collect(conn) for _ in range(2)]\n"
            " assert all(row['complete'] for row in records),'Diagnostic preflight incomplete'\n"
            " assert conn.execute('SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()').fetchone()[0] is True\n"
            "assert adapter.connections_created==1\n"
            "print(json.dumps({'pass':True,'read_only_connection_verified':True,'verified_tls':True,"
            "'one_data_connection':True,'identity_sha256':" + repr(binding['identity_sha256']) + ","
            "'max_collection_ms':max(row['collection_ms'] for row in records)}))")


class DiagnosticActions:
    """Single-use, scope-bound setup. Caller owns stage retention and final restoration."""
    def __init__(self, execution, inventory, sources, context, cid, *, artifact_output, upload=api_upload):
        if not isinstance(context, ProtectedContext) or not context.password:
            raise ValueError('Protected diagnostic context required')
        self.execution, self.session = execution, execution.session
        self.runtime = execution.runtime
        self.target = validate_target(context.target)
        self.sources, self.inventory = copy.deepcopy(sources), copy.deepcopy(inventory)
        validate(self.sources)  # Expensive dependency closure once; recheck sealed bytes on every action.
        self.cid, self.context, self.upload = cid, context, upload
        from pathlib import Path
        artifact_output = Path(artifact_output).absolute()
        if (artifact_output.parent != (ROOT / 'tmp').absolute() or artifact_output.resolve() != artifact_output
                or artifact_output.is_symlink() or artifact_output.name == self.runtime.output.name):
            raise ValueError('Separate canonical observer artifact owner required')
        self.artifact_owner_name = artifact_output.name
        self.owner = owner_path(self.session.config['primary']['repo'], self.artifact_owner_name) + '/' + execution.arm
        self.directory = '/tmp/' + self.artifact_owner_name + '/' + execution.arm
        sealed_owner = execution.configurations.seals['primary']['owner']
        if self.owner == sealed_owner or self.owner.startswith(sealed_owner + '/'):
            raise ValueError('Observer artifacts must not enter sealed configuration directory')
        self.used, self.cleanup_used, self.attempted = False, False, False
        self.writable_layer_verified = False
        self._guard(5)
        if (inventory.get('decision') != 'ADR0184' or inventory.get('arm') != execution.arm
                or inventory.get('prepared_pair_sha256') != digest(execution.pair)
                or len([r for r in inventory['containers'] if r['role']=='api' and r['container_id']==cid
                        and r['host_role']=='primary']) != 1):
            raise ValueError('Exact current worker inventory and primary API required')
        pg = execution.saved['model']['services']['pgbouncer']
        env = pg['environment']
        mounts = [m for m in pg.get('volumes', []) if m['type'] == 'bind'
                  and m['target'] == env.get('SERVER_TLS_CA_FILE') and m.get('read_only') is True]
        original_target = {'host':env.get('DB_HOST'), 'port':int(env.get('DB_PORT', '5432')),
                           'dbname':env.get('DB_NAME'), 'user':env.get('DB_USER')}
        if (env.get('SERVER_TLS_SSLMODE') != 'verify-full'
                or original_target != {k:self.target[k] for k in original_target}
                or len(mounts) != 1 or mounts[0]['source'] != self.target['ca_source_path']
                or execution.saved['bind_sha256'].get(self.target['ca_source_path']) != self.target['ca_sha256']):
            raise ValueError('Original saved CA trust required')

    def _guard(self, timeout, *, cleanup=False):
        self.execution._guard(timeout, cleanup=cleanup)
        binding = self.runtime.guard.binding
        if not cleanup and (not self.context.password or dict(self.context.target) != self.target):
            raise ValueError('Protected diagnostic context changed')
        if (binding.get('worker_observer_owner_name') != self.artifact_owner_name
                or self.owner != owner_path(self.session.config['primary']['repo'], self.artifact_owner_name) + '/' + self.execution.arm
                or self.directory != '/tmp/' + self.artifact_owner_name + '/' + self.execution.arm):
            raise ValueError('Original separate scope-bound observer owner required')
        if (binding.get('worker_observer_manifest_sha256') != validate(self.sources, check_closure=False)
                or binding.get('worker_diagnostic_target_sha256') != digest(self.target)):
            raise ValueError('Exact scope-bound observer sources and diagnostic target required')

    def _stable(self, before, after):
        verify_phase(self.execution.pair, self.execution.saved, self.execution.arm, after, 'workers')
        if any(observation_identity(before[host]) != observation_identity(after[host])
               for host in ('primary', 'secondary')):
            raise ValueError('Runtime changed during diagnostic setup')
        from two_host_topology import fingerprint
        actual = {row['Id']:row for values in after.values() for row in values['rows']}
        if any(self.directory == m['Destination'] or self.directory.startswith(m['Destination'].rstrip('/')+'/')
               for m in actual[self.cid].get('Mounts', [])):
            raise ValueError('Diagnostic artifacts must reside in the container writable layer')
        self.writable_layer_verified = True
        expected = {entry['container_id']:entry for entry in self.inventory['containers']}
        if set(actual) != set(expected) or any(
                actual[cid]['State']['StartedAt'] != entry['started_at']
                or actual[cid]['Image'] != entry['image_id']
                or fingerprint(actual[cid]) != entry['runtime_fingerprint'] for cid, entry in expected.items()):
            raise ValueError('Diagnostic runtime differs from bound inventory')

    def _upload(self, name, value):
        self._guard(45)
        return self.upload(self.session, self.cid, self.owner, self.directory, name, value)

    def prepare(self):
        if self.used:raise ValueError('Diagnostic setup cannot be replayed after ambiguous outcome')
        self.used = True
        self._guard(45)
        before = self.execution._observe()
        self._stable(before, before)
        self.runtime._write('worker-diagnostic-intent', {'container_id': self.cid, 'owner': self.owner,
                                                     'directory': self.directory, 'files': list(FILES),
                                                     'manifest_sha256': self.sources['manifest_sha256']})
        self.attempted = True  # A partial upload still requires both-site credential cleanup.
        for name, content in self.sources['files'].items():self._upload(name, content)
        self._guard(45)
        proof = self.session.api(self.cid, verification_program(self.sources, self.directory), 45)
        if proof != {'observer_sources_verified':True, 'manifest_sha256': self.sources['manifest_sha256']}:
            raise ValueError('Exact transferred observer source proof required')
        self._guard(45)
        transport = self.session.clients['primary'].open_sftp()
        try:
            with transport.open(self.target['ca_source_path'], 'rb') as stream:certificate = stream.read(262145)
        finally:transport.close()
        if (not 0 < len(certificate) <= 262144 or b'BEGIN CERTIFICATE' not in certificate
                or hashlib.sha256(certificate).hexdigest() != self.target['ca_sha256']):
            raise ValueError('Existing trusted certificate binding differs')
        from diagnostic_connection import identity
        parameters = {k: self.target[k] for k in ('host','port','dbname','user')}
        parameters.update(password=self.context.password, sslmode='verify-full',
                          sslrootcert=self.directory + '/diagnostic-ca.pem')
        raw = json.dumps(parameters, sort_keys=True, separators=(',',':'))
        connection = {'decision':'ADR0180','database':self.target['dbname'],
                      'identity_sha256':identity(self.target['user'],self.target['dbname']),
                      'endpoint':[self.target['host'],self.target['port']],
                      'bundle_sha256':hashlib.sha256(raw.encode()).hexdigest(),'ca_sha256':self.target['ca_sha256']}
        self.inventory['diagnostic_connection_binding'] = connection
        self._upload('diagnostic-ca.pem', certificate.decode('ascii'))
        self._upload('diagnostic.private.json', raw)
        self._upload('diagnostic-preflight-inventory.private.json', json.dumps(self.inventory))
        self._guard(45)
        preflight = self.session.api(self.cid, preflight_program(self.inventory, self.directory), 45)
        if (not isinstance(preflight, dict)
                or set(preflight) != {'pass','read_only_connection_verified','verified_tls','one_data_connection',
                                     'identity_sha256','max_collection_ms'}
                or any(preflight[k] is not True for k in ('pass','read_only_connection_verified','verified_tls','one_data_connection'))
                or preflight['identity_sha256'] != connection['identity_sha256']):
            raise ValueError('Exact read-only full-visibility diagnostic preflight required')
        import math

        from database_wait_evidence import MAX_OVERHEAD_MS
        overhead = preflight['max_collection_ms']
        if type(overhead) not in {int,float} or not math.isfinite(overhead) or not 0 <= overhead <= MAX_OVERHEAD_MS:
            raise ValueError('Diagnostic preflight overhead gate failed')
        self._stable(before, self.execution._observe())
        receipt = {'observer_sources_verified':True,'manifest_sha256':self.sources['manifest_sha256'],
                   'inventory_sha256':digest(self.inventory),'diagnostic_preflight':preflight,'runtime_unchanged':True}
        self.runtime._write('worker-diagnostic-ack',receipt)
        return {'inventory':copy.deepcopy(self.inventory), 'receipt':receipt}

    def container_retired(self):
        """Require exact restored observations and successful all-container enumeration."""
        self._guard(45, cleanup=True)
        if self.writable_layer_verified is not True:raise ValueError('Original writable-layer proof required')
        before = self.execution._observe(cleanup=True)
        verify_restored(self.execution.saved,{h:o['rows'] for h,o in before.items()},
                        before['primary']['volumes'],before['primary']['bind_sha256'])
        entry = next(r for r in self.inventory['containers'] if r['container_id']==self.cid)
        present = [r for r in before['primary']['rows'] if r['Id']==self.cid]
        if present:
            from two_host_topology import fingerprint
            row = present[0]
            if (row['State']['StartedAt']!=entry['started_at'] or row['Image']!=entry['image_id']
                    or fingerprint(row)!=entry['runtime_fingerprint']):
                raise ValueError('Present diagnostic container identity changed')
            return False
        if not re.fullmatch(r'[0-9a-f]{64}',self.cid):raise ValueError('Exact diagnostic container ID required')
        code = ("import json,re,subprocess;cid="+repr(self.cid)+"\n"
                "r=subprocess.run(['docker','ps','-aq','--no-trunc'],capture_output=True,text=True,timeout=15,check=True)\n"
                "ids=r.stdout.splitlines();assert all(re.fullmatch(r'[0-9a-f]{64}',i) for i in ids) and len(set(ids))==len(ids)\n"
                "assert cid not in ids;print(json.dumps({'diagnostic_container_retired':True}))")
        receipt = self.session.call('primary',code,30)
        if receipt != {'diagnostic_container_retired':True}:raise ValueError('Exact retirement proof required')
        after = self.execution._observe(cleanup=True)
        verify_restored(self.execution.saved,{h:o['rows'] for h,o in after.items()},
                        after['primary']['volumes'],after['primary']['bind_sha256'])
        if any(observation_identity(before[h])!=observation_identity(after[h]) for h in ('primary','secondary')):
            raise ValueError('Restored runtime changed during retirement proof')
        return True

    def cleanup(self, *, restored=False):
        if type(restored) is not bool:raise ValueError('Explicit restoration mode required')
        if not self.attempted or self.cleanup_used:raise ValueError('Known single-use diagnostic cleanup required')
        self.cleanup_used = True
        self._guard(45, cleanup=True)
        previous = self.session.cleanup_mode
        self.session.cleanup_mode = True
        failures = []
        try:
            for site, directory in [('container', self.directory), ('primary', self.owner)]:
                try:
                    self._guard(45, cleanup=True)
                    if site=='container' and restored and self.container_retired():continue
                    code = cleanup_program(directory)
                    receipt = (self.session.api(self.cid, code, 45) if site=='container'
                               else self.session.call(site, code, 45))
                    if receipt != {'diagnostic_credentials_removed':True}:raise ValueError('Exact cleanup acknowledgement required')
                except BaseException as exc:  # noqa: BLE001 - always attempt the other owned credential site.
                    failures.append(type(exc).__name__)
        finally:self.session.cleanup_mode = previous
        if failures:raise RuntimeError('Both-site diagnostic credential cleanup requires recovery')
        self.runtime._write('worker-diagnostic-cleanup', {'container_id':self.cid,'diagnostic_credentials_removed':True})
        return {'diagnostic_credentials_removed':True}
