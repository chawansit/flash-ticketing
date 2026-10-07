"""ADR0196 guarded dependency probes; no registered CLI, worker entrypoint or dispatch."""
import copy
import hashlib
import inspect
import re
import time
from datetime import UTC, datetime
from urllib.parse import parse_qsl, unquote, urlsplit

import work_envelope as policy
from fetch_status_refresh_parents import owner_path
from stage_status_refresh_images import new_stage_output
from two_host_topology import bindings, environment, semantic
from worker_separation_execution import observation_identity, row_identity, verify_phase
from worker_separation_runtime import observation_program
from worker_separation_snapshot import runtime_semantic
from worker_separation_topology import WORKERS

CHECKS = ('verified_rds_tls', 'redis_ready', 'pooler_ready', 'private_kafka_ready', 'all_four_apis_reachable')


def dependency_context(execution, ca_pem):
    """Derive connection authority from the sealed pair and original pooler snapshot."""
    model = execution.pair['control']['primary']
    routes = {tuple(model['services'][r]['environment'][k] for k in ('DATABASE_URL', 'REDIS_URL', 'KAFKA_BOOTSTRAP'))
              for r in WORKERS}
    if len(routes) != 1:
        raise ValueError('All workers must share exact dependency authority')
    database_url, redis_url, kafka = routes.pop()
    db, cache = urlsplit(database_url), urlsplit(redis_url)
    from prepare_two_host_scaling import private_ipv4
    primary = private_ipv4(db.hostname)
    query = parse_qsl(db.query, keep_blank_values=True)
    if (len({key for key, _ in query}) != len(query)
            or any(not ((key == 'sslmode' and value == 'disable')
                        or (key == 'connect_timeout' and value in {'1', '2', '3', '4'}))
                   for key, value in query)):
        raise ValueError('Pooler URI overrides are forbidden')
    if db.port != 5432 or kafka != primary + ':19092' or cache.scheme not in {'redis', 'rediss'}:
        raise ValueError('Exact prepared private dependency routes required')
    pg = execution.saved['model']['services']['pgbouncer']
    env = pg['environment']
    rds = {'host': private_ipv4(env['DB_HOST']), 'port': int(env.get('DB_PORT', '5432')),
           'dbname': env['DB_NAME'], 'user': env['DB_USER'], 'password': env['DB_PASSWORD']}
    if ((rds['user'], rds['password'], rds['dbname']) != (unquote(db.username or ''), unquote(db.password or ''), unquote(db.path[1:]))
            or not 1 <= rds['port'] <= 65535 or env.get('SERVER_TLS_SSLMODE') != 'verify-full'
            or not isinstance(ca_pem, str) or not 0 < len(ca_pem.encode()) <= 1024 * 1024):
        raise ValueError('Exact application identity and verified pooler TLS required')
    mounts = [m for m in pg.get('volumes', []) if m['type'] == 'bind' and m['target'] == env.get('SERVER_TLS_CA_FILE') and m.get('read_only') is True]
    if (len(mounts) != 1 or execution.saved['bind_sha256'].get(mounts[0]['source']) != hashlib.sha256(ca_pem.encode()).hexdigest()):
        raise ValueError('CA content must match exact original read-only pooler bind')
    return {'rds': rds, 'ca_pem': ca_pem, 'database_url': database_url, 'redis_url': redis_url,
            'kafka': kafka, 'primary': primary}


def probe(context):
    """Executed only inside an owned probe; authenticate/read, never process business data."""
    import ipaddress
    import json
    import os
    import socket
    import tempfile
    import urllib.request
    from urllib.parse import urlsplit

    import psycopg
    from kafka.admin import KafkaAdminClient
    from redis import Redis

    def private(host, port):
        addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        parsed = [ipaddress.ip_address(a[4][0]) for a in addresses]
        if not parsed or any(not a.is_private or a.is_loopback or a.is_unspecified or a.is_link_local or a.is_multicast for a in parsed):
            raise ValueError('Dependency must resolve exclusively to private addresses')

    for key, port in (('database_url', 5432), ('redis_url', 6379)):
        route = urlsplit(context[key])
        private(route.hostname, route.port or port)
    private(context['rds']['host'], context['rds']['port'])
    private(context['primary'], 19092)
    params = dict(context['rds'])
    expected = (params['user'], params['dbname'])
    fd, ca = tempfile.mkstemp(prefix='flash-readiness-', suffix='.pem')
    try:
        with os.fdopen(fd, 'w') as stream:
            stream.write(context['ca_pem'])
        params.update(sslmode='verify-full', sslrootcert=ca, connect_timeout=4,
                      options='-c default_transaction_read_only=on -c statement_timeout=3000 -c lock_timeout=1000',
                      application_name='flash-worker-dependency-readiness')
        with psycopg.connect(**params) as conn:
            row = conn.execute("SELECT current_user,current_database(),current_setting('transaction_read_only')='on',pg_is_in_recovery(),(SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid())").fetchone()
            if row != (*expected, True, False, True):
                raise ValueError('Verified primary RDS read-only identity required')
        with psycopg.connect(context['database_url'], connect_timeout=4, application_name='flash-worker-pooler-readiness') as conn:
            conn.execute('SET TRANSACTION READ ONLY')
            conn.execute("SET LOCAL statement_timeout='3s'")
            conn.execute("SET LOCAL lock_timeout='1s'")
            row = conn.execute("SELECT current_user,current_database(),current_setting('transaction_read_only')='on',pg_is_in_recovery(),(SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid())").fetchone()
            if row != (*expected, True, False, True):
                raise ValueError('Authenticated pooler and encrypted primary backend required')
    finally:
        os.unlink(ca)
    cache = Redis.from_url(context['redis_url'], socket_connect_timeout=3, socket_timeout=3)
    try:
        if cache.ping() is not True:
            raise ValueError('Authenticated Redis ping required')
    finally:
        cache.close()

    client = KafkaAdminClient(bootstrap_servers=context['kafka'], request_timeout_ms=3000,
                              api_version_auto_timeout_ms=3000)
    try:
        observed = client.describe_cluster()['brokers']
        brokers = {(b['host'], b['port']) for b in observed}
        if len(observed) != 1 or brokers != {(context['primary'], 19092)}:
            raise ValueError('Exact private Kafka advertised broker required')
    finally:
        client.close()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    if len(context['api_urls']) != 4 or len(set(context['api_urls'])) != 4:
        raise ValueError('Four distinct observed API endpoints required')
    for url in context['api_urls']:
        with opener.open(url, timeout=2) as response:
            content = response.read(16385)
            if response.status != 200 or len(content) > 16384 or json.loads(content).get('status') != 'ready':
                raise ValueError('Exact API readiness required')
    return {k: True for k in ('verified_rds_tls', 'redis_ready', 'pooler_ready', 'private_kafka_ready', 'all_four_apis_reachable')}


def probe_program(saved, before, context, image, name, scope_sha, timeout):
    """Private stdin program with a shared remote deadline and exact probe cleanup."""
    if (not re.fullmatch(r'sha256:[0-9a-f]{64}', image) or not re.fullmatch(r'[0-9a-f]{64}', scope_sha)
            or type(timeout) is not int or not 10 <= timeout <= 60):
        raise ValueError('Exact fresh immutable bounded probe required')
    owner, marker, host = name.rpartition('-dependency-')
    if not marker or host not in {'primary', 'secondary'}:
        raise ValueError('Exact dependency probe host suffix required')
    owner_path('/qualification/repository', owner)
    prefix = observation_program(saved, bool(before['volumes']))
    prefix = prefix[:prefix.rindex('print(json.dumps(observe()))')].replace('time.monotonic()+25', 'time.monotonic()+' + str(timeout))
    body = 'import json\n' + inspect.getsource(probe) + '\ncontext=' + repr(context) + '\ntry:\n print(json.dumps(probe(context)))\nexcept BaseException:\n raise RuntimeError("Dependency readiness failed") from None\n'
    labels = {'org.flash-ticketing.dependency-scope': scope_sha,
              'org.flash-ticketing.dependency-context': policy.digest(context), 'org.flash-ticketing.dependency-owner': name}
    args = ['docker', 'create', '--pull=never', '--name', name, '--network=bridge', '--read-only', '--user=65534:65534',
            '--cap-drop=ALL', '--security-opt=no-new-privileges', '--memory=256m', '--cpus=0.5', '--pids-limit=64',
            '--tmpfs=/tmp:rw,noexec,nosuid,size=8m,mode=1777', '--no-healthcheck', '--entrypoint=python', '-i']
    for key, value in labels.items():
        args += ['--label', key + '=' + value]
    args += [image, '-']
    helpers = '\n'.join(inspect.getsource(f) for f in (environment, bindings, semantic, runtime_semantic, policy.digest))
    helpers += '\n' + inspect.getsource(row_identity).replace('policy.digest', 'digest') + '\n' + inspect.getsource(observation_identity)
    code = prefix + '\n' + helpers + '\nexpected=' + repr(policy.digest(observation_identity(before))) + '\nimage=' + repr(image) + '\nname=' + repr(name)
    code += '\nlabels=' + repr(labels) + '\nargs=' + repr(args) + '\nbody=' + repr(body) + r'''
def ids():
 return subprocess.check_output(['docker','ps','-aq','--no-trunc','--filter','name=^/'+name+'$'],text=True,timeout=budget(3)).split()
def owned(row):
 h=row['HostConfig'];c=row['Config']
 return (row['Name']=='/'+name and row['Image']==image and all(c['Labels'].get(k)==v for k,v in labels.items())
  and c['Entrypoint']==['python'] and c['Cmd']==['-'] and c['User']=='65534:65534' and c.get('Healthcheck')=={'Test':['NONE']}
  and h['NetworkMode']=='bridge' and h['ReadonlyRootfs'] is True and h['Memory']==268435456
  and h['NanoCpus']==500000000 and h['PidsLimit']==64 and h['CapDrop']==['ALL']
  and h['SecurityOpt']==['no-new-privileges'] and h['RestartPolicy']=={'Name':'no','MaximumRetryCount':0}
  and h.get('Tmpfs')=={'/tmp':'rw,noexec,nosuid,size=8m,mode=1777'}
  and all(m['Type']=='tmpfs' and m['Destination']=='/tmp' and m['RW'] is True for m in row.get('Mounts',[])))
if digest(observation_identity(observe()))!=expected or ids():raise ValueError('Changed runtime or existing probe prevents launch')
created=None;checks=None;attempted=False
try:
 attempted=True
 value=subprocess.run(args,capture_output=True,text=True,timeout=budget(5),check=False)
 if value.returncode!=0:raise RuntimeError('Probe creation failed')
 created=value.stdout.strip()
 if len(created)!=64 or any(c not in '0123456789abcdef' for c in created):raise ValueError('Exact probe ID required')
 row=json.loads(subprocess.check_output(['docker','inspect',created],text=True,timeout=budget(3)))[0]
 if row['Id']!=created or not owned(row):raise ValueError('Exact created probe semantics required')
 value=subprocess.run(['docker','start','-ai',created],input=body,text=True,capture_output=True,timeout=budget(35),check=False)
 if value.returncode!=0 or len(value.stdout.encode())>16384:raise RuntimeError('Dependency readiness failed')
 checks=json.loads(value.stdout)
finally:
 # Cleanup is separately bounded even after the readiness deadline, never broad or retried.
 deadline=max(deadline,time.monotonic()+10)
 current=ids() if attempted else []
 if current:
  if len(current)!=1 or (created is not None and current!=[created]):raise ValueError('Unknown probe identity retained')
  row=json.loads(subprocess.check_output(['docker','inspect',current[0]],text=True,timeout=budget(3)))[0]
  if row['Id']!=current[0] or not owned(row):raise ValueError('Unowned probe retained')
  subprocess.run(['docker','rm','-f',current[0]],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,timeout=budget(5),check=True)
  if ids():raise ValueError('Probe removal incomplete')
if digest(observation_identity(observe()))!=expected:raise ValueError('Runtime changed during dependency probe')
print(json.dumps({'checks':checks,'probe_removed':True,'runtime_unchanged':True}))
'''
    return code


class ReadinessActions:
    """Transport-bound component; registration and complete audit integration remain external."""
    def __init__(self, execution, ca_pem, *, monotonic=time.monotonic):
        self.execution = execution
        self.context = dependency_context(execution, ca_pem)
        self.context_sha = policy.digest(self.context)
        self.clock, self.used = monotonic, False
        self._guard(5)

    def _guard(self, timeout):
        self.execution._guard(timeout)
        if (policy.digest(self.context) != self.context_sha
                or self.execution.runtime.guard.binding.get('worker_dependency_context_sha256') != self.context_sha):
            raise ValueError('Protected dependency context must match exact fresh scope')

    def verify(self):
        if self.used or self.execution.failed or 'configure_common_infrastructure' not in self.execution.used:
            raise ValueError('Single-use readiness after common infrastructure required')
        self.used = True
        deadline = self.clock() + 90
        def call(host, program, limit):
            remaining = int(deadline - self.clock())
            if remaining < 1:
                raise TimeoutError('Shared readiness deadline expired')
            self._guard(min(limit, remaining))
            return self.execution.session.call(host, program, min(limit, remaining))
        record = {'decision':'ADR0196', 'context_sha256':self.context_sha, 'scope_binding_sha256':self.execution.scope_sha}
        try:
            self._guard(90)
            self.execution.runtime._write('dependency-intent', record)
            before = {h:call(h, observation_program(self.execution.saved, h == 'primary'), 30) for h in ('primary','secondary')}
            verify_phase(self.execution.pair, self.execution.saved, self.execution.arm, before, 'infrastructure')
            urls = []
            for row in before['primary']['rows']:
                if row['Config']['Labels']['com.docker.compose.service'] == 'api':
                    endpoints = [(address,port) for target, protocol, address, port in bindings(row) if target == 8000 and protocol == 'tcp']
                    if len(endpoints) != 1 or endpoints[0][0] != self.context['primary']:
                        raise ValueError('Exact observed private API endpoint required')
                    urls.append('http://' + endpoints[0][0] + ':' + str(endpoints[0][1]) + '/health/ready')
            if len(urls) != 4 or len(set(urls)) != 4:
                raise ValueError('Four distinct observed APIs required')
            context = {**copy.deepcopy(self.context), 'api_urls':sorted(urls)}
            image = self.execution.pair['control']['primary']['services']['consumer']['image']
            owner = new_stage_output().name
            for host in ('primary','secondary'):
                remaining = min(45, int(deadline - self.clock()))
                name = owner + '-dependency-' + host
                self.execution.runtime._write('dependency-probe-intent', {**record,'host':host,'name':name,'image':image})
                code = probe_program(self.execution.saved, before[host], context, image, name, self.execution.scope_sha, remaining - 12)
                receipt = call(host, code, remaining)
                if (not isinstance(receipt, dict) or set(receipt) != {'checks','probe_removed','runtime_unchanged'}
                        or receipt['probe_removed'] is not True or receipt['runtime_unchanged'] is not True
                        or not isinstance(receipt['checks'],dict) or set(receipt['checks']) != set(CHECKS)
                        or any(receipt['checks'][k] is not True for k in CHECKS)):
                    raise ValueError('Exact complete dependency and cleanup receipt required')
                self.execution.runtime._write('dependency-probe-ack', {**record,'host':host,'checks':receipt['checks']})
            after = {h:call(h, observation_program(self.execution.saved, h == 'primary'),30) for h in ('primary','secondary')}
            if any(observation_identity(before[h]) != observation_identity(after[h]) for h in before):
                raise ValueError('Independent runtime observation changed')
            result = {'binding_sha256':policy.digest(self.execution.runtime.audit_binding),'checked_at':datetime.now(UTC).isoformat(),
                      'context_sha256':self.context_sha,'checks':{k:True for k in CHECKS},'runtime_unchanged':True,'probes_removed':True}
            self.execution.runtime._write('dependency-ack', result)
            self.execution.dependency_receipt = copy.deepcopy(result)
            return result
        except BaseException:
            self.execution.failed = True
            raise
