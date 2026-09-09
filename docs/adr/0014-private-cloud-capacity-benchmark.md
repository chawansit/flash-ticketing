# ADR 0014: Private-network capacity benchmark

Status: Accepted; historical and latest staged validation results recorded below.

## Context
Windows generation dropped arrivals at 100 RPS without backend errors. The user supplied
an independent c6.2xlarge.2 generator (8 vCPUs/16 GiB) for the existing c6.xlarge.2 backend.

## Decision
Use direct private-IP HTTP traffic between the authorized ECS instances. Bind only the
isolated benchmark API to the backend private address and install a narrowly scoped
Docker forwarding firewall rule rejecting other external source addresses for that port.
SSH is administration only, not the measured data path. Do not change cloud services or
purchase resources. Preserve current application/DB/Redis/Kafka settings for comparison.

Extend the measurement harness for bounded in-flight task retention, explicit phase
progress, transport error diagnostics, and up to two million samples for a sustained run.
Retain the 50ms scheduling-lag drop rule. Report generator limits distinctly from server
limits. Require reads p95 <=150ms, holds p95 <=300ms, no unexpected responses or drops.
Stop escalation on failure; refine the boundary then run a 30-minute sustained stage
at a passing rate if the generator and fixture budgets permit it. Use unique seats within
each invocation and allow earlier holds to expire before reuse; do not simulate payments
in this read/hold throughput workload. Separate payment/correctness tests remain required.

## Alternatives
Public-IP or SSH-tunneled requests introduce a different network path. Buying managed
services now would change the system under test. Keeping every completed asyncio task
would distort a longer run's memory use.

## Consequences
Capacity applies only to this topology, inventory and workload. No HA or multi-node
production conclusion follows. Latency samples/workload planning still use O(requests)
memory, capped at two million; active tasks are bounded. A saturated generator produces
an inconclusive capacity result, never a backend maximum.

## Failure/recovery
Keep failed artifacts. Stop benchmark services and remove the temporary forwarding rule
at completion, preserving synthetic volumes. Keep credentials in private temporary files,
never Git. A failed connection check must be diagnosed without opening unrestricted ingress.

## Validation
Private connectivity, staged load and sustained validation executed; see validation evidence below. Extends ADRs 0012/0013;
no runtime booking or durability decision is superseded.

## Interrupted run
Both ECS hosts restarted while the 200-RPS stage was in progress. Only the completed
100-RPS artifact remains valid; the partial stage is retained and restarted separately.
The temporary Docker forwarding rule was lost on reboot and restored before resumption.
The benchmark remains dependent on the user's cloud security-group rules during host
boot. No public-access audit or persistent firewall qualification is claimed.

For the resumed benchmark, disable automatic API-container restart in the private
Compose override. This prevents Docker from reopening the benchmark port after a host
reboot before the temporary source restriction is restored. An interrupted API requires
explicit guarded restart; this does not represent a production availability policy.

## Generator scaling if required
If a single Python process drops arrivals while CPU-bound, use four independent
generator processes with disjoint show/viewer partitions and a shared future start
time. Preserve total inventory, viewer count and 95/5 mix. Require each worker's gate
to pass and start skew <=100ms. Report aggregate throughput and worst-worker p95;
never average percentiles or label that value an exact merged percentile. This changes
the generator implementation, not the backend. Retain failed single-process evidence.

## Executed validation evidence
The four-worker 352-RPS run completed 1,800 seconds and 633,600 HTTP requests with
zero generator drops. Worst-worker read p95 was 24.657 ms and hold p95 71.163 ms,
but 20 holds returned 503: the zero-error gate failed. No production maximum is
claimed. All 31,660 acknowledged holds matched persisted holds/orders and expired.
Two real-stack end-to-end tests passed (100 contenders/one seat and duplicate
payment callbacks/one ticket). Ruff passed for the changed benchmark scripts.
See [the report and raw artifacts](../capacity/huawei-private/README.md).
Earlier interrupted and cold-bootstrap failures remain separate. Backend services
were stopped, temporary forwarding guard removed after shutdown, and synthetic
credentials deleted. The full application suite was not rerun in this measurement.

## Staged validation after ADR 0017

The user requested 400, 500, 650 and 800 RPS, each for five minutes, stopping at
the first failed gate, then a 30-minute run at the highest passing level. Keep the
existing backend image/configuration and four generator processes. For totals not
divisible by four, distribute integer rates with a difference of at most one RPS:
650 becomes 163/163/162/162. Each process retains a disjoint show/viewer partition,
shared start time, the same 95/5 mix and unchanged per-worker gates. Record assigned
rates explicitly. This extends harness allocation only; no runtime architecture,
locking, messaging, TTL, admission or persistence decision changes. Do not round the
requested aggregate rate. Preserve failed results and distinguish generator limits
from backend limits. Sustained fixtures use a separate seat range after expiry drain.
[Executed staged validation](../capacity/staged-browse-body/README.md): 400 RPS passed
five minutes; 500 RPS had 45 ADMISSION_FULL responses in five minutes, so 650/800
were not run. After expiry/queue drain, 400 RPS passed 30 minutes: 720,000 requests,
zero errors/drops, read/hold p95 9.428/41.906 ms. All 49,455 accepted holds across
the runs passed durability/expiry checks. Two coordinator CLI tests and two cloud
end-to-end tests passed; the full application suite was not rerun. This is a
verified operating point, not an exact production maximum.
