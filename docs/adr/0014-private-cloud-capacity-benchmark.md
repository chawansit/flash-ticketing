# ADR 0014: Private-network capacity benchmark

Status: Accepted; benchmark executed. Sustained 352-RPS zero-error gate failed.

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
