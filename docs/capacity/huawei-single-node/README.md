# Huawei single-node staging benchmark

Status: single-node baseline executed; maximum production capacity not established.

## Topology and reproducibility

Backend: user-confirmed Huawei ECS General computing-plus **c6.xlarge.2**, **4 vCPUs / 8 GiB** advertised (Ubuntu reports about 7.4 GiB usable), Ubuntu 24.04, one instance
of each Compose service. Dedicated project `flash-cloud-bench`, synthetic data only.
Application base commit f0315e1. Checkout-test fixture selection corrected separately;
no application runtime code changes. Docker Compose and Python virtual-environment
packages installed on the authorized ECS. Existing unrelated processes are untouched.

External generator: Windows desktop, Python/httpx, SSH local forwarding to ECS
loopback API port. WAN and encrypted tunnel overhead are included. Postgres, Redis,
Kafka and API have no newly exposed public ingress. JWT/webhook secrets generated
on ECS; development mode enables synthetic credential export. This is not a hardened
live production configuration, nor an HA deployment.

The stack uses the repository Compose defaults: single API process, 8 hold admissions,
12 app DB pool maximum, PgBouncer transaction pooling, Redis 256 MiB/noeviction,
30-second cache TTL and bounded reconciliation. See ADR 0013 for limitations.

Commands on ECS after copying the repository at the recorded revision:

```sh
docker compose -p flash-cloud-bench up -d --build
docker compose -p flash-cloud-bench run --rm seed
docker compose -p flash-cloud-bench run --rm tests
```

Prepare 800 x 300-seat synthetic fixtures using `scripts/seatmap_load.py` inside the
API container (one request, outside measurement), then export one-hour credentials
with `scripts/export_load_manifest.py`. Transfer only the manifest privately.
Run `scripts/http_load_generator.py` externally with 8,000 viewers, 95% conditional
reads / 5% unique-seat holds, 180 seconds per stage, 64 maximum in-flight requests,
rates 50/100/200/400, unused seat offsets 100/150/200/250. Stop at the first failed
read/error/drop gate. Bootstrap/warming is excluded. Run the backend observer with
`TEST_DATABASE_URL` pointing directly to the isolated PostgreSQL container and
`REDIS_URL` to its Redis, sampling every two seconds. Docker stats collected about
every ten seconds includes workers; API Prometheus snapshots include query/pool timing.

## Correctness setup

Initial suite: 70 passed, checkout failed because demo seed was absent. After seed:
70 passed, checkout selected another test's occupied event. Fixed the test to select
Bangkok Demo Concert explicitly. Corrected suite: **71 passed, 2 dependency warnings,
9.83 seconds**, zero skipped. This includes 100 same-seat contenders with exactly
one durable hold and payment/Kafka fulfillment with duplicate callbacks.

Staged load exercises reads and holds, not payment throughput. Integration fault tests
are not a substitute for multi-node failover or long-duration production soak tests.

## Measured stages

| Offered total RPS | Duration | Read p95 | Hold p95 | Generator drops | HTTP/transport errors | Gate |
|---:|---:|---:|---:|---:|---:|---|
| 50 | 180 s | 32.07 ms | 36.37 ms | 0 | 0 | Pass |
| 100 | 180 s | 40.33 ms | 49.14 ms | 5 | 0 | Fail: generator drops |

[50 RPS result](50rps.json), [100 RPS result](100rps.json). The 100-RPS generator
completed 17,995 of 18,000 offered requests; 900 holds succeeded. At 50 RPS all 9,000
requests completed with 450 successful holds. [Durable counts](durable-counts.txt)
confirm 1,350 holds and 1,350 orders for synthetic load actors in the isolated DB.
The initial fixture request and correctness-test actors are excluded by this predicate.

Rates 200 and 400 were not run because the 100-RPS generator gate failed. There is no
evidence of server saturation at 100 RPS: the observed ceiling is a limitation of this
test path, not a cloud service limit. A dedicated same-region generator is required to
remove Windows/WAN/SSH constraints before seeking higher capacity. No linear scale-out
projection or maximum production RPS is justified by this run. A three-minute passing
stage is also not a long-duration production soak or a 5-10x scaling qualification.

## Backend observations

[Load observations](load-observations.json) contain 243 two-second samples covering
bootstrap, stages and cooldown, with zero observer errors and zero missing maps.
Minimum sampled map TTL 10 s; maximum reconciliation age 20.42 s. SQL connections
stayed between 7 and 8; zero sampled lock waiters; oldest overdue hold 0.459 s.
Redis used about 47.6-48.2 MiB. Sampling cannot rule out shorter transient waits.

[Resource samples](resources.log), aligned to each result's completion timestamp minus
elapsed time, give 18 samples per stage. Summed container CPU average was 62.62% of
one core at 50 RPS and 113.48% at 100 RPS (about 15.7% and 28.4% of four vCPUs).
Sampled total peaks were 179.14% and 246.11% of one core. These sums are approximate
because docker stats observations are not perfectly simultaneous. API peaks were
39.80% and 50.31% of one core. Other processes and SSH costs are not in these sums.

[API metric start](api-metrics-start.txt) and [end](api-metrics-end.txt) counter
differences give 17,722 DB execute calls averaging 0.260 ms and 1,420 pool acquisitions
averaging 0.026 ms. These are client-side means over the capture window, not SQL-only
execution time or transaction p95, and do not cover other worker processes' pools.

## Recovery evidence

The initial Redis recreation reused an anonymous data volume and loaded its snapshot.
[Recovery observations](recovery-observations.json) include a BusyLoadingError, then
all 800 maps present at the next sample. This is restart/reload recovery, not proof of
empty-cache reconstruction. [Post-restart tests](recovery-tests.log): 2 passed.
An additional explicitly empty Redis volume check is recorded separately.

## Remaining production validation

Full production qualification still requires a dedicated same-region load generator,
longer steady-state and burst tests, payment-heavy traffic, hot-event skew, deliberate
failures under traffic, and multi-node/managed-service failover. The current test uses
800 active shows; 200 locations with up to 2,000 screens and multiple scheduled shows
need their own inventory and traffic model. This run supplies measured staging evidence
for c6.xlarge.2, not approval to deploy live customer traffic with Compose defaults.

### Verified empty-cache reconstruction

[Empty recovery log](empty-recovery.log) records Redis `DBSIZE` equal to **0** after
recreation with `--renew-anon-volumes`, before starting the other benchmark services.
[Subsequent samples](empty-cache-observations.json) show all 800 maps restored with
expiry. [End-to-end checks](empty-recovery-tests.log): **2 passed in 0.88 seconds**.
This was a cold stack restart, without concurrent client load. Sampling began after
startup, so it does not establish precise recovery latency or a production RTO.

All benchmark containers were stopped afterward; synthetic volumes and the private
configuration remain on ECS for repeat tests. Expiring token manifests were removed
from ECS and the local temporary directory. SSH sessions/tunnels were closed.
