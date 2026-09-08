# ADR 0013: Isolated single-ECS capacity validation

Status: Accepted for benchmark deployment; production qualification pending.

## Context
The user authorized a production-level test on an existing Huawei c6.xlarge.2 (General computing-plus, 4 vCPUs/8 GiB advertised) ECS.
There is one supplied cloud host. A cloud-hosted backend with an external generator
can measure a node baseline but cannot establish multi-node availability or maximum
production capacity.

## Decision
Deploy the existing pinned Compose stack under a dedicated cloud benchmark project.
Use synthetic fixtures, isolated volumes, private loopback ports and SSH forwarding.
Run the generator on the Windows machine. Use development mode solely for synthetic
credential issuance; label results as production-style single-node staging evidence.
Keep database locking, TTL, messaging and idempotency decisions unchanged. Generate
fresh JWT/webhook secrets on ECS; never publish credentials. Run correctness checks
before staged load. Stop escalation on latency/errors/drops; preserve failed results.

## Alternatives
A real multi-node deployment with managed HA storage and a same-region generator
requires additional hosts/services and sizing decisions. Same-host generation hides
resource contention. Public test ports add unnecessary exposure.

## Consequences
All backend services share 4 CPUs and memory; no HA, replica failover or production
security certification. WAN/SSH latency and local generator capacity constrain results.
The development DB credentials are confined to this isolated disposable test stack;
this configuration is not approved for live customer data.

## Failure and recovery
Do not touch existing services or volumes. Retain synthetic data and failed-run evidence.
If image/package retrieval fails, diagnose without untrusted mirror substitutions.
Do not buy infrastructure. Stop the benchmark stack after collecting evidence; leave
volumes available for repeat tests. Close forwarding connections after retrieval.

## Validation
Executed on c6.xlarge.2: corrected fixture selection gives 71/71 tests passing.
50 RPS for 180 seconds passed; 100 RPS had five generator drops and no HTTP/transport
errors, so higher stages were not run. All 1,350 accepted holds/orders persisted.
Load samples: 7-8 SQL connections, no sampled lock waiters or missing maps.
Redis snapshot reload and explicitly empty-volume cold reconstruction were tested;
post-recovery end-to-end checks passed twice. No under-load HA/RTO claim is made.
Ruff and whitespace checks passed. See [full report](../capacity/huawei-single-node/README.md).
Extends ADR 0012; supersedes no runtime decision. Maximum production capacity remains
unverified because the Windows/WAN/SSH generator gate failed at 100 RPS.

## Isolated recovery check
After measured traffic ends, recreate only the benchmark Redis container to exercise
empty-cache reconstruction from PostgreSQL. Observe readiness and all fixture map TTLs,
then rerun end-to-end contention and payment checks. This is a single-node recovery
exercise, not HA failover. Never inject this fault into another Compose project.
