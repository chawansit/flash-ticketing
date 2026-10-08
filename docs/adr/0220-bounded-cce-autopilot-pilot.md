# ADR0220: Bounded CCE Autopilot pilot

## Status
Accepted for preparation and a bounded pilot under direct user authorization on 2026-10-08. No workloads deployed or CCE capacity measured.

## Context
The user created a Huawei CCE Autopilot cluster and supplied an API endpoint plus X.509 certificate files. The measured ECS 1+3 topology passed 60 journeys/s; 84/s probe local qualification is in progress. Separating background services from API CPU is a relevant next experiment, but a new topology cannot inherit existing ECS benchmark qualification.

## Decision
Prepare separate Kubernetes Deployments for each application role, with immutable registry image digests, explicit CPU/memory requests and limits, readiness checks and an aggregate database connection budget. Keep existing RDS, DCS and Kafka dependencies initially. Begin with verified-TLS read-only cluster/network discovery; inspect effective admission settings before selecting resources or applying workloads. Do not bypass TLS or expose certificates, private keys or connection secrets in Git.

The user explicitly approved a bounded CCE pilot with no additional spending cap for TODAY ONLY, 2026-10-08 Asia/Bangkok. This narrowly supersedes ADR0172 zero-new-infrastructure-spend for the CCE pilot until 2026-10-08T23:59:59+07:00. Existing ECS experiments retain their original boundaries. Experiments remain bounded and tracked; no unlimited test duration or unattended schedules are introduced. Do not start a pilot that cannot complete owned cleanup before expiry. Ask for a new spending decision tomorrow before new billable pilot actions or extending owned workloads. Existing user-created cluster management charges continue; do not delete the user's cluster.

Prepare and validate the deployment before cloud mutation. A CCE load experiment requires a separately registered and locally qualified work-envelope profile, fresh identity, frozen baseline and all existing customer/correctness/audit/restoration gates. Initial API and background resource allocation must be stated explicitly; auto-scaling is disabled until connection budgeting and replica correctness are validated. The ECS 84/s probe remains independent and may not be relabeled as CCE evidence.

## Alternatives
Continue ECS-only rebalance: already measured and retains a valid baseline. Move only workers: isolates CPU contention with less deployment change. Migrate all dependencies including Kafka: changes too many factors and adds operational uncertainty. Enable unbounded automatic scaling immediately: risks multiplying database connections and undermines benchmark comparability.

## Consequences
Autopilot introduces pod, management and potential network/storage charges. Resource isolation may reduce CPU contention, but Kubernetes orchestration does not repair payment or database contention by itself. No improvement is claimed until completed paid tickets, customer errors, latency, CPU and database waits are measured. Registry availability, private routing, storage and shutdown behavior must be verified before launch.

## Failure and recovery behavior
Fail closed on cluster authentication, TLS, private networking, image identity, secret, pod budget or dependency checks. Preserve zero double-booking/payment loss, idempotency, customer authorization and durable queues. Isolate the pilot in an owned namespace and use exact ownership labels. Keep a restoration snapshot before changing any ECS traffic or workers. On failure stop progression, drain owned work, audit durability and restore the baseline; never delete unrelated cluster resources or the cluster itself. Remove transferred access material after each preflight. Uncertain cleanup blocks additional load.

## Validation evidence
Local X.509 client certificate and private key match; certificate is valid from 2026-10-08T02:04:27Z until 2031-10-08T02:04:27Z. Verified-TLS GET /version from the workstation timed out; no cluster mutation occurred. ECS-network read-only access passed with both the separately supplied certificates and the kubeconfig. Both temporary remote credential directories were removed. The kubeconfig uses another valid client certificate/key with the same CA. Kubernetes reported v1.36.2-r0-36.0.3; namespace listing succeeded. Proposed fixed allocation: 20 pods, 9 vCPUs, 14 GiB; no autoscaling, new ELB or new storage. Registry/source qualification and deployment validation remain pending. See [Access evidence](../capacity/cce/access-preflight-2026-10-08.json) and [Pilot plan](../capacity/cce/pilot-plan-2026-10-08.json).

Official references: [X.509 cluster access](https://support.huaweicloud.com/intl/en-us/usermanual-cce-autopilot/cce_11_0175.html), [Autopilot billing](https://support.huaweicloud.com/intl/en-us/productdesc-cce-autopilot/cce_12_0002.html), [Supported resource combinations](https://support.huaweicloud.com/intl/en-us/price-cce-autopilot/cce_03_0005.html).
