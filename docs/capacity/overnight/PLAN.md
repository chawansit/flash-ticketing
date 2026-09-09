# Overnight engineering sequence

Authorized scope: refresh hot-seat bottleneck measurements; implement and compare an evidence-based scheduling optimization; mixed workload plus increasing RPS; sustained load and isolated failure/recovery drills on the existing two Huawei ECSs. Preserve all failures. No capacity escalation after a stage fails its SLO/correctness gates. No production maximum claim from two VMs.

1. Baseline: 100 and 1,000 contenders, warm verified connections, explicit client queue/protocol/application/dispatcher/Redis/database timings; one live durable owner per wave.
2. Candidate: record an ADR before implementation, run focused and full applicable tests, compare fresh contention waves and uniform 400 RPS. Adopt only with preserved ownership/auth/idempotency and measured benefit; retain unmet targets explicitly.
3. Mixed read/distinct holds/hot contention, then 400/500/600/800 RPS bounded stages. Stop escalation at first failing stage; retain diagnostic output. Expected seat conflicts and overload are separated from correctness, transport and latency gates.
4. Thirty-minute soak at the highest conservatively validated passing rate, then bounded Redis, PostgreSQL and Kafka interruption/recovery checks including duplicate payment callbacks and exactly one ticket. Restore each dependency in finally paths. No overlapping fault injections or retries hiding load errors.

Final steps: verify acknowledged holds after expiry, report queues and failures, remove credential manifests, stop benchmark containers, remove temporary firewall rule, leave ECSs and synthetic volumes available, publish code/ADRs/results. A failed candidate does not stop independent baseline validation; unsafe or unavailable infrastructure stops dependent load.

Completion: all four steps executed. Both mixed 30-minute soaks failed their strict error gate; no new clean sustained capacity point is claimed. Final durable/recovery audits and cleanup passed. See README.md and validation.json for outcomes and retained failures.
