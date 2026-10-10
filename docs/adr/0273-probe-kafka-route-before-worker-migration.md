# ADR0273: Probe the Kafka route before worker migration

## Status
Accepted for bounded diagnosis. Extends ADR0228 with an explicit Kafka transport probe; ADR0271 placement and financial guarantees remain unchanged.

## Context
ADR0271 run adr0151-4b826076c234 stopped before paid traffic when consumer-0 exited with NoBrokersAvailable. Its filtered startup record identifies Kafka bootstrap unavailability. Kafka is running with its expected listener, and the ECS host firewall is inactive. These facts do not distinguish private network filtering from forwarding or protocol failure. All test resources were removed and original runtime restored.

## Decision
Reuse the registered zero-customer dependency probe with an explicit transport selector. The existing pooler probe remains the default. The Kafka variant creates one owner-bound private forwarder on TCP9092 and one bounded probe pod, checks TCP and an ApiVersions response without joining a consumer group or publishing, and removes both. Record exact variant and source binding. No worker migration or paid stage starts during this diagnosis.

## Alternatives
Blindly repeat all fourteen workers: costly and ambiguous. Change broker listeners or security groups without evidence: unjustified. Expose Kafka publicly: unnecessary. Replace Kafka: changes the benchmark architecture.

## Consequences
One short isolated probe adds limited cost and identifies which dependency fails. Successful TCP does not establish full Kafka consumer health or capacity. Protocol verification does not qualify paid traffic.

## Failure and recovery behavior
Keep current runtime unchanged. Use namespace/container ownership and captured identity before deletion, restore no unrelated resources, and retain failure type without raw logs or secrets. Failure to prove cleanup blocks subsequent experiments. The paid-stage allowance stays unconsumed.

## Validation evidence
Pending transport-binding, generated lifecycle/cleanup regression tests and one bounded cloud probe. No capacity improvement measured.

Executed local validation: 78 dependency and worker contract tests passed, including both transports under nine lifecycle/cleanup faults, fragmented ApiVersions responses, wrong correlation, truncated reply, connection refusal and exact variant binding. Ruff passed. The current authorization file is an explicit overlay with its original historical bytes preserved; historical lock values are unchanged.

Cloud evidence: adr0151-4bd053291e47 timed out on the Kafka request but did not distinguish connection from protocol. After adding explicit failure-stage capture, adr0151-ad0b300d5c1c confirmed TimeoutError during TCP connect from CCE. The same owner-verified forwarder passed TCP and ApiVersions from ECS. RDS and Redis passed from CCE. Both probe ledgers are FAILED_RESTORED: zero customer writes/paid stages, namespaces and bridges removed, credentials removed, runtime fingerprint unchanged and generator idle. The failing private route is established; the exact security-group, egress or network-policy rule requires infrastructure verification. Do not assume the broker itself is failing. The focused 63 dependency tests, 15 worker tests, 53 envelope tests and 17 reproduction tests passed; Ruff passed. [Redacted result](../capacity/cce/worker-rebalance-startup-result-2026-10-10.json).
