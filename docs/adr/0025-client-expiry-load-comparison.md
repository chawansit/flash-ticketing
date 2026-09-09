# ADR 0025: Controlled client keep-alive expiry load comparison

Status: Accepted; experiment completed. Default remains 5s; 2s is opt-in only.

## Context
ADR 0024 reproduced 22 transport failures near the 5s server idle boundary; a 2s client expiry had none in 480 reads. This low-rate result does not establish mitigation effectiveness or reconnect cost under sustained load. Earlier 400 RPS tests also produced intermittent ReadError.

## Decision
Expose an opt-in bounded client keep-alive expiry in the HTTP load generator and coordinator, retaining the 5s default. Count measured TCP connect attempts/completions/failures from bounded transport traces by operation, excluding bootstrap. Do not retry any operation. Run 400 RPS (95% reads / 5% holds), four 300s runs in 5s/2s/2s/5s order on the same private Huawei backend/generator, using disjoint seat offsets and unchanged application/server settings. Collect latency, errors, generator CPU, backend CPU and connection evidence, then verify acknowledged holds and expiry. Trace instrumentation is identical in both policies.

## Alternatives
Adopting 2s from a 480-read sample would lack sustained-load validation. Changing server timeout simultaneously would confound the comparison. Automatic retries would obscure failure rates and require separate semantics for mutations. A randomized longer study is deferred; ABBA reduces simple order bias but cannot eliminate time effects or characterize rare failures precisely.

## Acceptance criteria
Both candidate runs must meet the existing zero-error/zero-drop/accounting gates, read p95 <=150ms and hold p95 <=300ms; all acknowledged holds must persist and expire consistently. Report connection and CPU cost for every run rather than treating zero failures alone as sufficient. If neither policy reproduces an error, claim only bounded compatibility and cost observations, not demonstrated error-rate reduction. Default adoption remains a separate decision.

## Consequences
Shorter client expiry can increase connection churn and CPU. These are benchmark client settings, not server or production-user browser settings. Trace counters measure connections attempted by measured requests, not all host connections or idle socket closes. CPU samples are descriptive and container-wide. Zero errors in a bounded sample does not prove elimination or maximum production capacity.

## Failure and recovery
Retain failed runs and their nonzero exits. Abort later runs on missing results or unhealthy infrastructure; ordinary gate failures remain evidence. No retries. Use private development credentials, retain no manifests in published artifacts, drain holds before verification, stop test services and remove temporary firewall access after capture. No booking, messaging, persistence or idempotency decision is changed; ADR 0024 remains historical evidence.

## Validation evidence
Targeted tests: 9 passed in 6.04s. Full unit suite: 52 passed in 18.23s. Ruff passed. All four 400 RPS x 300s runs passed, totaling 480,000 requests with zero errors/drops. Candidate TCP connections were 1,340 versus baseline 603 (2.22x). Read p95 was 12.09/12.18/16.57/16.21ms; hold p95 49.03/51.44/59.60/54.94ms in ABBA order. API CPU means were 0.646/0.686/0.707/0.687 cores; full-response mix differed, so CPU/latency causality is not established. All 24,000 acknowledged holds persisted and expired consistently; final queues were zero. The first database observer launch lacked TEST_DATABASE_URL; corrected collection began about 16s into A1. No load run was discarded. Cleanup completed. [Full evidence](../capacity/client-expiry/README.md).

The candidate meets bounded compatibility gates but shows no error-reduction benefit in this workload and increases connection churn. Keep 5s as the default. Any production pooling or retry change requires a separate decision and representative idle/traffic testing; earlier ReadError root cause remains unconfirmed.
