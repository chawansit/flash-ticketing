# ADR0298: Bind the committed-status load comparison

## Status

Accepted under the standing engineering envelope for one fresh control and, only after all control gates pass, one candidate. Does not supersede historical failed qualifications. Extends ADR0297 with explicit runtime and experiment identities.

## Context

ADR0297 produced a verified, published shared image, but the existing cloud runner only admits previously pinned images and factors. Reusing a spent scope or changing its meaning would undermine comparison and recovery evidence. Five-minute stages are already proven; long-run congestion remains unresolved.

## Decision

Reuse the existing bounded 84 offered journeys/s, 300-second runner, with 25,200 distinct journeys per arm. Run the same immutable ADR0297 image for both arms. Bootstrap ECS and API roles with ORDER_STATUS_COMMITTED_SNAPSHOT=0; enable it only for the six native CCE fulfillment consumers in the candidate. Keep committed-event refresh/deduplication, one-second freshness, four 1-vCPU/2-GiB APIs, fourteen 250m/512Mi workers, 24 physical database connections, admission 20, consumer pool budget 48, generator concurrency 500, gateway delays and customer recovery unchanged. Keep the existing CCE ingress path for both arms; this is not another ingress comparison.

Each arm has a fresh one-paid-stage scope and a 3,600-second experiment ceiling. Candidate admission requires a fully passed, restored same-image control with matching runner, configuration, diagnostic, resource and source identities. The normal runtime snapshot is captured before bootstrap and restored after each arm. No increase in infrastructure specifications or weakening of quality gates.

## Alternatives

Modify the runner to a longer duration before the first comparison: defer to avoid combining profile changes with first image admission. Reuse historical control from another image: rejected. Change image and feature simultaneously between arms: rejected. Enable the flag on all roles: rejected. Direct untracked deployment/load: rejected.

## Consequences

The first pair measures the feature at the existing short workload. It cannot prove that nine-minute congestion is fixed, maximum capacity or hourly qualification. A subsequent longer run and increased rate require their own explicit profiles, retaining correctness and diagnostic gates.

## Failure and recovery behavior

Stop after a failed control. Preserve first-attempt errors, recovery attempts, final failures, generator drops, timing and incomplete diagnostics. Never upgrade failed evidence. Require zero double-booking and payment loss, post-TTL audit, both Kafka lanes and all queues drained, owned resources removed and original runtime restored. Failed financial/cleanup recovery blocks further load until resolved.

## Validation evidence

ADR0297 passed 211 candidate-source tests and published/pulled the immutable image with 22-module verification. Runner regression execution passed 156 checks; one setup check rejected a source hash changed during a formatting edit. With files stable, all 20 new comparison checks and that cleanup check passed (21 checks). Ruff and whitespace verification passed. An earlier test expectation incorrectly counted eight Secret roles instead of seven roles across fourteen pods; corrected before cloud execution. Read-only cloud preflight verified idle generator, drained queues, restored normal runtime and empty secondary host. Cloud comparison is pending; no capacity improvement is claimed.

Registry pull and 22-module cloud image verification passed. Initial scope registration stopped before deployment/load because the dependency spending guard still expected two ingress stages. Updated that guard only for the exact ADR0298 scope. Three focused checks and 100 dependency/hourly admission regressions passed; Ruff passed. No spending boundary or quality gate changed.


Cloud comparison completed with both arms PASSED_RESTORED: 25,200 dispatched and paid-and-issued journeys per arm, zero generator drops/final customer failures, zero double-booking/payment loss, complete queue drain and restoration. Control had zero initial customer errors; candidate had four payment503s (0.01587%), all recovered. Consumer acquisitions fell 75,603 to 50,405 (33.3%), with 25,195 committed reads avoided. Worst-shard journey p95 fell 2,753.45 to 2,607.27ms (5.3%). Issuance inside 300s was 25,082 and25,102 respectively; after-drain totals are not inside-window throughput.

Candidate failure-time holders included payment commits aged 469.99ms and query BEGIN phases 339.32ms; nearby samples showed WAL/data-file waits. Full query/backend attribution and underlying storage cause remain unproven. Candidate pooler maximum wait rose 4.147 to 37.684ms. This is mixed evidence: deterministic read reduction, small latency improvement, remaining tail risk. Keep default off; do not claim 100 successful tickets/s or hourly qualification.

Evidence: [comparison](../capacity/cce/committed-fulfillment-status-comparison-2026-10-13.json), [control](../capacity/cce/committed-fulfillment-status-control-result-2026-10-13.json), [candidate](../capacity/cce/committed-fulfillment-status-candidate-result-2026-10-13.json), [failure-time analysis](../capacity/cce/committed-fulfillment-status-failure-time-analysis-2026-10-13.json).
