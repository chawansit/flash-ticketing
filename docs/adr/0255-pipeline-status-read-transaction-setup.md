# ADR0255: Pipeline status-read transaction setup

## Status
Accepted for isolated default-off implementation and local validation; cloud benefit unmeasured.

## Context
The failed ADR0252 hourly trace retains 56 order_status/query_BEGIN holders, with maximum observed occupancy 362.487 ms, alongside payment/callback commit holders up to 443.303 ms. Missing snapshots prevent attribution of all timeouts or WAL/disk causality. Current status reads issue explicit BEGIN, local timeout configuration and a single joined SELECT sequentially while occupying a database connection. This decision targets avoidable network round trips on those reads, not a claimed repair to RDS commit latency.

## Decision
Add a default-off ORDER_STATUS_READ_PIPELINE flag. For get_order only, enqueue the accepted explicit BEGIN and unchanged local timeout configuration in the same PostgreSQL pipeline as the existing actor-scoped SELECT. Fetch the complete result before returning and commit before publishing a cache result. Preserve the write transaction path, READ COMMITTED statement semantics, pool budgets and query/lock/idle timeout values. Do not remove BEGIN globally: the rejected earlier candidate stays rejected. Payment-operation recovery reads remain unchanged to isolate this correction. Normalize the unqualified branch-wide skip-BEGIN implementation back to the accepted explicit-BEGIN control before testing; this restores the benchmark baseline, it is not a new performance factor. Both comparison arms use identical customer recovery; only the status-read pipeline flag differs.

## Supersession
Extends explicit transaction setup for this one optional read path. No default write, persistence, lock ordering, idempotency, TTL, payment, outbox or delivery guarantee is superseded.

## Alternatives
Increase connection limits: can move congestion into RDS. Remove every explicit BEGIN: previously rejected. Raise acquisition timeout: changes customer waiting and hides congestion. Add read replicas or more API pods: changes topology before this correction is measured. These alternatives are deferred.

## Consequences
Fewer sequential status-read setup round trips are expected, not proven capacity improvement. Pipeline errors can surface at fetch or exit; rollback and pool return must restore a usable connection. Commit spikes may remain and require another evidence-based decision if this candidate does not help.

## Failure and recovery behavior
Use the existing transaction error, rollback and pool-return behavior. No uncommitted result is cached. Cancellation/SQL error must not leak a checked-out connection or settings. Disable the flag to restore control behavior. Stop at failed control/gates; preserve all evidence.

## Validation evidence
Executed against local PostgreSQL 17.6: four integration cases passed for control/pipeline owner isolation and result equivalence, unchanged lock/statement/idle timeout values, SQL error rollback and subsequent pool reuse. Broader reservation/payment/Redis regressions passed (139 selected cases, overlapping the ADR0254 tests). The complete unit suite passed 2,098 tests with two skipped. A subsequent 22-case integration run passed, including actual API subprocess termination/restart with the pipeline both off and on.

Both cloud arms are prepared to use the same recovery-enabled API image and generator. Control sets ORDER_STATUS_READ_PIPELINE=0, candidate sets it to 1. Four 1-vCPU/1-GiB API pods, four connections per API (two payment/two general), shared acquisition budget 20, PgBouncer 24 and simulator 12/pool 10 remain unchanged. The candidate requires a fresh passing and restored control receipt. The profile refuses hourly use and refuses deployment before registry publication/pull verification.

Pipeline execute-call timings include enqueue time rather than complete server execution; compare total transaction/connection occupancy, fetch wait and commit latency rather than interpreting faster enqueue histograms as faster SQL.

No cloud load started: a fresh SWR credential is required after Authenticate Error. The isolated correction is implemented and locally verified; improvement in slot occupancy, latency or ticket throughput is unmeasured. Missing historical snapshots still prevent a complete timeout attribution.

Protocol reference: [Psycopg pipeline documentation](https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html) explains ordered command execution, batching and synchronization on fetch, pipeline exit and commit. Local behavior tests use the pinned installed driver rather than assuming current documentation alone proves correctness.

Historical ADR0241 image reconstruction is pinned to the exact be4cbd8 baseline source bytes under artifacts/slot-comparison-baseline, with a fixed manifest and per-module digests. Later recovery code is not silently included in older comparisons. Tampering still fails before output creation. The recovery image separately starts from the accepted immutable registry image and verifies all 22 parent source modules; only six allowlisted modules change.
