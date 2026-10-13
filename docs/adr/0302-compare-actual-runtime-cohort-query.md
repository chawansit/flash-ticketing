# ADR0302: Compare actual runtime cohort query

## Status

Accepted for bounded read-only measurement under the standing work envelope. No deployment or paid-stage activation.

## Context

ADR0301's unchanged paid repeat passed all quality and restoration gates with zero customer errors. Its actual ADR0283 LATERAL observer accumulated 31.40 million buffer hits (62.57% of recorded hits) and 44.43 seconds of query execution over the dispatch/completion span. ADR0300 compared base SQL rather than that runtime query; its result cannot justify adoption. API CPU was not saturated during the repeat. Write/WAL contention remains visible, without proof of the exact cause of prior payment503s.

## Decision

Compare the exact query produced by observe_cce_paid_pipeline.install_indexed_cohort_query against the existing ADR0247 array alternative on the retained, verified ADR0301 cohort. Use one protected verified-TLS, read-only database connection while generators are idle and no paid experiment is active. Bind the fixture and SQL/adapter hashes, preserve raw plans privately, and publish only counters, plan costs and provenance.

Execute three alternating-order pairs, collecting all nine exact counters and EXPLAIN ANALYZE with buffers and WAL. Use repeatable-read, read-only snapshots and a two-second deadline per statement; the remote measurement has a 55-second guard. No statistics reset, planner overrides, schema/index changes, business writes, pod changes, additional permanent connections or customer traffic. Preserve the existing sampling cadence and all independent financial audits.

Reject counter differences, statement failures, incomplete measurements or a measured regression. Any runtime integration is a separate identity-bound decision; idle query improvements do not authorize claims about customer capacity.

## Alternatives

Adopt from the base-query microbenchmark: rejected. Reduce sample frequency or omit correctness counts: rejected. Increase connections or resize RDS: deferred without a measured comparison. Re-run paid traffic before choosing a correction: deferred.

## Consequences

Completed-cohort measurements are sensitive to cache order and do not represent growing cohorts, contention or hour-long qualification. Alternate order and retain every result; do not silently discard slower or failed samples. Summed SQL execution is not CPU utilization.

## Failure and recovery behavior

Reject active load, missing fixture evidence, mismatched certificate or unexpected runtime identity. Hold the existing local run lock during the bounded read-only inspection. Roll back read-only transactions on errors, close the connection, delete the temporary certificate and clear credentials. Retain failed evidence and verify original runtime identity afterward. Financial rows and queue ownership remain unchanged.

## Validation evidence

Executed three alternating-order pairs on ADR0301's verified 25,200-ticket cohort. All nine counters matched in all six measurements. Actual-runtime EXPLAIN execution mean was 245.322ms versus 219.787ms (10.41% lower); buffer hits were 203,179 versus 142,291 (29.97% fewer), with zero physical reads in all plans. Every sample is retained, including the slower final candidate plan and first-current SELECT cache warming. [Measured comparison](../capacity/cce/actual-runtime-cohort-query-comparison-2026-10-13.json).

The first local preflight stopped before SQL because the wrapper called a nonexistent acquire method; RunLock acquires in its constructor. Its lock and session were closed, the call was corrected, and a fresh attempt completed. Original runtime identity was unchanged. No customer load, deployed correction or paid-ticket capacity improvement occurred.
