# ADR0287: Capture complete slot history from existing API metrics

## Status

Accepted for explicitly bound new experiments under the standing work envelope. Supersedes ADR0272's mandatory repeated Kubernetes log polling only for this opt-in capture contract. Legacy scopes retain ADR0272. ADR0286 remains FAILED_RESTORED; no historical result is upgraded.

## Context

The ADR0286 hour delivered 302,280 unique paid-and-issued tickets inside the hour, with zero final customer failures, double bookings or payment loss. Full qualification failed on the redundant live log collector. That collector retained only exception classes; its exact failure is unknown. Final 600-second access-log retrieval hit the 8 MiB cap on every API pod. These are separate observations, not proof of a common cause.

Executed offline reanalysis of the immutable one-second metric trace, without adding log records, recovered all 72 failure sequences and reconciled settled terminal driver counters. The existing observer already reads the redacted failure ring from all four admitted API processes. Repeated Kubernetes log retrieval adds a second transport and failure channel without adding required failure records in this hour.

## Decision

For a fresh source-bound scope, use that existing metric trace as authoritative admission history. Require all four native admission receipts, unchanged process identities, offered-window bracketing within two seconds, the existing maximum two-second sampling gap, strictly monotonic samples, every failure sequence from one through each settled terminal counter, consistent duplicate records and agreement with independent driver failure counters. Retain every sample, including pre-window and terminal observations. Missing records after ring rollover remain a failure; no log-based retry or inferred record fills a gap.

Bind the decision and capture implementation digest before deployment/dispatch. Preserve the existing `live_admission` completeness gate, explicitly record its ADR0287 source, and require complete history before that gate can pass. Do not start the ADR0272 polling thread or retrieve generic access logs for this opt-in route. Keep the legacy route unchanged. Capture safe reason codes for diagnostic failures so a new failure is actionable.

No application image, SQL, connection budget, pod resources, workload, customer recovery, latency threshold, financial audit or queue-drain rule changes. This is a diagnostic transport correction, not an application performance claim or a remedy for unproven WAL/storage stalls.

## Alternatives

- Increase access-log byte limits: still transfers unrelated HTTP logs and cannot fix an unknown live-collector exception.
- Add another log endpoint or durable failure stream: application/image changes and extra operational work are unnecessary while the existing trace can prove completeness.
- Ignore diagnostic failures or silently regrade ADR0286: rejected; original evidence and all correctness/coverage gates remain binding.
- Keep both transports mandatory: duplicates evidence collection and retains the observed qualification failure channel.

## Consequences

New tests avoid repeated Kubernetes control-plane calls and access-log payloads. Burst loss beyond the retained ring is detected by sequence/counter reconciliation and still fails qualification. This correction cannot establish why payment holders stalled in commit or guarantee that future bursts will fit the ring. It also does not establish maximum capacity or flash-opening readiness.

## Failure and recovery behavior

Wrong source binding, replaced process, missing endpoint/sample, coverage gap, conflicting or missing sequence, counter reset or unsettled terminal counters fail diagnostic completeness. Preserve the failed trace and safe failure code, stop escalation, and still perform independent financial/post-TTL verification, queue drain and exact owned-resource restoration. Never replay a spent scope automatically.

## Validation evidence

Before implementation: strict metrics-only reanalysis of ADR0286's retained 3,666 samples recovered all 72 failure records without logs. Executed after implementation: 122 capture/entry/verification/profile tests passed, followed by 144 legacy observer, lifecycle, rollover and pipeline tests passed with one skipped test. Ruff passed. A full repository suite is not claimed. The new contract validated all 3,666 retained hourly samples and all 72 failure sequences with unchanged admitted process identities and window coverage. Maximum commit phase age was 664.239 ms; holder connection age was 698.8 ms, a different measurement.

All 72 failures have a nearest independent database activity sample within 0.491 seconds. Seventy-one coincide with WALWrite and WalSync waits; these are temporal correlations, potentially shared across several failures and other database work, not proof of a disk/WAL root cause. See [offline evidence](../capacity/cce/metric-admission-offline-evidence-2026-10-12.json).

Fresh cloud run adr0151-1336b509b866 at unchanged84/s for300s passed all ten measurement gates and exact restoration. All25,200 scheduled journeys dispatched, completed and reconciled to unique paid-and-issued tickets. Eighteen initial payment errors recovered; zero drops/final customer failures/payment loss/double-booking. All22 slot failure sequences were captured from364 metric samples, with zero additional Kubernetes log requests and no diagnostic collection failures. Payment p95 was111.227ms and total journey p954630.176ms (worst shard, not a combined percentile). This is diagnostic correctness evidence, not a demonstrated performance gain. See [cloud short result](../capacity/cce/metric-admission-paid-result-2026-10-12.json). Only if all gates and restoration pass may a separately registered fresh hour follow, using the existing 90-minute ceiling. No automatic replay of either spent scope. The original hourly qualification remains failed.
