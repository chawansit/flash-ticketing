# ADR0367: Recover admission history after traffic stops

## Status
Accepted2026-10-19 for the requested observer correction. Supersedes ADR0287's zero-log transport only in an explicit ADR0367 qualification; legacy modes retain their transport.

## Context
Fresh180 run4575dd20f256 recorded103 acquisition failures across four APIs. Each process retained only16events; the observer recovered67distinct records. Completeness correctly failed. The largest burst coincided with WalSync/WALWrite waits and500–640ms connection ownership. Missing events cannot be inferred. The immutable application already emits each redacted failure with sequence and slot ownership.

## Decision
Keep the application image, admission/timeout limits, one-second metrics, SQL, durability, resources and all quality gates unchanged. Select a source-bound terminal-admission mode for fresh guarded180/200 qualifications. After customer dispatch/completion stops, read each of the four exact owned API pod logs once, with600s lookback and64MiB input ceiling per pod. Filter on the trusted API host before returning data; retain at most128 redacted failures per API. Verify namespace/pod identities before and after each read. No database queries or log reads during offered traffic.

Merge those independently observed records with existing metric snapshots. Require exact nonconflicting sequences1..terminal failure count and agreement with all settled driver counters. Retain process/coverage checks, reject truncation, overflow, malformed records, wrong run/process/receipts and missing history. Log evidence cannot make customer errors pass. Record four additional log requests and the observer change explicitly in the plan. No persistence, financial locking, messaging, idempotency, TTL or scaling changes.

## Alternatives
Enlarge the application's ring: requires another immutable application image and increases every metric response. Poll logs during traffic: adds measurement work to the load window. Ignore missing sequences: rejected. Unbounded logs or customer-log retention: rejected.

## Consequences
After-window diagnostics add bounded log transfer and parsing time. Input beyond the ceiling, more than128 failures per pod, or a missing first sequence still fails closed. Clean zero-error qualification remains mandatory before200.

## Failure and recovery
Independent database audits, other diagnostics and mandatory restoration must still run when log capture fails. Historical failed results remain failed. No automatic paid repeat, infrastructure resize or main merge.

## Validation evidence
Final capture checks41passed; broader runner regression136passed/2optional skipped; actual extended transport test passed. Exact published-image22-helper/22-runtime network-disabled rehearsal and isolated admission passed. Default legacy transport retained. Initial fixture isolation and operator/runtime-view test errors corrected before any cloud work. No new cloud capacity result claimed.
