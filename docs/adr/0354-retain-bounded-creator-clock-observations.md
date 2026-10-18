# ADR0354: Retain bounded creator-clock observations

## Status

Accepted for preparation only. Supersedes ADR0334 single observation behavior and extends ADR0353 source binding. Existing clock limits, strict freshness and all consumed runs remain unchanged. No paid retry authorized.

## Context

ADR0353 control passed/restored with48000 journeys and no drops/errors. Candidate passed native safety and post-TTL audits, then failed creator-clock admission before observers or paid dispatch. The failed raw timing sample was not retained, so latency versus drift cannot be determined. Both arms restored and the allowance closed; the longer pair budget was not exhausted.

## Decision

Retain each nonce-bound raw response, local wall/monotonic endpoints and rejection before validating it. Allow at most three consecutive read-only observations for the same immutable fixture, each using a new nonce and the existing30s transport timeout. Only bounded-clock uncertainty can trigger another observation; malformed responses, nonce mismatch and transport failures fail immediately. Persist rejected observations even when a later observation passes. Never retry fixture creation, paid dispatch or a consumed arm.

The first valid observation must independently satisfy unchanged offset limits of two seconds at both endpoints, RTT at most five seconds and wall/monotonic agreement within0.1s. No averaging, widened tolerance or stale proof reuse. Strict fixture freshness/sale-window validation runs after sampling; an expired fixture still fails. Existing safety reserves and pair clock remain immutable.

## Alternatives

Loosen clock bounds or freshness: rejected. Infer past RTT from a missing sample: unsupported. Retry paid stage: forbidden. Retain diagnostics only with a single sample: preserves diagnosis but remains unnecessarily vulnerable to an isolated slow read-only observation. Unbounded retries: rejected.

## Consequences

At most three read-only requests, bounded by90s of transport time, precede strict fixture validation. This may recover a transient observation delay but does not establish the historical cause or improve application throughput. Persistent clock uncertainty still stops the arm. Source changes require fresh same-pair control and new paid approval.

## Persistence, locking, messaging, idempotency, TTL and scaling

Only private experiment evidence and read-only observation count change. Preserve application transactions, locks, messaging, idempotency, expiry/TTL, pool limits, resources and images. Store attempts in the existing checkpoint record before validation; never overwrite earlier attempts or resample an already sampled fixture on replay.

## Failure and recovery

Fail closed after three rejected timing observations or immediately on malformed/unauthenticated response or transport error. No successful clock proof is installed on failure. Retain all sample timings and failure classes for diagnosis; normal financial reconciliation, drain and restoration remain mandatory after any failure.

## Validation evidence

Initial regression run113 passed,3 failed because synthetic bindings omitted the newly mandatory clock contract. Corrected those fixtures; affected binding/drift tests passed, including a new explicit clock-contract rejection. Across runs117 unique cases passed. New observation tests cover first-failure/later-success with both raw samples retained, three-attempt exhaustion, distinct nonces, malformed/transport hard failure and replay denial; existing strict freshness and clock-limit rejection tests passed. Ruff passed. Actual both-arm plans/manifests/bindings and real isolated control reservation passed with live bookkeeping unchanged. Seven fresh templates compile; no approval receipt, live sequence or paid allowance was created. [Readiness receipt](../capacity/cce/creator-clock-observation-readiness-2026-10-19.json). Logs: tmp/adr0354-regressions.log, tmp/adr0354-corrected-bindings.log, tmp/adr0354-preparation.log and tmp/adr0354-ruff-final.log.
