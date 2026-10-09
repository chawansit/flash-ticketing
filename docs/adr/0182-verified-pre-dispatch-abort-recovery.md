# ADR0182: Verified pre-dispatch abort recovery

## Status
Accepted under the standing work envelope; implemented and locally tested. Fresh read-only recovery verified the consumed ADR0181 abort; a new comparison is pending.

## Context
ADR0181 safety qualification passed. Its measured control was blocked before the paid-stage dispatch reservation because the diagnostic inventory binding was added after its qualification fingerprint was recorded. Full diagnostic preflight, credential cleanup, post-TTL safety payment audit, global queue drain and topology restoration passed. The standing finalizer nevertheless marked the scope RECOVERY_REQUIRED because an unstarted paid stage cannot pass paid-load gates. These failed gates must remain failed and the consumed scope must never reopen.

## Decision
Correct the inventory receipt order by qualifying the complete diagnostic-bound inventory after staging and before dispatch. Preserve exact fingerprint and chronology checks. Add a narrowly scoped append-only recovery receipt for this known pre-dispatch abort. The original reservation status, reports, counters, failed gates and artifact bytes remain immutable. Recovery requires retained zero-dispatch evidence, the exact known receipt failure, full safety financial/authorization/concurrency/expiry audits, both credential cleanup sites, original topology restoration and a fresh read-only observation of an idle generator, unchanged runtimes, no secondary test resources and zero queues. Recovery never creates load permission or reuses the consumed scope; it only clears its restoration block so a separately qualified fresh reservation may be considered.

## Alternatives
Ignore receipt fingerprints or paid correctness gates: rejected. Rewrite the old result as passed or rerun its scope: rejected. Require human approval despite proven restoration: unnecessary under the standing envelope. Treat every failed unstarted stage as recovered: rejected; this receipt is restricted to the exact observed failure and verified retained artifacts.

## Consequences
A harness abort is distinct from a backend capacity failure. Additional recovery evidence is required, and any ambiguous dispatch, ownership, financial integrity or cleanup remains blocking. No capacity result is inferred from safety probes or a two-sample diagnostic trace.

## Failure and recovery behavior
Only the exact ADR0181 reservation and its retained dry/failed control reports may be verified. Reject symlinks, missing or changed artifacts, nonzero dispatch counters, alternate failures, false safety/financial/cleanup gates, active runs and stale fresh probes. Append the recovery receipt under the existing exclusive lock. Future scopes still require the ordinary registry, source/configuration binding, new identity and all gates. Preserve human pauses.

## Validation evidence
Pending local rejection tests and fresh read-only verification. The known scope retained a passing safety pair, no paid-stage customers, complete diagnostic preflight, two complete runtime samples, removed credentials, zero restored queues and restored original topology. These are recovery facts, not throughput qualification.

Executed 100 local recovery/envelope/diagnostic runner checks in 33.63 seconds, including the corrected inventory receipt and unchanged chronology checks. After identifying the actual lowercase Docker absence message, 28 focused recovery/parser checks passed in 0.21 seconds. Earlier read-only helper failures (incorrect configuration key, overly strict container-removal assumption, and case-sensitive message parsing) were retained; no load or cloud mutations occurred. The final read-only verifier completed in 19.594 seconds and passed all eleven recovery gates. The original RECOVERY_REQUIRED reservation and failed gates remain unchanged; its separate [verified recovery receipt](../capacity/flash-sale-opening/pre-dispatch-abort-recovery-2026-10-06.json) clears only the restoration block. No paid-stage throughput or capacity improvement was measured.
