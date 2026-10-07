# ADR0208: Readiness failure phase receipts

## Status
Accepted for implementation. Supersedes only ADR0196 readiness failure reporting. Readiness requirements, deadlines, atomic holds, payment durability, topology and budgets remain unchanged.

## Context
A fresh ADR0207 comparison stopped during private dependency readiness before customer dispatch. Both hosts passed immutable image staging. Automatic restoration succeeded; independent booking, queue, generator and sealed cleanup checks passed. The transport discarded the original child exception and later recovery failures overwrote its private error file. A read-only diagnostic on restored services cannot reproduce the temporary topology: the original pooler intentionally has no published port. The exact temporary readiness failure remains unknown.

## Decision
Track one bounded phase inside the dependency probe. Return an allowlisted phase, exception category and failure category without exception text, credentials, addresses or connection strings. Preserve the failure receipt with scope/context identity only after exact probe cleanup and unchanged-runtime verification. The lifecycle must still fail closed and restore before any worker start or customer dispatch. Failed cleanup, lost responses and malformed receipts remain transport failures rather than verified dependency receipts.

No retry, deadline extension, weaker gate or backend image change is introduced. Keep successful receipt shape unchanged. Exercise generated code and actual owned container cleanup on both success and dependency failure before cloud use.

## Alternatives
Repeating the full comparison with a generic error loses evidence and spends time without answering the hypothesis. Publishing raw exceptions risks leaking secrets. Diagnosing only after restoration changes dependency routes. Capture safe phase evidence at the actual failure instead.

## Consequences
The next fresh readiness check can identify a dependency failure precisely. This changes diagnostic reporting only and provides no measured throughput benefit. Transport failures may still lack a dependency category.

## Failure and recovery behavior
A structured failure never authorizes worker startup. Remove only the authenticated owned probe and verify unchanged runtime before returning the receipt. Preserve the failed experiment and consumed scope. Retain mandatory restoration, zero-double-booking, financial and complete queue checks. Never replay a lost probe response or ambiguous experiment.

## Validation evidence
Fresh failed run: adr0153-parents-f441cc96fac4, zero customer/paid dispatch. Independent recovery: tmp/adr0153-parents-f1ad3524bff0/recovery-verification.json, all required checks passed; original result retained and scope closed FAILED_RESTORED. Read-only restored-route diagnostic: tmp/adr0153-parents-bc2a74806533/dependency-diagnostic.private.json, pooler connection refused as expected for its unpublished original route; not evidence of the temporary setup cause. Implemented. Initial validation executed: 181 passed and two local inventory-fixture failures in 80.73s; both corrected native cleanup cases passed in 5.48s. Final combined ADR0208/0209 suite: 278 passed, zero failures/skips, in 213.02s. Actual fresh cloud failure receipt retained at Kafka metadata discovery (NoBrokersAvailable) in tmp/adr0153-parents-48875bc9806a/0042-dependency-probe-failure.json. RDS, pooler and Redis completed before that failure. No customer dispatch or capacity claim.
