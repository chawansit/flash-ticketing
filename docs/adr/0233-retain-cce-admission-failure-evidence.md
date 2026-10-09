# ADR0233: Retain CCE admission failure evidence before cleanup

Status: Accepted for local implementation and one fresh diagnostic control; capacity remains unqualified.

## Context

ADR0231 control adr0151-c5edbd09ed73 restored successfully but had two immediate database-admission errors: one payment request and one status read. One-second metrics cannot identify the precise guard occupancy at rejection. The frozen backend already emits structured failure-time snapshots. The runner deleted owned API pods without retaining those snapshots, so selecting a global versus role-specific budget correction from this run would be speculative.

## Decision

Immediately after the paid generator completes, read logs from each exact namespace/pod UID before observation collection and cleanup. Read a fixed 600-second window with an 8-MiB response ceiling per pod, retain at most 128 whitelisted admission snapshots per pod, and record byte truncation, overflow and malformed-event counts explicitly. Filter on the authenticated primary helper before returning diagnostic data; exclude arbitrary log messages, secrets and request identifiers. Verify namespace and pod ownership both before and after each read. Persist a private evidence file and a sanitized hash/count receipt. Capture errors remain explicit and cannot prevent independent financial verification, queue drain or cleanup. Do not infer absence of failures from an incomplete log capture.

Run one fresh unchanged 84 journeys/s, five-minute diagnostic control under the user's continuing instruction, existing four-pod/connection budgets and uncapped comparison allowance. Preserve the consumed failed control, with no hourly progression until all short-control gates pass. This changes evidence retention only, not customer retries, latency/error gates, application code, admission capacity, financial transactions or topology. ADR0228 observation behavior is extended; correctness decisions remain in force.

## Alternatives

Raise waiting limits immediately, increase database connections, or infer occupancy from one-second samples. These do not distinguish global admission, role admission, native pool saturation or retained timeout positions at the moment of failure. An unrestricted log export risks secrets and unnecessary payloads.

## Consequences

One bounded diagnostic read per API after the offered window adds no per-request work. Logs may be truncated or rotated; incomplete capture is reported rather than treated as proof. A clean control cannot prove an intermittent error fixed. Subsequent correction and any new paid stage require a fresh declared identity and an ADR reflecting the captured evidence.

## Failure and recovery

Reject foreign, replaced or missing namespace/pod identities. Partial capture retains successful receipts and reports missing pods. Capture failure cannot suppress the original customer result or skip restoration and UID-bound deletion. The private log receipt is never published with credentials. Preserve failed measurement reports byte for byte.

## Validation evidence

Independent recovery of adr0151-c5edbd09ed73 passed all ten gates, reconciled 25,199 payments/tickets and one unpaid expired order, verified zero duplicates and empty queues. Local ownership, bounded parsing and cleanup tests must execute before a fresh diagnostic control. No new performance improvement or hourly qualification is claimed.

Local validation: 276 affected capture, lifecycle, transport, recovery and transition tests passed in 4.59 s. Ruff and repository naming checks passed. One initial cohort had 239 passes and one outdated literal transport-bound assertion; corrected before qualification. No new cloud load started at this checkpoint.

Cloud validation: the diagnostic control retained four global-limit snapshots, each with shared used=12, general=7, payment=5 and retained=0. Every pod log read reached its byte ceiling, so capture_incomplete=true. It had 22 customer errors; snapshots do not explain every error. The failed report and independent final recovery are retained unchanged. The evidence supports the single acquisition-headroom hypothesis in ADR0234; no complete log coverage is claimed.
