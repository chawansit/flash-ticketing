# ADR0343: Qualify the payment claim cutoff at 180 offered journeys/s

## Status
The user explicitly selected 180/s control, then 180/s candidate only if the fresh control passes. Completed: the fresh control failed quality and was independently reconciled/restored. The conditional candidate was not run. Extends ADR0341/ADR0342; supersedes their lack of paid authorization only for this bounded conditional sequence. No financial or correctness decision is superseded.

## Context
The ADR0341 candidate is registry-qualified at manifest sha256:730bff63781ccffa1aa8ab081e5ab73621d01ea86269d4e6aa74846857e57ff4. Local-only ADR0342 preparation intentionally cannot dispatch. Best passing five-minute issuance remains 158.0633/s at 160 offered/s. The historical 180/s step failed quality despite full independent reconciliation/restoration; its failure remains unchanged.

## Decision
Run one fresh unchanged-image control first through the existing qualified 180/s runner. Its image remains ADR0324 manifest sha256:4efa10e301666db5aebc58a3a218df8987307c310c55936ba1c4e3b13de921b2. Candidate integration work was saved under owned tmp/adr0343-candidate-integration and removed from the control's active source paths. No unfinished candidate code enters the control source bundle.

Only after a fully passed and restored fresh control may the candidate integration be completed, qualified and given its separately accounted one-stage allowance. Bind the exact published candidate through the existing runner's source, runtime, inventory, financial and restoration paths. Never substitute a local-only receipt or relax parent safety gates. Each offered window is180/sÃ—300s, 54,000 scheduled journeys. The conditional sequence is bounded to60minutes; the protected launcher starts its sequence clock and requires at least1,200seconds remaining before control dispatch. Candidate admission must also enforce the remaining sequence budget. No automatic paid retry. No candidate allowance is opened merely because its publication passed.

Keep four 1-vCPU/2Gi APIs with 3+3 clients, fifteen 250m/512Mi background pods, 24 physical backends/no reserve, admission20 and 500ms acquisition deadline. Simulator16/10, generator4 processes/800 active journeys/500 HTTP connections/16 clients, gateway delays, recovery and freshness remain unchanged. No resize or main merge.

Persistence/locking, messaging, idempotent callbacks, 15-second leases and cache TTL behavior remain ADR0341/ADR0338. Only the reviewed due-time predicate differs in application bytes. Bootstrap/restoration preserve the exact original service snapshots. Scaling is fixed.

## Alternatives
Run the local-only proposal directly: rejected because it lacks live proofs and paid admission. Bypass image/source verification: rejected. Increase resources or alter latency/correctness: outside approval. Automatically rerun a failed paid stage: rejected.

## Failure and recovery behavior
Fail closed on wrong image/source/configuration, missing approval, consumed allowance, snapshot drift or incomplete readiness. A failed control prevents a candidate if a control sequence is selected. Existing database financial/post-TTL audits, queues and exact restoration are mandatory even after failed load quality. Preserve raw evidence and consumed counters on observer/transport failure; reconcile independently before claiming restoration.

## Validation evidence
Executed the actual control preparation path in isolated envelope/journal/state copies, using the real retained image proof, protected configuration and saved service model: API and15-worker manifests, binding and reservation passed without publication mocks. Assertions verified3+3 API slots, simulator16/10 and unchanged control image. Real live ledger/goal hashes were unchanged by this qualification. The original source reproduction verifier passed535 historical files and31 current overlays. The protected launcher compiled and its help path executed; existing kubeconfig material parsed. Those preparation checks started no cloud service or paid traffic; the subsequent approved control is recorded below.

The fresh live goal initially recorded one control allowance; the completed control consumed it. The candidate remains conditional and unstarted. Existing runner financial/post-TTL audits and cleanup are reused, not replaced. Protected ECS and diagnostic database passwords were not retained from the previous execution; the launcher accepts hidden terminal input or an explicitly provided private session-file path and retrieves registry credentials privately from the existing helper. No credential values are retained in the handoff. [Compact control preparation receipt](../capacity/cce/claim-cutoff-180-control-prepared-2026-10-17.json).

Candidate live integration is parked and not locally qualified; its exploratory lint findings are not passing checks. The candidate remains cloud-unmeasured, and no throughput improvement is demonstrated. Best passing remains158.0633 paid-issued tickets/s.

## Executed control and sequence closure
The approved180/s×300s unchanged-image control ran once:54000 scheduled,41965 dispatched,12035 generator drops,1697 initial error journeys,1695 recovered and2 final customer failures; worst-shard journey p95=6973.42ms. Conservative in-window issuance40952/300=136.5067/s. The zero-drop and complete-accounting gates failed. The candidate condition was not met, no candidate allowance was opened and the sequence stopped without a paid retry.

The original strict financial target54000 failed and is preserved. A separate executed repeatable-read read-only database audit reconciled all41965 dispatched purchases with41965 durable payments/unique tickets, zero double-booking/payment loss, both safety cohorts, callbacks and elapsed TTL. Exact original runtime remained stable before/after the audit, queues empty, generator idle and owned namespace/helpers absent. No data mutation or paid traffic occurred during reconciliation. Operational status is FAILED_RESTORED; failed quality and raw result hash are retained. API CPU3.55/4cores, mean acquisition24.26ms and payment pickup3.411s indicate pressure; this unchanged-image sample does not isolate its cause.

[Compact result](../capacity/cce/claim-cutoff-180-control-result-2026-10-17.json) and [independent reconciliation/restoration](../capacity/cce/claim-cutoff-180-control-recovery-2026-10-17.json). Best passing record remains158.0633/s. No further paid run, resize or gate relaxation is authorized.
