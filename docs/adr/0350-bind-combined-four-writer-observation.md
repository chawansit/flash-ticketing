# ADR0350: Bind combined four-writer observation

## Status

Accepted for local implementation and qualification. Extends ADR0347–0349; does not authorize another cloud run or supersede their stop rules.

## Context

Approved fresh control adr0151-d5123df337b4 reached observer admission, then stopped before paid dispatch. Pipeline startup reported four APIs, six consumers, no sample errors and diagnostics_ready false. The combined worker bundle uses ADR0345 with four reservation writers, but cce_writer_scale_verification.selected omits that label and delegates to the historical three-writer startup, resource-summary and pipeline rules. Prior helper qualification exercised transfer and installation without exercising these local gates.

## Decision

Route an ADR0345 worker bundle through the existing exact four-writer gates only after unchanged local validate_bundle verifies its active arm, immutable image/source identity, counts, resources and process proofs. Apply this selection consistently to startup, resource summary and final pipeline checks. Keep historical three-writer delegation unchanged. Bind the changed verifier into the combined profile source contract. Retain the first pipeline row privately before startup validation so failed readiness remains diagnosable. Exercise both actual arm bundles through these gates and the actual observer startup path, with missing metrics/counts and invalid identity rejected.

## Alternatives

Relax diagnostics, project four writers into three, or disable resource coverage: rejected. Change worker count or rebuild the application: unrelated to the harness defect. Add independent special-case startup logic: rejected because collection and summary need the same declared topology.

## Consequences

The combined topology uses the already established four-writer evidence requirements. This corrects harness admission and creates no measured throughput gain. Retained failed results and consumed pair remain unchanged.

## Persistence, locking, messaging, idempotency, TTL and scaling

No application persistence, locking, messaging, idempotency, TTL or scaling changes. Four API pods, fifteen workers, twenty-four physical database backends and sixteen simulator delivery slots remain unchanged. Local identity validation still requires the exact active pair.

## Failure and recovery

Invalid identities, missing process metrics, wrong counts, resource errors and counter resets remain failures. The completed control is FAILED_RESTORED, with no paid window and no candidate. Both safety cohorts were checked by the existing cloud runner; cleanup, queues, original runtime and integrity verification passed. No automatic retry, resize, pair reset or correctness relaxation. A fresh pair requires fresh explicit approval under ADR0347.

## Validation evidence

Implemented and locally qualified on 2026-10-18. Initial broad regression run:301 passed,5 failed,2 skipped. Three Windows path-matching mistakes in new negative tests were corrected; the other two failures identified the stale reproduction overlay manifest, which was refreshed without changing historical blobs. Final focused correction run:42 passed,0 failed. Across these runs,306 distinct regression cases passed. The explicitly skipped actual-image case was separately executed and passed against both immutable images with22 transferred helpers each; Windows symlink coverage remains unavailable. Ruff passed. Both arms exercise actual observer.start and four-writer startup, resource-summary and final pipeline gates, including missing metrics/counts, invalid identity, database-wait failure and counter-reset rejection. Actual-image replay remains network-disabled/synthetic inventory structural evidence, not cloud load.

Raw cloud result retained at tmp/adr0151-d5123df337b4/cce-comparison.private.json, canonical hash ef212c5b9e8af7bac6e33ecc37d51a8a355fa302b2536cce1963d9b32d959c38. [Compact stopped result](../capacity/cce/combined-confirmation-160-control-result-2026-10-17.json), [replacement-RDS readiness](../capacity/cce/combined-observer-new-rds-readiness-2026-10-18.json). No paid throughput was measured. Logs: tmp/adr0352-regressions.log, tmp/adr0352-corrections-final.log, tmp/adr0352-actual-images.log, tmp/adr0352-ruff-final.log.

Subsequent approved ADR0352 cloud execution confirmed corrected observer startup in both arms: four APIs, six consumers, four writers, complete diagnostics and no sample errors. Control adr0151-72c681a1e054 passed its48000-journey paid window and fully restored. Candidate adr0151-d36daa7833d2 passed observer startup but the original shared time margin stopped it at launch_cpu/check(900), before paid dispatch. Candidate fully restored; no candidate throughput comparison exists. [Pair result](../capacity/cce/replacement-rds-160-pair-result-2026-10-18.json).
