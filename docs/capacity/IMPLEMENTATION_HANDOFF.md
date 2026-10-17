# Ticket throughput implementation handoff

Goal: improve verified paid-and-issued tickets/s over five minutes toward300/s. Work in E:/MVP Project, branch codex/customer-recovery-2026-10-10. Read AGENTS.md first. **No cloud experiment, lock, agent stop marker or unresolved recovery is active.** The latest approved fresh control was stopped before safety/paid dispatch. Its operational recovery is closed; no candidate ran and no new throughput was measured. No automatic cloud repeat or resize.

## Read first

- [Latest stopped control](cce/combined-confirmation-fresh-pre-dispatch-result-2026-10-17.json).
- [Independent restoration recovery](cce/combined-confirmation-fresh-pre-dispatch-recovery-2026-10-17.json).
- [Qualified transferred-helper correction](cce/combined-observer-helper-local-2026-10-17.json).
- Passing160/s reference only when needed: cce/api-client-pool-headroom-result-2026-10-17.json.

Best passing offered load remains160/s×300s:48,000 dispatched/confirmed,0 drops/errors;47,419 conservative in-window tickets=158.0633/s. Historical fresh180/s control failed:54,000 scheduled,41,965 dispatched,12,035 drops,1,697 initial failures,1,695 recovered,2 final failures;40,952 conservative tickets=136.5067/s. All41,965 dispatched purchases independently reconciled and original runtime restored; failed quality unchanged. This is not300/s qualification.

## Latest actual cloud attempt

User explicitly approved a fresh160/s control then conditional candidate after ADR0348. Run adr0151-d47e3c15103e; ledger bounded_cce_ticket_target__a03b05b78bdd. Before its paid stage, a separate network-disabled actual-image rehearsal proved the transferred worker verifier imports absent cce_simulator_roundtrip_profile. An explicitly agent-owned stop marker prevented known observer startup failure before safety or paid traffic. No human pause was requested.

Original raw report: tmp/adr0151-d47e3c15103e/cce-comparison.private.json, hash6d8592d8c4739485edbeccbd23bb7f24f3a03fcde0523727f73e99bdb0977987.0 paid stages,0 safety fixtures and no paid window. Runtime restoration passed; original integrity verification stayed false/RECOVERY_REQUIRED because the stop interrupted final verification. Separate read-only exact before/after runtime, queues, generator, namespace and owned-container checks passed. Only operational recovery was closed FAILED_RESTORED; raw/hash unchanged. Agent-owned marker removed and no active scope/lock remains. Do not describe this as a failed160/s load measurement or a passed financial load audit.

Stopped sequence: tmp/adr0348-sequence.approved.private.json, candidate_permitted false. It was NOT reset. Earlier ADR0347 run adr0151-5363d2b5b94c also stopped before paid dispatch, after both safety financial cohorts passed; inventory renewal rejected the ADR0345 worker role adapter. Its compact result is cce/combined-confirmation-control-pre-dispatch-result-2026-10-17.json, raw hashd5d702386ebee31e91307985631d27b5cee543b0e5ef199a7ef44332edc95949. Both stopped pairs remain retained.

## Implemented application candidate

[ADR0346](../adr/0346-reduce-successful-callback-write-waits.md) combines ADR0344 simulator atomic claim/claim-ack pipeline with default-off PAYMENT_CALLBACK_WRITE_PIPELINE for independent writes after successful BOOK. Financial lock order, fresh post-lock clock/expiry, callback IDs, exact financial/outbox writes and durable commit remain. Local callback median13.3175→10.3392ms (22.364% reduction);29 native/preparer checks passed. This excludes cloud contention/HTTP delay and is not a measured cloud gain.

Exact image is already published after explicit user approval: [registry receipt](cce/callback-write-pipeline-registry-2026-10-17.json), manifest sha256:b114f6b15e4e07f11da19e98a82a21d910c39879b7aadf1bc96e8e11a74595ba; source19da6fb554d1e592e3eba59a52427b10510c213ab62c2a80181b8d5d27123925. Pulled22-module verification passed. No additional publication is needed for unchanged bytes.

Reproducible preparer: scripts/prepare_callback_write_pipeline_image.py --build --receipt tmp/NEW_PRIVATE_RECEIPT.json. Immutable ADR0324 parent with only reviewed worker/callback/config/wiring changes. Preserve parent synchronous authentication/payment-initiation; do not copy unrelated checkout drift. Original ADR0344/0345 images/receipts stay unchanged.

At16delivery slots,300/s needs mean occupied cycle≤53.333ms. Retained78.231ms successful delivery would conditionally cap delivery alone near204.5/s even with zero claim/ack. Observer-lifetime comparisons are diagnosis, not measured capacity. API/backend necessary budgets are13.333CPUms and80backend-ms per journey;800active journeys require mean journey≤2.667s. None alone proves300/s.

## Corrected preparation and observer package

[ADR0347](../adr/0347-qualify-combined-confirmation-path.md) binds the distinct combined comparison; [ADR0348](../adr/0348-bind-combined-worker-inventory-renewal.md) fixes exact pair-bound15-worker inventory renewal and retains worker proofs before rejection. [ADR0349](../adr/0349-seal-transferred-combined-worker-verification.md) makes transferred worker verification portable with static reviewed control/candidate manifest/source pins.

Local admission still validates the active exact arm/pair. Remote observation accepts only independently pinned immutable identities from a hash-sealed admitted bundle; all schema/source/image/UID/IP/process/command/environment/resource checks remain. It no longer imports local authority/profile files. New scripts/cce_combined_worker_identity.py is included in transferred helpers, source binding and historical reproduction. Worker role adapter label remains ADR0345; experiment selector is ADR0347. Common ECS bootstrap/restoration remains immutable ADR0324.

Executed267 broad regressions passed,2 skipped (explicit Docker case rehearsed separately; Windows symlink unavailable);17 focused checks passed with overlapping cases. Exact22-file helper selection/upload captured from actual observer.start with offline sessions. Both actual immutable images passed network-disabled4API/15worker sealed bundle verification, outer observer entry/CLI/hash rejection/adapter installation and retained cloud inventory structural replay with explicitly synthetic fresh timestamp. This does not run DB/network observations or financial load. Ruff passed. Actual plan/manifests/binding and real-publication reservation in isolated bookkeeping passed.

Current private proof paths:
- tmp/adr0349-full-helper-package-qualified.json; logs tmp/adr0349-full-helper-test.log and tmp/adr0349-focused-tests-final.log.
- tmp/adr0349-pair-qualified.private.json and tmp/adr0349-preparation.log.
- tmp/adr0349-control-goal.prepared.private.json and tmp/adr0349-control-admission-qualified.json; log tmp/adr0349-control-admission.log.
- tmp/adr0349-independent-restoration.log and tmp/adr0151-d47e3c15103e/independent-restoration.

Optional actual-image rehearsal: tests/unit/test_cce_transferred_combined_workers.py uses QUALIFY_COMBINED_OBSERVER_HELPERS_RECEIPT (fresh owned tmp path) and QUALIFY_COMBINED_OBSERVER_INVENTORY (explicit retained inventory path). Historical fixture timestamp is synthetic for local structural replay; never present it as fresh cloud evidence.

## Next gate and execution bounds

Fresh explicit160/s×300s control→conditional candidate approval is required under stopped-pair rules; no new live allowance/run is open. Publication permission was already granted and completed. On approval, verify current source contract against the prepared proof, register a fresh pair/journal boundary and shared3600s deadline. Do not reset earlier sequences. Adapt tmp/adr0348-authorize-control.py, control-execute.py, authorize-candidate.py and candidate-execute.py to fresh private paths. Candidate requires exact same-pair fully passed/restored control, matching sources/configuration and original deadline. Stop on failure; no automatic paid rerun, resize, correctness/freshness relaxation or main merge.

Resources remain four1vCPU/2Gi API pods with3general+3payment clients;15workers250m/512Mi;24physical PG backends/no reserve; shared admission20 and500ms acquisition;16simulator delivery slots/10DB clients; generator4processes/800active journeys/500HTTP connections/16clients. Cache1s/poll500ms/lease15s and gateway/recovery behavior unchanged. Zero generator drops, existing latency gates, at most0.01%final customer failures, zero duplicate charges/double-booking/payment loss and full drain.160/s is the regression gate toward300/s, not the goal itself.

Use scripts/capacity_run_brief.py for compact bounded watching and retained-evidence checks; docs/capacity/QUOTA_WORKFLOW.md. Existing run_cce_paid_comparison.py performs financial/drain/restoration audits; reuse cce_bounded_financial_audit.py. Local evidence checks are not database reconciliation. For cleanup-only early stops, independently verify operational recovery and keep financial/load scope explicit. Do not print cumulative state JSON/private reports, recursively scan tmp or expose credentials. Serialize owned JSON LF to avoid Windows whitespace expansion.

Preserve user edits in tests/unit/test_cce_ecs_transition.py, test_cce_hourly_generator.py, test_cce_paid_observers.py, .codex-remote-attachments/ and outputs/. Never stage/revert them. Reviewed changes may be pushed to this branch; main merge requires approval. Do not contact the retired secondary ECS. Credentials must never enter handoff or a new-chat prompt.
