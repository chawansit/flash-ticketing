# Ticket throughput implementation handoff

Status (2026-10-18): observer correction and replacement-RDS preparation are complete. User requested readiness notification before paid traffic. **Ready for fresh160/s x300s control approval, then conditional candidate only after fully passed/restored same-pair control. No live allowance, sequence, pair clock, lock or paid stage is open.**

## Read first

- [Current readiness](cce/combined-observer-new-rds-readiness-2026-10-18.json).
- [Replacement RDS cutover](cce/rds-endpoint-switch-2026-10-18.json).
- [Latest stopped control](cce/combined-confirmation-160-control-result-2026-10-17.json).
- ADR0350 (observer fix), ADR0351 (RDS switch), ADR0352 (exact new-RDS preparation binding).

Work in E:/MVP Project, branch codex/customer-recovery-2026-10-10. Read AGENTS.md. Goal remains verified paid-and-issued five-minute throughput toward300/s. No300/s or new-RDS throughput measurement has occurred.

## Current RDS and live state

User already restored the old database and requested bringing services online. Live authority is10.1.21.48 via its supplied private DNS and verify-full. Supplied CA bundle hash09ae697f83a10a275bf576bfb5a5ecea8d3d387217059f48e66b49b7622a2323. Existing ticketing runtime role/credentials retained. Both deployment .env.rds files updated with private backups; current pooler/recovery files are /root/rds-switch-20261018c on API ECS. Old RDS untouched and original pooler retained stopped. Do not route back after new writes without reconciliation.

Cutover checked both old/new migration checksums (seven),20 public tables and matching key-table estimates. Exact count attempts hit a bounded timeout; no full reconciliation is claimed. Four APIs and Kafka healthy, load-balancer readiness HTTP200. Final preparation recapture verified normal topology, generator idle and drained global queues. Diagnostic adapter on new RDS passed full visibility, verify-full, one read-only connection and credential cleanup. No fixture or capacity traffic was started in preparation.

## Completed observer correction

ADR0345 bundle had four writers but cce_writer_scale_verification.selected omitted the label, invoking historical three-writer startup/resource/pipeline gates. ADR0350 adds only the exact validated combined label to existing four-writer gates; local arm/image/source/count/resource/process proofs remain mandatory. First pipeline row is now retained privately before startup validation.

Initial broad run301 passed,5 failed,2 skipped. Fixed three Windows path-matching errors in new negative tests and refreshed the declared reproduction overlay manifest for two failed reproduction checks. Final correction run42 passed. Across runs306 distinct regressions passed. Actual22-helper package passed separately in both actual immutable images with network disabled; retained inventory timestamp is explicitly synthetic structural replay. Windows symlink case remains unavailable. Ruff and actual published-image plan/manifests/binding/control reservation in isolated bookkeeping passed. No fake control was used to grant candidate admission.

Logs: tmp/adr0352-regressions.log, adr0352-corrections-final.log, adr0352-actual-images.log, adr0352-ruff-final.log, adr0352-final-preparation.log, adr0352-live-capture-final.log, adr0352-diagnostic-preflight.log.

## Prepared execution and remaining human gate

- tmp/adr0352-pair.prepared.private.json: matching published control/candidate manifests and new-RDS binding, unstarted pair.
- tmp/adr0352-control-goal.prepared.private.json and control-admission-qualified.json: exact prepared control and isolated admission proof.
- tmp/adr0352-new-rds/snapshot.private.json, config.private.json, diagnostic-target.private.json and live-preparation.private.json: replacement inputs. Static snapshot hashes and exact allowed DB/CA changes are enforced by scripts/cce_replacement_rds_binding.py.
- tmp/adr0352-authorize-control.py, control-execute.py, authorize-candidate.py and candidate-execute.py: compiled only. Wrappers use the replacement target and normal snapshot; no old-RDS restoration input.
- Current source contract890a80df8fbced0083a31f78a5777b04dc7b7fb78e4797ebc3af529294dc3c39; prepared control goal d62ff6d47197f9b4f4cc35bfa3f336e4b42c7d80fb9779b5ae25dd7ff26dbafd. Recheck before use.

User supplied refreshed SWR login after stored credentials were rejected. Protected Docker login and both immutable manifest/config reads passed; no publication needed. tmp/adr0352-registry-qualified.json records time; authorizer requires refresh if over one hour old. Never include credentials in a handoff or prompt.

**Do not execute the authorizers yet.** This user request was preparation and notification, not paid-run approval. Future explicit approval is recorded as tmp/adr0352-fresh-pair-user-approval.private.json with exactly approved=true, rate=160, seconds=300, database=10.1.21.48, control_then_conditional_candidate=true. It does not exist. Then recheck current source/live readiness/registry, authorize once, and use an ephemeral private credential input consumed/deleted by the wrapper. One fresh journal boundary and shared3600s pair deadline, assigned at approval; never reset consumed pairs. Candidate authorizer requires exact same-pair PASSED_RESTORED control, matching source/settings/new-RDS binding and original remaining deadline. No automatic retry, resize, gate relaxation or main merge.

## Images and work envelope

Control remains immutable ADR0324 manifest4efa10e301666db5aebc58a3a218df8987307c310c55936ba1c4e3b13de921b2. Combined candidate remains ADR0346 manifest b114f6b15e4e07f11da19e98a82a21d910c39879b7aadf1bc96e8e11a74595ba, source19da6fb554d1e592e3eba59a52427b10510c213ab62c2a80181b8d5d27123925. Registry receipt cce/callback-write-pipeline-registry-2026-10-17.json. Reproducible preparer scripts/prepare_callback_write_pipeline_image.py uses immutable parent and selected reviewed files, preserving parent synchronous auth/payment initiation. Do not copy unrelated checkout drift into the image.

Candidate combines simulator atomic claim/transaction pipeline with API-only default-off PAYMENT_CALLBACK_WRITE_PIPELINE=1. Locks, fresh post-lock clock/expiry, financial/outbox writes, receipt IDs and durable commit remain. Local callback median13.3175 to10.3392ms (22.364% lower), not a measured cloud gain. Application image unchanged by observer/RDS preparation.

Fixed:4API1vCPU/2Gi,3general+3payment clients each;15workers250m/512Mi;24physical backends/no reserve; shared admission20/500ms acquire;16simulator slots/10DB clients; generator4processes/800active/500HTTP connections/16clients. Cache1s/poll500ms/lease15s unchanged. Require zero generator drops, existing latency gates, at most0.01%final failures, no duplicate charges/double-booking/payment loss, full financial/drain/restoration audits. Use existing runner audits; local evidence checks are not database reconciliation.

## Retained history

Latest control adr0151-d5123df337b4 / bounded_cce_ticket_target__08647b663a64 stopped before paid dispatch on observer diagnostics, FAILED_RESTORED. Both safety cohorts passed financial/post-TTL checks; normal runtime, queues, integrity and cleanup passed. Raw tmp/adr0151-d5123df337b4/cce-comparison.private.json, hashef212c5b9e8af7bac6e33ecc37d51a8a355fa302b2536cce1963d9b32d959c38. No candidate/throughput measurement. Prior allowance closed CONSUMED_FAILED_RESTORED; old sequence tmp/adr0349-sequence.approved.private.json is unchanged.

Earlier no-paid stops adr0151-5363d2b5b94c and adr0151-d47e3c15103e remain retained/restored. Details in cce/combined-confirmation-control-pre-dispatch-result-2026-10-17.json and combined-confirmation-fresh-pre-dispatch-result-2026-10-17.json plus its independent recovery. Never reset them.

Historical best passing160/s (old RDS):48,000 dispatched/confirmed,0 drops/errors;47,419 conservative in-window tickets=158.0633/s. Raw tmp/adr0151-c83348917efc/cce-comparison.private.json. Historical180/s control failed:54,000 scheduled,41,965 dispatched,12,035 drops,1,697 initial failures,1,695 recovered,2 final;40,952 conservative=136.5067/s. All41,965 independently reconciled and restored; raw tmp/adr0151-a0d63f35960f/cce-comparison.private.json. Those remain orchestration references; they do not qualify the replacement RDS.

Preserve user edits in tests/unit/test_cce_ecs_transition.py, test_cce_hourly_generator.py, test_cce_paid_observers.py, .codex-remote-attachments/ and outputs/. Do not stage/revert them. Reviewed owned changes may be committed/pushed to this branch. No main merge. Prefer named bounded reads and scripts/capacity_run_brief.py; no recursive temporary-artifact searches, full cumulative state output or credential printing. Serialize owned JSON with LF.
