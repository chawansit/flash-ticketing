# Ticket throughput implementation handoff

Status (2026-10-19): ADR0353 pair CLOSED. Control adr0151-cc8e2534f4ee PASSED_RESTORED (48000 journeys, zero drops/errors,158.223 conservative tickets/s). Candidate adr0151-37fea92d24d2 FAILED_RESTORED on creator-clock uncertainty before paid dispatch; native safety and financial/post-TTL checks passed. Both fully restored. No active allowance, lock or automatic rerun. New ADR0354 clock-observation handling is locally prepared; no new paid approval exists.

Read [latest pair](cce/replacement-rds-timing-160-pair-result-2026-10-19.json) and [clock readiness](cce/creator-clock-observation-readiness-2026-10-19.json) first. Failed clock sample timings were not retained, so historical RTT versus drift is unknown. New source retains each raw sample and allows at most three fresh read-only probes with unchanged two-second offset/five-second RTT/strict freshness bounds. Malformed/transport errors fail immediately.117 unique checks passed across initial/corrected runs; Ruff, both actual preparation paths and isolated control reservation passed. No cloud load rerun. Private prepared pair/goal/admission and seven compiled fresh templates use tmp/adr0354- prefix. Timing remains ADR0353 (7200s pair/3600s control); exact replacement snapshot remains tmp/adr0352-new-rds. Fresh explicit approval and current registry/live preflight required for a new pair. Never run consumed ADR0353 templates/reset its sequence. Its original clock/report hashes remain preserved in tmp/adr0353-sequence.approved.private.json. New source contract is in the clock readiness receipt.

## Read first

- [Previous pair result](cce/replacement-rds-160-pair-result-2026-10-18.json).
- [Passed new-RDS control](cce/replacement-rds-160-control-result-2026-10-18.json).
- [Candidate pre-dispatch stop](cce/replacement-rds-160-candidate-result-2026-10-18.json).
- [Prior preparation readiness](cce/combined-observer-new-rds-readiness-2026-10-18.json).
- [Replacement RDS cutover](cce/rds-endpoint-switch-2026-10-18.json).
- [Latest stopped control](cce/combined-confirmation-160-control-result-2026-10-17.json).
- ADR0350 (observer fix), ADR0351 (RDS switch), ADR0352 (exact new-RDS preparation binding).

Work in E:/MVP Project, branch codex/customer-recovery-2026-10-10. Read AGENTS.md. Goal remains verified paid-and-issued five-minute throughput toward300/s. The new-RDS160/s control passed; candidate load and300/s remain unmeasured.

## Current RDS and live state

User already restored the old database and requested bringing services online. Live authority is10.1.21.48 via its supplied private DNS and verify-full. Supplied CA bundle hash09ae697f83a10a275bf576bfb5a5ecea8d3d387217059f48e66b49b7622a2323. Existing ticketing runtime role/credentials retained. Both deployment .env.rds files updated with private backups; current pooler/recovery files are /root/rds-switch-20261018c on API ECS. Old RDS untouched and original pooler retained stopped. Do not route back after new writes without reconciliation.

Cutover checked both old/new migration checksums (seven),20 public tables and matching key-table estimates. Exact count attempts hit a bounded timeout; no full reconciliation is claimed. Four APIs and Kafka healthy, load-balancer readiness HTTP200. Final preparation recapture verified normal topology, generator idle and drained global queues. Diagnostic adapter on new RDS passed full visibility, verify-full, one read-only connection and credential cleanup. No fixture or capacity traffic was started in preparation.

## Completed observer correction

ADR0345 bundle had four writers but cce_writer_scale_verification.selected omitted the label, invoking historical three-writer startup/resource/pipeline gates. ADR0350 adds only the exact validated combined label to existing four-writer gates; local arm/image/source/count/resource/process proofs remain mandatory. First pipeline row is now retained privately before startup validation.

Initial broad run301 passed,5 failed,2 skipped. Fixed three Windows path-matching errors in new negative tests and refreshed the declared reproduction overlay manifest for two failed reproduction checks. Final correction run42 passed. Across runs306 distinct regressions passed. Actual22-helper package passed separately in both actual immutable images with network disabled; retained inventory timestamp is explicitly synthetic structural replay. Windows symlink case remains unavailable. Ruff and actual published-image plan/manifests/binding/control reservation in isolated bookkeeping passed. No fake control was used to grant candidate admission.

Logs: tmp/adr0352-regressions.log, adr0352-corrections-final.log, adr0352-actual-images.log, adr0352-ruff-final.log, adr0352-final-preparation.log, adr0352-live-capture-final.log, adr0352-diagnostic-preflight.log.

## Consumed execution

User explicitly approved160/s x300s control on new RDS, then candidate only after fully passed/restored control. Fresh preflight passed; same source contract890a80df8fbced0083a31f78a5777b04dc7b7fb78e4797ebc3af529294dc3c39 and one3600s pair deadline were retained.

Control adr0151-72c681a1e054:48000 scheduled/dispatched/fulfilled,0 drops,3 initial/3 recovered/0 final failures;47469 conservative in-window tickets=158.23/s. Worst-shard journey p95 including recovery3604.24ms. All measurement, existing database financial/post-TTL, global drain and restoration gates passed. Retained-evidence audit also passed; it is not a new database reconciliation.

Candidate adr0151-d36daa7833d2 was admitted only after that exact passed/restored control. Both safety cohorts and corrected observer startup passed. At cce_paid_lifecycle.run -> observers.launch_cpu -> stage.check(900), the remaining shared pair time was below900s. PAIR_TIME_BUDGET_EXHAUSTED occurred before customer dispatch;0 paid stages started. No candidate throughput/latency/failure-count comparison exists. This is a failed attempt to qualify within the envelope, not a failed160/s load measurement. Candidate integrity, drain, runtime/namespace/resource/credential cleanup all passed. Do not run a paid-load retained audit against a no-paid report; its financial scope is safety cohorts only.

- Exact pointers and raw hashes are in the two compact result files and tmp/adr0352-sequence.approved.private.json. The sequence is closed, with original clock/start/report pointers preserved.
- Raw reports: tmp/adr0151-72c681a1e054/cce-comparison.private.json and tmp/adr0151-d36daa7833d2/cce-comparison.private.json. Do not rewrite them.
- tmp/adr0352-authorize-control.py, control-execute.py, authorize-candidate.py and candidate-execute.py are CONSUMED templates. Do not execute again or reset their sequence/approval receipt.
- New-RDS restoration inputs remain tmp/adr0352-new-rds/snapshot.private.json, config.private.json, diagnostic-target.private.json. scripts/cce_replacement_rds_binding.py pins exact old/new snapshots and target/CA changes.
- Control retained audit: tmp/adr0352-control-retained-audit.json. Detailed execution logs: tmp/adr0352-control-execution.log and candidate-execution.log. Use scripts/capacity_run_brief.py and named bounded reads.

Live goal metadata closed CONSUMED_FAILED_RESTORED; actual journal entries retain control PASSED_RESTORED and candidate FAILED_RESTORED. No clock extension, infrastructure resize, relaxed correctness gate or automatic retry occurred. Fresh SWR credentials are in protected Docker storage; never put credentials in a handoff or prompt.

Historical ADR0353 preparation and its approved execution are retained in ADR0353 and the latest pair result above. The two-hour budget was sufficient; candidate stopped on creator-clock uncertainty, not time budget. Both consumed reports and original clocks remain immutable. Use only the fresh ADR0354 preparation for any future separately approved pair.

## Images and work envelope

Control remains immutable ADR0324 manifest4efa10e301666db5aebc58a3a218df8987307c310c55936ba1c4e3b13de921b2. Combined candidate remains ADR0346 manifest b114f6b15e4e07f11da19e98a82a21d910c39879b7aadf1bc96e8e11a74595ba, source19da6fb554d1e592e3eba59a52427b10510c213ab62c2a80181b8d5d27123925. Registry receipt cce/callback-write-pipeline-registry-2026-10-17.json. Reproducible preparer scripts/prepare_callback_write_pipeline_image.py uses immutable parent and selected reviewed files, preserving parent synchronous auth/payment initiation. Do not copy unrelated checkout drift into the image.

Candidate combines simulator atomic claim/transaction pipeline with API-only default-off PAYMENT_CALLBACK_WRITE_PIPELINE=1. Locks, fresh post-lock clock/expiry, financial/outbox writes, receipt IDs and durable commit remain. Local callback median13.3175 to10.3392ms (22.364% lower), not a measured cloud gain. Application image unchanged by observer/RDS preparation.

Fixed:4API1vCPU/2Gi,3general+3payment clients each;15workers250m/512Mi;24physical backends/no reserve; shared admission20/500ms acquire;16simulator slots/10DB clients; generator4processes/800active/500HTTP connections/16clients. Cache1s/poll500ms/lease15s unchanged. Require zero generator drops, existing latency gates, at most0.01%final failures, no duplicate charges/double-booking/payment loss, full financial/drain/restoration audits. Use existing runner audits; local evidence checks are not database reconciliation.

## Retained history

Prior stopped control adr0151-d5123df337b4 / bounded_cce_ticket_target__08647b663a64 stopped before paid dispatch on observer diagnostics, FAILED_RESTORED. Both safety cohorts passed financial/post-TTL checks; normal runtime, queues, integrity and cleanup passed. Raw tmp/adr0151-d5123df337b4/cce-comparison.private.json, hashef212c5b9e8af7bac6e33ecc37d51a8a355fa302b2536cce1963d9b32d959c38. No candidate/throughput measurement. Prior allowance closed CONSUMED_FAILED_RESTORED; old sequence tmp/adr0349-sequence.approved.private.json is unchanged.

Earlier no-paid stops adr0151-5363d2b5b94c and adr0151-d47e3c15103e remain retained/restored. Details in cce/combined-confirmation-control-pre-dispatch-result-2026-10-17.json and combined-confirmation-fresh-pre-dispatch-result-2026-10-17.json plus its independent recovery. Never reset them.

Historical best passing160/s (old RDS):48,000 dispatched/confirmed,0 drops/errors;47,419 conservative in-window tickets=158.0633/s. Raw tmp/adr0151-c83348917efc/cce-comparison.private.json. Historical180/s control failed:54,000 scheduled,41,965 dispatched,12,035 drops,1,697 initial failures,1,695 recovered,2 final;40,952 conservative=136.5067/s. All41,965 independently reconciled and restored; raw tmp/adr0151-a0d63f35960f/cce-comparison.private.json. Those remain orchestration references; they do not qualify the replacement RDS.

Preserve user edits in tests/unit/test_cce_ecs_transition.py, test_cce_hourly_generator.py, test_cce_paid_observers.py, .codex-remote-attachments/ and outputs/. Do not stage/revert them. Reviewed owned changes may be committed/pushed to this branch. No main merge. Prefer named bounded reads and scripts/capacity_run_brief.py; no recursive temporary-artifact searches, full cumulative state output or credential printing. Serialize owned JSON with LF.
