# Ticket throughput implementation handoff

Goal: improve **verified paid-and-issued tickets/s over five minutes**, toward 300/s. An offered journey is not a completed transaction. Work in `E:/MVP Project`, branch `codex/customer-recovery-2026-10-10`. Read `AGENTS.md` first. No cloud experiment is active. The user now explicitly approved180/s control, then180/s candidate only if the fresh control passes; see ADR0343 below. No automatic paid retry or resize.

## Current evidence

| Test | Offered | Dispatched and confirmed | Drops | Initial errors recovered | Final customer failures | Conservative tickets/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| ADR0338 passing reference | 160/s × 300s | 48,000 | 0 | 0 | 0 | **158.0633** |
| ADR0339 failed step | 180/s × 300s | 42,855 | 11,145 | 892 | 0 | **139.43** |

Read only these result files initially:

- `docs/capacity/cce/api-client-pool-headroom-result-2026-10-17.json`
- `docs/capacity/cce/qualified-pools-180-result-2026-10-17.json`

For ADR0339, incomplete slot-history observation skipped the normal terminal audit. Separate read-only reconciliation verified 42,855 payments/unique tickets, zero double-booking/payment loss, callbacks, elapsed TTLs and empty queues. Exact original services were restored; owned CCE test resources removed and generator idle. Operational status is FAILED_RESTORED; **the load result remains failed**. Recovery evidence: `docs/capacity/cce/qualified-pools-180-recovery-2026-10-17.json`.

## What to investigate next

Local correction completed under [ADR0341](../adr/0341-bound-payment-claim-eligibility.md): only the simulator due-at predicate changes from volatile `clock_timestamp()` to `statement_timestamp()`, allowing the existing partial index to bound the scan. Lease clocks, locks, token-fenced ack, callback IDs, polling and cache freshness remain unchanged. [Original local receipt](cce/payment-claim-cutoff-local-2026-10-17.json) pins the exact ADR0324 parent and sole changed module. The reproducible candidate is now registry-qualified as documented below, remains cloud-unmeasured and does not replace the passing reference.

Executed focused checks: **20 passed**, including five isolated native PostgreSQL tests of actual claim/delivery/ack behavior and a future-heavy query-plan comparison (old filter visits at least20,000 future rows; candidate index bound filters zero). Ruff passed. Existing image preparation/installer and a network-disabled candidate run verified22 runtime modules and imported worker identity. ADR0339 retained-evidence audit passed; no new database reconciliation or cloud call occurred. Owned local logs/recipe: `tmp/adr0341-tests.log`, `tmp/adr0341-image-build.log`, `tmp/adr0341-prepare.py`.

API CPU rose 2.40→3.55 cores out of four; acquisition mean 11.00→21.34ms. Payment pickup rose 0.105→3.146s; callback delivery 60.92→76.74ms. Cache misses rose 48,759→138,178 despite fewer completed journeys. Writer command-age mean stayed approximately stable at 1.56→1.51s. These observer-lifetime comparisons suggest confirmation backlog and status-read amplification; a single causal holder is not yet proven.

The corrected scan is a proven local inefficiency; its share of cloud confirmation backlog and status-read amplification is unknown, especially when most pending attempts are already due or leased. The user superseded the160/s proposal with180/s control then conditional180/s candidate at the same budget, stopping after failed control. Compare pickup delay, misses/dispatched journey, API CPU/acquisition and conservative issuance under unchanged gates and existing financial/drain/restoration audits. The new approval is limited to this conditional sequence.

## Tested configuration and boundaries

Four CCE API pods, each1vCPU/2Gi and3general+3payment clients; fifteen background pods;24physical PgBouncer backends/no reserve; shared admission20/API; acquisition deadline500ms. Simulator16delivery jobs/10DB clients. Generator4processes,800active journeys,500HTTP connections,16clients. Keep gateway delays/recovery/freshness unchanged. App image manifest: `sha256:4efa10e301666db5aebc58a3a218df8987307c310c55936ba1c4e3b13de921b2`.

Keep current latency gates, zero generator drops, at most0.01%final customer failures, zero duplicate charges/double-booking/payment loss and full drain. One experiment≤60minutes; no cumulative time cap. No resize/new spending without approval. Reviewed code/evidence may be pushed to this branch; main merge requires approval. Do not contact the retired secondary ECS. Credentials are deliberately omitted; use authorized private session material and never commit it.

## Small read and execution set

Use `scripts/capacity_run_brief.py --help` and `docs/capacity/QUOTA_WORKFLOW.md`. Existing cloud runner: `scripts/run_cce_paid_comparison.py`; financial audits: `scripts/cce_bounded_financial_audit.py`. Explicit run paths: passing `tmp/adr0151-c83348917efc`; failed paid `tmp/adr0151-fa8e65219b30`. Do not recursively scan `tmp` or read the full cumulative state/ADR corpus. Load only the relevant source section when needed.

Preserve pre-existing user edits in `tests/unit/test_cce_ecs_transition.py`, `test_cce_hourly_generator.py`, `test_cce_paid_observers.py`, `.codex-remote-attachments/` and `outputs/`. Do not stage, revert or overwrite them. No hourly or300/s qualification is claimed by these short tests.

Preparation continuation: use tracked scripts/prepare_payment_claim_cutoff_image.py --build --receipt tmp/NEW_PRIVATE_RECEIPT.json, not the old temporary recipe. Four wrapper tests and a fresh22-module network-disabled build passed; payment-claim-cutoff-reproducible-local-2026-10-17.json retains the new image identity and the exact same runtime source map as the original candidate.

Latest local binding: [ADR0342](../adr/0342-bind-local-payment-claim-comparison.md) and [sanitized receipt](cce/payment-claim-binding-local-2026-10-17.json). Existing runner entry point `prepare_payment_claim_proposal` reuses its API/worker builders:19 binding checks and109 existing compatibility cases passed; the exact candidate image validated all8 generated role configurations with networking disabled. This uses a synthetic fixture; live snapshot reconciliation and live candidate registration are incomplete. Local-only dispatch is rejected.

Publication approved and complete: [registry receipt](cce/payment-claim-cutoff-registry-2026-10-17.json). The exact user-approved candidate was published to the specified existing repository after permission, local identity and unused-tag checks. Immutable manifest `sha256:730bff63781ccffa1aa8ab081e5ab73621d01ea86269d4e6aa74846857e57ff4` matched registry bytes and the pulled local image;22-module verification passed with networking disabled. Credentials are omitted. Registry-qualified, not cloud-measured. Publication itself consumed no paid allowance and changed no deployed service; the subsequent180/s approval is recorded below.

Current approved sequence: [ADR0343](../adr/0343-qualify-payment-claim-cutoff-at-180.md), [compact preparation](cce/claim-cutoff-180-control-prepared-2026-10-17.json). One fresh180/s300s control allowance is registered and unconsumed. Actual unmocked isolated admission passed, and the frozen reproduction verifier passed535 files. Control uses the unchanged4efa... image; candidate integration is parked in `tmp/adr0343-candidate-integration` and unqualified. Protected launcher: `tmp/adr0343-control-execute.py`; sequence state: `tmp/adr0343-sequence.approved.private.json`. Launch awaits hidden ECS/database input or an explicitly provided protected session-file path; never copy secret values into chat or handoff. Kubeconfig parsed. No paid stage has started. If control fails, stop and reconcile/restore; do not launch candidate or rerun control. Only after a fully passed/restored fresh control may candidate registration/qualification and the one conditional candidate allowance proceed within the remaining60-minute sequence budget.
