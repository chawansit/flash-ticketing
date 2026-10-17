# Ticket throughput implementation handoff

Goal: improve **verified paid-and-issued tickets/s over five minutes**, toward 300/s. An offered journey is not a completed transaction. Work in `E:/MVP Project`, branch `codex/customer-recovery-2026-10-10`. Read `AGENTS.md` first. No cloud experiment is active; the last paid allowance is consumed. Do not launch new cloud load without explicit approval.

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

Local correction completed under [ADR0341](../adr/0341-bound-payment-claim-eligibility.md): only the simulator due-at predicate changes from volatile `clock_timestamp()` to `statement_timestamp()`, allowing the existing partial index to bound the scan. Lease clocks, locks, token-fenced ack, callback IDs, polling and cache freshness remain unchanged. [Local receipt](cce/payment-claim-cutoff-local-2026-10-17.json) pins the exact ADR0324 parent and sole changed module. This candidate is built locally, unpublished and cloud-unmeasured; it does not replace the passing reference.

Executed focused checks: **20 passed**, including five isolated native PostgreSQL tests of actual claim/delivery/ack behavior and a future-heavy query-plan comparison (old filter visits at least20,000 future rows; candidate index bound filters zero). Ruff passed. Existing image preparation/installer and a network-disabled candidate run verified22 runtime modules and imported worker identity. ADR0339 retained-evidence audit passed; no new database reconciliation or cloud call occurred. Owned local logs/recipe: `tmp/adr0341-tests.log`, `tmp/adr0341-image-build.log`, `tmp/adr0341-prepare.py`.

API CPU rose 2.40→3.55 cores out of four; acquisition mean 11.00→21.34ms. Payment pickup rose 0.105→3.146s; callback delivery 60.92→76.74ms. Cache misses rose 48,759→138,178 despite fewer completed journeys. Writer command-age mean stayed approximately stable at 1.56→1.51s. These observer-lifetime comparisons suggest confirmation backlog and status-read amplification; a single causal holder is not yet proven.

The corrected scan is a proven local inefficiency; its share of cloud confirmation backlog and status-read amplification is unknown, especially when most pending attempts are already due or leased. Next proposal: register/retain immutable candidate preparation, then seek a fresh allowance for matched160/s×300s control/candidate arms at the ADR0338 budget, stopping after a failed control. Compare pickup delay, misses/dispatched journey, API CPU/acquisition and conservative issuance under unchanged gates and existing financial/drain/restoration audits. No paid stage,180/s rerun, deployment or publication is currently authorized.

## Tested configuration and boundaries

Four CCE API pods, each1vCPU/2Gi and3general+3payment clients; fifteen background pods;24physical PgBouncer backends/no reserve; shared admission20/API; acquisition deadline500ms. Simulator16delivery jobs/10DB clients. Generator4processes,800active journeys,500HTTP connections,16clients. Keep gateway delays/recovery/freshness unchanged. App image manifest: `sha256:4efa10e301666db5aebc58a3a218df8987307c310c55936ba1c4e3b13de921b2`.

Keep current latency gates, zero generator drops, at most0.01%final customer failures, zero duplicate charges/double-booking/payment loss and full drain. One experiment≤60minutes; no cumulative time cap. No resize/new spending without approval. Reviewed code/evidence may be pushed to this branch; main merge requires approval. Do not contact the retired secondary ECS. Credentials are deliberately omitted; use authorized private session material and never commit it.

## Small read and execution set

Use `scripts/capacity_run_brief.py --help` and `docs/capacity/QUOTA_WORKFLOW.md`. Existing cloud runner: `scripts/run_cce_paid_comparison.py`; financial audits: `scripts/cce_bounded_financial_audit.py`. Explicit run paths: passing `tmp/adr0151-c83348917efc`; failed paid `tmp/adr0151-fa8e65219b30`. Do not recursively scan `tmp` or read the full cumulative state/ADR corpus. Load only the relevant source section when needed.

Preserve pre-existing user edits in `tests/unit/test_cce_ecs_transition.py`, `test_cce_hourly_generator.py`, `test_cce_paid_observers.py`, `.codex-remote-attachments/` and `outputs/`. Do not stage, revert or overwrite them. No hourly or300/s qualification is claimed by these short tests.

Preparation continuation: use tracked scripts/prepare_payment_claim_cutoff_image.py --build --receipt tmp/NEW_PRIVATE_RECEIPT.json, not the old temporary recipe. Four wrapper tests and a fresh22-module network-disabled build passed; payment-claim-cutoff-reproducible-local-2026-10-17.json retains the new image identity and the exact same runtime source map as the original candidate.

Latest local binding: [ADR0342](../adr/0342-bind-local-payment-claim-comparison.md) and [sanitized receipt](cce/payment-claim-binding-local-2026-10-17.json). Existing runner entry point `prepare_payment_claim_proposal` reuses its API/worker builders:19 binding checks and109 existing compatibility cases passed; the exact candidate image validated all8 generated role configurations with networking disabled. This uses a synthetic fixture; live snapshot reconciliation and live candidate registration are incomplete. Local-only dispatch is rejected. Repository pull/push permission was verified privately, but automatic approval review rejected publication before execution because explicit approval for this exact private-image/source transfer was missing. No publication or registry digest/pull proof exists. Next obtain explicit image-transfer approval, finish immutable identity/snapshot/registration, then propose separately approved160/s×300s control then candidate within60minutes, stopping after failed control. No allowance or cloud service was changed.
