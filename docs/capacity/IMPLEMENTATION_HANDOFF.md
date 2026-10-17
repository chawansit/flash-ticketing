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

API CPU rose 2.40→3.55 cores out of four; acquisition mean 11.00→21.34ms. Payment pickup rose 0.105→3.146s; callback delivery 60.92→76.74ms. Cache misses rose 48,759→138,178 despite fewer completed journeys. Writer command-age mean stayed approximately stable at 1.56→1.51s. These observer-lifetime comparisons suggest confirmation backlog and status-read amplification; a single causal holder is not yet proven.

Inspect the payment simulator's claim/delivery/ack loop, committed status caching and polling, and API admission using targeted searches. Make one evidence-supported local correction, with an ADR before an architectural choice. Test its actual changed path. Keep the proven baseline and do not merely increase generator concurrency or add retries. Prepare the next fixed-budget comparison; seek approval only when that concrete paid stage is ready.

## Tested configuration and boundaries

Four CCE API pods, each1vCPU/2Gi and3general+3payment clients; fifteen background pods;24physical PgBouncer backends/no reserve; shared admission20/API; acquisition deadline500ms. Simulator16delivery jobs/10DB clients. Generator4processes,800active journeys,500HTTP connections,16clients. Keep gateway delays/recovery/freshness unchanged. App image manifest: `sha256:4efa10e301666db5aebc58a3a218df8987307c310c55936ba1c4e3b13de921b2`.

Keep current latency gates, zero generator drops, at most0.01%final customer failures, zero duplicate charges/double-booking/payment loss and full drain. One experiment≤60minutes; no cumulative time cap. No resize/new spending without approval. Reviewed code/evidence may be pushed to this branch; main merge requires approval. Do not contact the retired secondary ECS. Credentials are deliberately omitted; use authorized private session material and never commit it.

## Small read and execution set

Use `scripts/capacity_run_brief.py --help` and `docs/capacity/QUOTA_WORKFLOW.md`. Existing cloud runner: `scripts/run_cce_paid_comparison.py`; financial audits: `scripts/cce_bounded_financial_audit.py`. Explicit run paths: passing `tmp/adr0151-c83348917efc`; failed paid `tmp/adr0151-fa8e65219b30`. Do not recursively scan `tmp` or read the full cumulative state/ADR corpus. Load only the relevant source section when needed.

Preserve pre-existing user edits in `tests/unit/test_cce_ecs_transition.py`, `test_cce_hourly_generator.py`, `test_cce_paid_observers.py`, `.codex-remote-attachments/` and `outputs/`. Do not stage, revert or overwrite them. No hourly or300/s qualification is claimed by these short tests.
