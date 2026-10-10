# ADR0256: Qualify customer recovery for one hour

## Status
Accepted for implementation and targeted local validation. Cloud hourly qualification has not started.

## Context
ADR0254/0255 short control completed all 25,200 offered journeys with zero errors, drops or retries and passed all nine gates with exact restoration. The pipeline candidate recovered three payment 503 responses but worsened latency and occupancy; keep the pipeline off. Failure snapshots identify payment request/callback commit holders at rejection, not the complete wait history or underlying WAL/storage cause. The previous hour met the numerical ticket target but failed customer and diagnostic gates. A new hour must use the passing recovery-enabled control binary and report recovery independently of first errors and generator drops.

## Decision
Qualify the same immutable recovery image, pipeline off, four 1-vCPU/1-GiB API pods, four connections per API (two payment/two general), shared admission 20, PgBouncer 24, simulator dispatch 12/pool 10, unchanged gateway delays and current quality gates. Use the existing two-shard hourly allocation: 1,008 isolated shows, 300 seats each, 302,400 distinct journeys, 84 offered journeys/s for 3,600 seconds. Preserve original journey deadlines and the 3,720-second completion deadline. Forward the existing bounded same-key recovery policy through the hourly allocation adapter and aggregate its separate counters. Seal the two hourly adapter files and reuse the exact 80-file recovery workload; do not change scheduling, seats, polling, callback duplication or customer identity.

Require the exact fully passed/restored ADR0255 control evidence, published-image proof, and a fresh consumed-once hourly ledger. Preserve historical hourly source bytes and the immutable reproduction lock. The approved hourly experiment ceiling is 90 minutes including preparation, mandatory audits, and restoration. Cleanup continues if needed. No machine resizing or new connection budget is authorized.

For necessary diagnostic retention, accept only a structurally valid rolling suffix of the existing 16-event API ring. Independently accumulate events across one-second snapshots and require every sequence from 1 through the settled terminal failure counter. Rollover is acceptable only if the collector already retained all overwritten events. Missing events, reset counters, changed events, unknown fields, diagnostic errors and payload/trace ceilings still fail completeness. This changes collector interpretation, not application instrumentation, image, correctness or gates. Supersedes ADR0241's requirement that each single snapshot retain the entire lifetime history; preserves its requirement for complete independent terminal coverage.

## Alternatives
Enable the rejected pipeline: unsupported by measurements. Increase connections/pods or relax gates: changes the proven baseline. Run the no-recovery image again: does not qualify the intended customer behavior. Increase the API ring: requires another application image and increases metrics payload. Ignore rollover: conceals missing evidence. Generalized runner redesign: unnecessary.

## Consequences
Recovery adds bounded lookup/replay work only on transient failures, with original payment keys and TTL. A short pass does not predict the hourly error rate. The hour can still fail from real sustained overload, generator drops, deadline violations, incomplete diagnostics, or durability/queue mismatches. Commit stalls remain under investigation; no storage causality or maximum-capacity improvement is claimed.

## Failure and recovery behavior
Never replay payment blindly when authoritative lookup is unavailable. Preserve expired holds, customer authorization, duplicate callback semantics and committed payments. Stop progression at any failed gate; retain original traces, check all paid outcomes after TTL, drain queues, retire only owned fixtures and restore the exact original ECS/CCE state. Any missing rolling sequence fails observability even when customers recover. No automatic repeat of a consumed hour.

## Validation evidence
Executed: 73 focused checks passed; expanded affected checks passed with 319 passes/one skip, followed by 74 passes after correcting two historical permission fixtures. The initial whole-unit run had 2,081 passes/two skips and 36 failures; affected tests were corrected and rerun rather than claiming a full-suite pass. Source reproduction verifies all 535 archived inputs and 12 declared current overlays. Ruff passed for changed code/tests. Cloud load unexecuted. Existing short evidence: [paired recovery comparison](../capacity/cce/customer-recovery-comparison-2026-10-10.json).
