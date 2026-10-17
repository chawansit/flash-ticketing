# Repository engineering rules

Whenever selecting or changing an architectural pattern, create an ADR under docs/adr before implementing the choice. Include status, context, decision, alternatives, consequences, failure/recovery behavior and validation evidence. Link it from docs/adr/README.md. Cover persistence/locking, messaging, idempotency, TTL and scaling decisions. Supersede accepted decisions explicitly. Do not claim tests passed unless they were executed; distinguish implemented behavior from future scope.

## Quota efficient execution

- Begin continuation work with `docs/capacity/IMPLEMENTATION_HANDOFF.md`; then read only the linked current result and relevant source sections. Do not ingest the full ADR corpus, cumulative state JSON or recursively search temporary artifacts by default.
- Use `scripts/capacity_run_brief.py` for compact results, bounded local watching and retained-evidence checks. Reuse existing cloud runner financial audits and cleanup; local evidence checks are not a substitute for database reconciliation.
- Read named files, bounded sections and diffs. Print allowlisted counters and failure codes, not full private reports, credentials or long tracebacks. Capture detailed logs to owned temporary files.
- Run focused checks once after the final relevant edit; repeat or broaden only for new failures, changes or unresolved risks. Exercise the actual changed preparation path before cloud deployment.
- Let scripts monitor unchanged state. Keep host-required progress checkpoints to one line; report meaningful transitions, failures, decisions and final results without narrating each poll.
- Record one compact run result: offered rate/window, scheduled/dispatched/drops, initial/recovered/final failures, in-window paid tickets, latency, limiting evidence, financial/drain/restoration status and next action. Keep failed quality separate from recovered operational state.
- Preserve the work envelope. No automatic paid rerun, infrastructure resize, correctness relaxation or main merge. Never include credentials in a handoff or a new chat prompt.

