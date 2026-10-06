# Repository engineering rules

Whenever selecting or changing an architectural pattern, create an ADR under docs/adr before implementing the choice. Include status, context, decision, alternatives, consequences, failure/recovery behavior and validation evidence. Link it from docs/adr/README.md. Cover persistence/locking, messaging, idempotency, TTL and scaling decisions. Supersede accepted decisions explicitly. Do not claim tests passed unless they were executed; distinguish implemented behavior from future scope.

# Token-efficient capacity workflow

This policy was requested by the user on 2026-10-03. Apply it in future sessions.

- Start with `docs/capacity/CURRENT_STATE.json` and Git status/revision. Read only the linked decision, evidence or code needed for the current task; do not reload the full benchmark history.
- Reuse the script-driven orchestration in ADR 0040 and the bounded generator controls in ADR 0090. Prefer one command per authorized experiment for preflight, execution, audit, cleanup and report collection. Extending paid-stage orchestration is future work, not an implemented capability.
- Let scripts handle bounded waits, approved stage progression and stop conditions. Avoid repeated model polling of unchanged state; preserve required concise progress updates and user interruption handling.
- Read compact summaries first: revision/configuration changes, offered and achieved load, scheduled/completed journeys, drops/errors, p95 timings, correctness audit, queue drain, failed gates and evidence paths. Distinguish HTTP RPS from completed paid tickets/s and synthetic controls from backend capacity.
- Retain raw evidence in files. Retrieve bounded log excerpts only for failed gates or a specific hypothesis. Limit file searches to relevant directories and output to matching paths or selected lines.
- Compare one candidate with an identified baseline, changing one factor at a time. Report differences and unresolved questions instead of repeating historical results.
- Prefer High effort for root-cause analysis, concurrency correctness and architectural review; Medium for agreed implementation and routine result analysis; Low for established checks and status summaries. These are selection recommendations; do not claim the client's effort/model was changed without confirmation from a supported setting/tool.
- Run checks appropriate to the change; repeat or broaden them only after new edits, failures or unresolved concerns justify it. Never omit durability, zero-double-booking or queue gates to save tokens.
- Update `CURRENT_STATE.json` at a meaningful checkpoint or pause with current revision/topology status, evidence, confirmed findings, unresolved issue, next action and scope/authorization. Keep it compact, free of secrets and faithful to executed work.
- Preserve explicit pauses. This policy does not authorize new tests or unattended schedules while testing is paused. Create an ADR before future architectural/orchestration pattern changes; the workflow rules alone do not modify production architecture.

# Naming and publication discipline

- Resolve existing ADR names from docs/adr filenames and README; do not recreate slugs from memory. Use exact identifiers and preserve historical run/evidence names.
- Generate staging directories with stage_status_refresh_images.new_stage_output(); never rebuild an ownership prefix in a workflow. Ownership naming and validation share one definition.
- Run scripts/check_repository_names.py before commit/publication; staged and outgoing-revision hooks plus CI enforce structural checks. Fix naming failures before any cloud mutation. These checks do not authorize load or reset consumed scopes.
- Proofread human-facing text for spacing and spelling; automated structural checks do not guarantee prose correctness.

# Standing work envelope

The user approved ADR0172 on 2026-10-06. Read docs/capacity/work-envelope.json with the compact CURRENT_STATE checkpoint. Routine implementation, profiling, fixes, local tests, ADRs and evidence collection proceed independently within these boundaries. ADRs record decisions; they do not automatically require another human approval.

- Existing resources and sizes only; maximum new infrastructure spending is zero. Existing service charges continue; no overall bill cap was specified.
- Cumulative time may be as long as needed, explicitly approved; track it. Every experiment remains bounded. Use scripts/run_work_envelope.py for registered profiles, fresh reservations and exact bindings. Extend and locally qualify the registry before other experiments. Never reopen old scopes or bypass the runner.
- Preserve current profile latency/error gates, payment durability, zero double-booking, customer authorization, post-TTL checks and complete queue drain. Failed controls stop progression. Diagnose and correct independently; preserve evidence and use a fresh identity, never replay an ambiguous experiment.
- Human pause is separate from scope exhaustion. Stop new experimental actions when paused; continue owned cleanup and mandatory verification. Uncertain ownership or restoration blocks more load.
- Reviewed code and sanitized evidence may be pushed to codex/ branches. Main merges, other destinations and unattended schedules require approval. Run naming checks and appropriate tests; never publish secrets or private manifests.
- Escalate only new spending/infrastructure, requirement or correctness changes, publication exceptions or unresolved recovery requiring user involvement. Handle routine implementation independently.
- Report meaningful checkpoints with result, evidence, uncertainty and recommendation. Distinguish implemented, tested and production-qualified; update cumulative accounting faithfully.
