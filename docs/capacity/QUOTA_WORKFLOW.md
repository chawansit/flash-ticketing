# Compact performance execution workflow

Start a new implementation chat from [the handoff](IMPLEMENTATION_HANDOFF.md). Use the existing runner and financial audits. This workflow reduces repeated model reads and monitoring decisions; it does not promise a measured quota percentage.

## Compact summary

Run from the repository root:

```powershell
.venv/Scripts/python.exe scripts/capacity_run_brief.py --report docs/capacity/cce/qualified-pools-180-result-2026-10-17.json
```

The output contains only run/status, journey accounting, conservative issuance, latency and cleanup/quality classification. Keep a run report to these counters, the measured limiting evidence and the next action. Do not paste complete private JSON, tracebacks, full ADR lists or this conversation into a new chat.

## Scripted local monitoring

```powershell
.venv/Scripts/python.exe scripts/capacity_run_brief.py --run-dir tmp/adr0151-REPLACE_WITH_RUN_SUFFIX --watch --seconds 3600 --interval 10
```

Replace the directory with the exact active run path. Run once alongside the already-approved runner. The script polls locally and emits phase/status or first-error transitions; unchanged counters do not produce repeated output. It stops on a terminal report or its timeout. Inspect emitted transitions or the final compact result instead of repeatedly asking the model to read several files. Keep host-required progress checkpoints to one line. A watcher timeout means unknown, not passed cleanup or permission to restart load.

Raw checkpoints retain their original classification after independent recovery; use the published compact result for the final operational status. Watcher exit0 means it observed a terminal report, not that the load passed. Display rates are rounded; original evidence retains full precision.

## Scripted audits

The approved runner already executes the bounded database financial audit and cleanup. Reuse `scripts/cce_bounded_financial_audit.py`; never replace a database audit with generator completion counts. If observation/transport fails before auditing, perform a separately retained read-only reconciliation using the existing authenticated session, exact fixture ownership and raw report hash. Preserve the failed load and close only operational recovery when independently proven. Local evidence checks never launch that recovery or change a ledger.

Verify the retained compact report against its raw evidence:

```powershell
.venv/Scripts/python.exe scripts/capacity_run_brief.py --report docs/capacity/cce/qualified-pools-180-result-2026-10-17.json --raw-report tmp/adr0151-fa8e65219b30/cce-comparison.private.json --audit
```

The command checks the raw digest/run identity, financial source/counts, relationships, duplicates, pending work, queues and restoration. When needed it reads only the report's explicitly linked, hash-bound independent reconciliation. **Evidence audit PASS can accompany load quality FAIL.** Missing raw/private evidence fails closed; it does not authorize rerunning a test. This command performs no network or database audit.

## Targeted reads and tests

Read the handoff and compact result, search exact symbols in likely source files, then read a bounded section or diff. Capture long diagnostics to an owned file; surface only a failure code and relevant frames. Run focused checks after the final relevant edit; repeat/broaden only for new changes, failures or unresolved risk. Before deployment, exercise the actual changed fixture/preparation path and verify generated image/configuration identity.

Do not spend the load-test wait improving unrelated tooling. Record one result, one next correction and the remaining uncertainty. Create a new concise handoff when the baseline or next task changes.
