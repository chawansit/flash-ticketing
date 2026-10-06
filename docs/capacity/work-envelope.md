# Standing work envelope

The user approved ADR0172 on 2026-10-06. Routine engineering proceeds independently within [the recorded envelope](work-envelope.json). ADRs record decisions and alternatives; they are not an automatic approval gate.

| Boundary | Approved policy |
| --- | --- |
| Business goal | 300,000 unique paid-and-issued tickets per hour; not yet qualified |
| New infrastructure spending | Zero; no new services, resize, replicas or storage upgrades |
| Existing cloud charges | Continue at existing sizes; no overall billing cap specified |
| Cumulative test time | No cumulative cap; recorded; every experiment as effectively short as possible |
| Individual experiment | At most 3,600 seconds before stop; mandatory cleanup may extend this |
| Customer outcomes | Exact current profile gates, zero double-booking/payment loss, complete queue drain |
| Deployment | Bounded tests on the existing verified resources |
| Publication | Reviewed code and sanitized evidence to codex/ branches |
| Main merge / other destinations | Require separate approval |
| Unattended scheduling | Not authorized |

Select the shortest useful duration before execution and record the hypothesis and duration rationale. Prefer offline/local verification and short diagnostic probes, then extend only when needed to capture intermittent behavior or validate stability. Matched comparisons keep equal baseline/candidate windows; do not stop an apparently successful arm early to improve its reported result. Mandatory post-TTL durability, zero-double-booking, queue drain and restoration checks retain their required windows. Hour-long qualification still requires an hour. The 3,600-second experiment bound is a ceiling, not a target; the current matched diagnostic control retains its qualified 300-second load window.

Run the local status command without cloud access:

```powershell
.venv/Scripts/python.exe scripts/run_work_envelope.py
```

The currently qualified registry contains only slow_database_control: unchanged ADR0171 images, topology and budgets, one safety qualification then one 60 journeys/s, 300-second control. One command performs both protocols, stops if qualification fails, preserves evidence and records the result:

```powershell
.venv/Scripts/python.exe scripts/run_work_envelope.py --execute --config tmp/adr0153-checks/live-config.private.json --ssh-runtime tmp/adr0126-ssh-runtime
```

The default artifact is the exact immutable receipt in the qualified profile. An optional --artifact override must match it. Staging summaries are not artifact receipts. Live qualification still verifies installed source and immutable image identities; a declared receipt alone does not prove deployment. Credentials stay in protected inputs and the password terminal, never in the envelope, Git or public reports.

Each experiment gets a new append-only reservation and a new ledger. Old scopes are retained. The safety report must match the current binding, be fresh and show verified restoration before measured dispatch. The existing exclusive lock serializes mutations. Failed or ambiguous allocations are not refunded or replayed. Failed-but-restored customer experiments can be followed by a fresh corrected run; missing integrity/restoration or ownership proof blocks new load until recovery is verified.

The current profile requires every scheduled journey to be fulfilled by its unchanged 420-second generator completion deadline, no generator drops/customer retries, dispatch lag p95 at most 100 ms and maximum at most 500 ms. HTTP errors recovered within a fulfilled journey are reported separately from customer failure. Customer p95 timings remain reported measurements where the profile does not define a percentile threshold; this workflow does not invent a new SLO or relax an existing gate. Post-TTL financial counts, duplicate booking, full queues/Kafka drain, image/source proof and restoration still qualify independently.

Request a pause or resume without changing consumed counters:

```powershell
.venv/Scripts/python.exe scripts/run_work_envelope.py --pause
.venv/Scripts/python.exe scripts/run_work_envelope.py --resume
```

A pause marker can be written while the runner owns its lock. Checks stop further experimental remote actions, then allow mandatory evidence, financial checks and cleanup. This is cooperative stopping between bounded operations; it is not instantaneous cancellation of an already running remote call. Cleanup time is included in actual elapsed accounting.

Before publication, review the outgoing files, exclude secrets/private raw evidence, execute appropriate tests and canonical-name checks, then validate the destination:

```powershell
.venv/Scripts/python.exe scripts/run_work_envelope.py --check-publication codex/standing-work-envelope --reviewed --sanitized
```

The publication guard records policy compliance, not proof that a human/code review happened. The pre-push hook restricts destinations to codex branches and rejects deletion; naming hooks still validate outgoing revisions. A separately approved main merge should use the normal reviewed merge workflow rather than disabling these checks.

Extend the registry only after an ADR, implementation and executed local qualification. New profiles may proceed without another human approval if within the envelope. ADR0173 extends the locally qualified registry with database-side wait/WAL sampling on the unchanged 60/s control. Higher load and candidate comparisons remain outside this registry. Profile source/configuration/artifact changes need fresh binding and qualification, not a reset of previous ledgers.

Escalate only new spending/infrastructure actions, requirement/correctness changes, publication exceptions or unresolved recovery requiring user involvement. Report result, evidence, remaining uncertainty and recommendation at meaningful checkpoints. Always distinguish implemented, tested and production-qualified.

The ADR0173 profile uses the same protected configuration, immutable application images, workload and connection budgets. Select `--profile database_wait_control` with the existing `--execute`, `--config` and `--ssh-runtime` arguments. A transient read-only capabilities check closes before the existing pipeline observer starts; diagnostic startup must pass before buyer dispatch. Disabled WAL timing is reported as unavailable evidence. All existing customer, financial, queue and restoration gates remain required.
