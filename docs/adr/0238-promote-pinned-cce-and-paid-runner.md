# ADR0238: Promote pinned CCE deployment and the paid-load runner

## Status
Accepted for repository integration, 2026-10-09. Depends on backend PR #6; no main merge, deployment or load authorization is granted. Existing locking, payment, TTL, messaging, scaling and test gates are unchanged.

## Context
The historical CCE experiment used role-specific frozen images and an overlaid generator. Those assets differ from the combined backend source in PR #6. Shared runner dependencies and ignored temporary source paths currently prevent an ordinary checkout from reproducing the declared experiment.

## Decision
Promote the existing CCE adapter, bounded short/hourly orchestration and their shared helper dependencies as a stacked PR after PR #6. Retain immutable image/platform/config/source contracts, fixed resource and connection budgets, workload profiles, safety, durability, queue and restoration checks. Vendor the exact 78-file frozen harness, qualified polling helper and sanitized complete source-export snapshots as hash-addressed text assets, with SHA-256 validation, instead of requiring an unrelated Git object or an ignored temporary export. Historical contract readers may validate immutable packaged source snapshots when their original local export is absent; they never recreate an old experiment scope. Real existing export trees continue to undergo complete file-set/hash checks. Preserve exact committed bytes with Git attributes so raw migration checksums remain identical across Windows and Linux without changing existing migrations. Fail closed on asset, runtime, configuration or scope drift. The work envelope and consumed journal are historical records, not fresh permission. CI and this promotion run offline/local checks only.

The historical role-specific images remain the reproduction target. A future image built from PR #6 is a new identity requiring its own proof and comparison; no substitution is permitted. Architectural decisions governing those historical patterns remain in force; this ADR supersedes only the local Git-object/temporary-directory packaging dependency for harness retrieval.

## Alternatives
- Promote all experimental commits: too much unrelated code, evidence and live authorization state.
- Rebuild images from current backend source: would silently change the measured runtime.
- Create a new small runner: would discard already exercised recovery and correctness controls.
- Keep ignored exports and historical Git lookups: breaks fresh and shallow checkouts.

## Consequences
The PR includes a substantial shared helper dependency graph. Separate integration commits and a dependency inventory make that scope reviewable. Frozen text assets duplicate historical scripts deliberately and are not independently runnable entrypoints. Runtime access, registry credentials, kubeconfig, private fixtures and fresh authorization remain operator inputs and are never published. Exact private endpoints are retained as historical topology assertions; portability is future scope.

## Failure and recovery behavior
Reject missing or altered assets, mutable image references, mismatched source/configuration proofs, reused scopes and failed safety gates before paid dispatch. Preserve original failed evidence; stop progression, verify post-TTL durability, drain queues, retire only owned fixtures, restore the original deployment and delete only owned resources. Uncertain ownership/restoration blocks more load. Packaging does not fix the hourly observer trace ceiling, payment acquisition failure or the 18,000-seat projection issue.

## Validation evidence
Executed local verification: 1,802 unit tests passed, one test skipped because symlink creation is unavailable on this Windows host, 168 final focused checks passed, Ruff and repository naming passed, and all 535 pinned inputs plus the complete eight-role constructor verified offline. See [the promotion report](../capacity/cce/runner-promotion-2026-10-09.json). CI is pending. No cloud load, deployment, capacity improvement or production qualification is claimed. The retained hourly report remains FAILED_RESTORED despite more than 300,000 issued tickets inside its measured hour.
