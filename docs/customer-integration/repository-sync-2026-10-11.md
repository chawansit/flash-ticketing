# Repository documentation synchronization

Checked on 11 October 2026, Asia/Bangkok.

## Verified starting state

- Repository: `chawansit/flash-ticketing`.
- Working branch: `codex/customer-recovery-2026-10-10`.
- Verified starting commit: `1b615d83dbee55fed695e3a309b0a4ccd78bbed9`.
- Remote references refreshed with `git fetch origin --prune` before comparison.
- Local HEAD versus the matching GitHub branch: zero commits ahead and zero behind.
- Tracked `docs/` working-tree changes at that point: none; document bytes at HEAD and the matching remote branch matched.
- There were 1,709 tracked files under `docs/` at the starting commit. Compared with main, the working branch contained 187 additional document/evidence files and one modified file, 188 differences in total. This count is a comparison of tracked paths, not 188 documents awaiting individual approval.
- Main was at `4b23a5e` at the check. Main and the working branch are different publication surfaces; a document being on GitHub does not mean it is on main.

## Documentation prepared in this update

The [Thai integration guide](redis-kafka-adoption-th.md) explains Redis, Kafka, the database authority, phased adoption, numbered synchronous/asynchronous flows, recovery, and historical versus current capacity evidence. README links it. The baseline architecture, application-flow and local system-diagram pages now state their scope explicitly; application-flow distinguishes the new opt-in load-client recovery from an unimplemented frontend retry loop.

This is a documentation update. It selects no new runtime architecture, deploys nothing, runs no cloud load, changes no feature flags and claims no new performance improvement. The integration stages are proposed discussion options for the customer's team; their system has not been inspected.

The update is intended for the existing codex branch under standing publication permission. Main merge remains a separate approval. Synchronization is checked again after publication; Git does not continuously synchronize local files automatically.

## Files outside the equality claim

Three pre-existing local test changes were left untouched: `test_cce_ecs_transition.py`, `test_cce_hourly_generator.py`, and `test_cce_paid_observers.py`. Untracked remote attachments, private run inputs, credentials, certificates and raw evidence are not included in the public documentation equality claim and are not being uploaded. Therefore the complete local workspace is not identical to GitHub even when tracked documentation matches.

## Repeatable check

After fetching, compare HEAD with the matching remote branch, compare tracked documentation bytes and check local documentation changes. Compare main separately. Publish only reviewed changes, then verify the new local commit equals the remote branch and no tracked documentation remains uncommitted. Do not merge main or force-push merely to make these comparisons equal.
