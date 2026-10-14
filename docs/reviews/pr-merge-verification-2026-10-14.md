# Verification of backend and deployment PRs before merge

Fresh local checks passed for [PR #6](https://github.com/chawansit/flash-ticketing/pull/6) and [PR #7](https://github.com/chawansit/flash-ticketing/pull/7) on 14 October 2026. These PRs promote the older backend and deployment foundation. They do not contain the latest customer recovery or reproduce the image that achieved 100.90 paid-and-issued tickets/s.

[Machine-readable verification evidence](../capacity/pr-verification-2026-10-14.json) records checked commits, source comparisons, configuration, log hashes and cleanup.

## Checked commits and dependencies

| PR | Exact checked head | Base and scope |
| --- | --- | --- |
| 6 | a72d0b5a37bd3732580536c5c8f349f2b7b41712 | Main 4b23a5e681354f9c6c387b07e3554c88c46e0695; backend and workers |
| 7 | f4dd430332774915d3c09b22c64db9943076be5e | PR6 head; CCE deployment and paid-load reproduction |
| 8 | be4cbd884f8dc3160bf67625e0249bfaf9bcded4 | Still draft; full verification outside this final-PR scope |

GitHub heads were rechecked after execution and had not changed. Detached clean copies excluded the three unrelated local test edits. Native Python and container Python were 3.12.14; installed dependencies matched the exact PR lockfile.

## Executed checks

| Check | Result |
| --- | --- |
| PR6 lint and complete native unit suite | Passed; 328 unit tests |
| PR7 lint, repository naming and pinned offline reproduction | Passed |
| PR7 complete native unit suite | 1,802 passed; one Windows symlink case skipped |
| PR7 skipped case executed on Linux | Passed; one test, no skips |
| Full local PostgreSQL, Redis, Kafka and HTTP suite | 586 passed, zero failures/skips: 328 unit, 256 integration, two end-to-end |
| Installed application source | All 21 modules match the PR promotion receipt |
| Clean application defaults | All 14 checked defaults match the promotion documentation |
| Test resource cleanup | Owned containers, volumes and network removed |

PR6 and PR7 have byte-identical application source, migrations, dependency lock, Dockerfile, Compose, integration tests and end-to-end tests. The shared full-stack execution covers those identical inputs; separate fresh unit runs cover each PR's complete checkout.

The isolated local configuration used unique resource names and removed host port bindings to avoid the user's existing services. Application environments and commands were preserved. The stack used the PR-declared PostgreSQL 17.6, Redis 7.4.5 and Kafka 3.9.1 image tags. The newly built application verification image is local evidence, not a published benchmark image.

An initial setup omitted Compose tool/test profiles; it was corrected before test execution. Two attempts to prepare the Linux case encountered shared-filesystem read errors before pytest started; Docker-copied inputs allowed the unchanged case to pass. These setup failures are retained separately. Tests reported deprecation warnings; the focused Linux case also reported a nonfunctional pytest cache-permission warning.

## Source and image scope

The combined PR source matches its documented 21-module promotion map. It intentionally assembles changes from separately tested role images, so it is not byte-identical to every historical role image. A fresh local installed-image comparison and the full correctness suite verify the integrated identity.

Both PRs differ from the latest ADR0324 measured image in 11 module paths, including a diagnostics module absent from these older PRs. Neither has the primary-backed payment-operation inquiry endpoint introduced by the later customer-recovery work.

PR7's offline checker verifies preserved historical role images, workload and configuration contracts. Its success does not mean the combined PR backend or latest runtime is the same as those historical images. Merging these PRs alone cannot support a claim that main reproduces the latest performance result.

## Merge recommendation

Promote PR6 and then PR7 as reviewed foundations only, with their original scope and conservative feature defaults. Obtain the required main-merge approval and confirm hosted required checks before merging. PR7 currently targets PR6's branch; after the foundation merge, verify its main target and resulting commit scope.

Prepare separately reviewed follow-up PRs for the selected customer recovery, current runtime, exact image/configuration bindings and current evidence. Keep experimental optimizations opt-in; default fulfillment batch one remains the portable baseline, and batch four has only short-run qualification in the reference.

Hosted CI status remains unverified: the Codex GitHub CI connector requested account connection. Local check results are reported independently and are not presented as GitHub Actions success.

No cloud load, cloud deployment or main merge occurred during this verification. Temporary local verification images and private execution records are retained for audit.
