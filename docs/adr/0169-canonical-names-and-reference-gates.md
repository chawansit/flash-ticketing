# ADR0169: Canonical names and reference gates

- Status: Accepted for repository checks; runtime architecture unchanged
- Date: 2026-10-06

## Context

A cloud staging directory was manually named with a new ADR prefix while the ownership validator required the inherited ADR0153 prefix. This caused avoidable preflight failure. Repeated identifiers and filenames also make documentation and result references vulnerable to typos. The user requested prevention.

## Decision

Derive the ADR catalog from exact numbered filenames; reject duplicate numbers, invalid names, title/index identifier disagreement, missing index entries, missing local ADR links, case mismatches and unknown ADR references in compact capacity-report decision metadata. Check the staged Git snapshot before commit, the actual outgoing revision before push and the checked-out repository in CI. Use the existing ownership module as the single source for generated staging names and validation; expose a directory factory instead of reconstructing prefixes in callers. Historical run identifiers and raw evidence remain immutable. No pattern, scope or protocol counters are reopened by these checks.

## Alternatives

Manual proofreading alone allowed previous failures. Renaming historical runs would break ownership/evidence bindings. A global spelling checker would flag valid technical terms and cannot verify identity. None selected.

## Consequences

Failures identify the exact path and identifier before publication. Git hooks are enabled for this repository only; CI independently enforces the check. Checks cover structural identity and references, not every prose spelling error or external link. Compact report checking is bounded to 1 MiB; raw benchmark files are excluded from parsing.

## Failure and recovery

Stop commit/push/preflight on invalid identity. Do not guess a replacement, rename old evidence, bypass a failed protocol or dispatch load. Correct the source name/reference and repeat the relevant check. Preserve existing hooks when configured; do not overwrite unrelated settings.

## Validation evidence

Executed 66 related unit tests, including real staged/outgoing snapshot and hook rejection checks. After the final test-output formatting adjustment, all 16 focused naming tests passed again. Working repository and HEAD checks passed; changed-file Ruff and whitespace checks passed. No cloud calls, load, ledger reset or runtime change. See [local validation](../capacity/flash-sale-opening/naming-guards-local-validation-2026-10-06.json).
