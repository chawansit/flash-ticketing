# ADR0212: Canonical worker host workspaces

## Status
Accepted for implementation. Corrects ADR0194 and ADR0198 worker adapters to use the existing registered two-host configuration schema. Supersedes their assumption that every host has a repo field.

## Context
Fresh run adr0153-parents-a3792c9ad27c passed readiness and live inventory, then failed with KeyError before paid-stage creation. Exact protected offline reconstruction passed DiagnosticActions and reproduced PaidStage failing on secondary repo. The registered configuration requires primary repo, secondary prepared_directory and generator repo. Component fixtures used repo for all hosts and hid the mismatch. Original runtime, zero double-booking, queue drain and owned cleanup passed; no customer dispatch.

## Decision
Resolve each host workspace through one strict shared accessor using the registered field for that role. Validate all three paths before package reservation. Reuse the accessor for configuration installation, Compose execution, paid artifact paths, job ownership checks and restoration cleanup. Require canonical absolute paths; reject missing fields, unknown hosts and contradictory aliases. Do not modify the approved configuration, infer paths or add fallback directories.

Change connected and component fixtures to match the actual secondary schema. Reconstruct DiagnosticActions, PaidStage and RestoredPaidArm from recorded actual inputs offline before a fresh experiment. Exercise both placements and cleanup with the same schema.

## Alternatives
Adding repo to the private config changes the approved configuration binding and leaves duplicate sources of authority. A permissive fallback hides schema drift. Role-specific expressions in every consumer repeat the mistake. One accessor preserves the established authority.

## Consequences
This corrects harness integration only. Backend images, load, resource sizes, database connections, financial behavior and customer gates remain unchanged. Capacity improvement is unmeasured.

## Failure and recovery behavior
Invalid or ambiguous workspace configuration fails before reservation. Consumers revalidate paths at use. Existing exact artifact seals, scoped ownership and restoration remain mandatory. Retain failed evidence and consumed scopes; never replay an ambiguous run. Unresolved restoration blocks more load.

## Validation evidence
Recorded failure adr0153-parents-a3792c9ad27c restored with zero dispatch. Protected offline constructor reproduction: DiagnosticActions passed, PaidStage KeyError repo. Exact protected actual-input reconstruction now passes DiagnosticActions, PaidStage and RestoredPaidArm with corrected source hashes rebound locally. This exercises constructors only, with no cloud calls or customer dispatch. One isolated Linux job identity/interruption cleanup test passed in 4.79 seconds. The affected configuration, execution, paid stage, connected runner, profile and diagnostics suite passed 261 tests with one optional native-image skip in 373.25 seconds. That skipped native case was then executed explicitly against the immutable cached image and passed. Ruff, naming and diff checks passed. Live corrected setup and capacity remain pending.
