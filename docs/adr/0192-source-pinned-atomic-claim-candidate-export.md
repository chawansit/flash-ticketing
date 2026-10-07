# ADR0192: Source-pinned atomic claim candidate export

## Status
Accepted for offline candidate preparation and native PostgreSQL qualification under ADR0172. Extends ADR0189 validation to the frozen cloud baseline. Cloud runner registration, deployment and adoption remain pending; no existing architectural decision is superseded.

## Context
The fresh ADR0171 control passed all 18,000 scheduled paid-ticket journeys and restoration gates. Primary CPU pressure, intermittent commit delays and recovered callback pool timeouts remain observed. ADR0189 reduced the local claim from two statements to one, but its implementation lives in a newer working tree than the frozen ADR0163 cloud image. Replacing the full workers module from that checkout would introduce unrelated changes and invalidate the comparison.

## Decision
Prepare a fresh owned source tree from the already verified ADR0163 export. Read the atomic claim SQL and simulate_one implementation from immutable implementation revision 7ca20ed3e541f1853a937a991e40383f3854a1e3. Insert only the new SQL constant and replace only simulate_one. Require all other module AST nodes to remain identical, and require the delivery, lease duration, eligibility, due ordering, transaction separation and token-fenced acknowledgement to retain their existing behavior. Bind every dependency and runtime module to the committed parent manifests, and retain a complete candidate source receipt with hashes.

Qualify the exact exported runtime against the existing native PostgreSQL claim, contention, rollback, lost-response, token-fencing and duplicate-callback tests. Do not reuse stale cloud qualifications or infer production capacity from these tests. Integration into a registered arm-specific image comparison is a separate required step before cloud mutation; no new profile is authorized by an artifact receipt alone.

## Alternatives
Copy the latest workers module: rejected because unrelated changes would contaminate the candidate. Change the frozen control image or connection budgets: rejected for this comparison. Add a runtime feature flag now: unnecessary for source export and would add another deployment behavior. Increase load before measuring this candidate: rejected. General worker-placement work remains deferred.

## Consequences
The candidate is reproducible from immutable inputs and changes one runtime file at two explicit AST nodes. All other source and dependency bytes remain pinned. This prepares a testable candidate; it establishes no cloud performance gain, 84 tickets/s capacity or hourly qualification.

## Failure and recovery behavior
Reject unknown implementation revisions, unexpected parent source, modified delivery or acknowledgement, extra modules, symlinks, reused output directories and receipt tampering. Preserve failed local evidence. Native tests own isolated schemas and remove only their fixtures. Existing cloud services and journals are unchanged. Future cloud failures retain their normal stop, post-TTL, queue-drain and restoration requirements; ADR0190 does not cover them.

## Validation evidence
Implemented and locally qualified: 8 source-integrity tests passed in 31.74 seconds;5 native PostgreSQL tests against the exact exported runtime passed in 1.69 seconds (8.5 seconds including owned-container orchestration and cleanup). Ruff passed. No tests were skipped. The optional pytest cache emitted a filesystem warning. Initial local connection checks failed; they were retained in the report and no service settings were changed. The native test container was removed after verifying its exact identity. Cloud images, profile registration and execution remain pending. See [source export evidence](../capacity/flash-sale-opening/atomic-claim-source-export-2026-10-07.json). The verified starting control is [fresh control evidence](../capacity/flash-sale-opening/fresh-slow-database-control-2026-10-06.json). ADR0189's earlier local result is evidence for the hypothesis, not validation of this export or cloud capacity.
