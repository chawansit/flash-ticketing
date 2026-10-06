# ADR0185: Durable pre-dispatch fixture identity

## Status
Accepted for runner correction under ADR0172. This records evidence ownership, not a new payment or persistence architecture. Prior failed scopes remain unchanged.

## Context
The ADR0181 paid comparison returned fixture identities to the runner but kept the fixture file only inside disposable API containers. A subsequent read-only recovery probe found the exact containers absent. Current restored runtime and empty queues cannot establish a scoped per-entity financial proof without those show identities. Reconstructing ownership from timestamps or titles would be unsafe.

## Decision
Retain a minimal fixture identity receipt in the existing owned local stage directory immediately after successful fixture creation and before minting buyer tokens or dispatching customers. Validate the exact bounded show count, canonical unique UUIDs, development environment, distributed layout and 300 seats per show. Copy only schema, fixture ID, creation/sale timestamps, layout, show count and show IDs; omit tokens, credentials, origin and other source fields. Bind the canonical receipt hash into the stage record. Use exclusive creation and persist the stage record before proceeding. Include the local retention helper in the adapter source binding.

Evidence-write or validation failure stops before buyer dispatch. Keep the returned show IDs available to the existing retirement/cleanup path even if receipt persistence fails. Do not change workload, retry behavior, customer gates, topology, fixture lifetime or financial expectations. Existing profile bindings change because runner sources changed, so a future experiment requires fresh reservation and qualification; never replay a consumed scope.

## Alternatives
Keep fixture identities only in disposable containers: loses audit ownership after restoration. Copy private buyer manifests: retains unnecessary secrets. Infer fixtures from database titles or time ranges: rejected because neither proves exact experiment ownership. Reclassify the completed paid failure using matching totals: rejected because aggregate equality is not per-entity proof.

## Consequences
Receipts contain non-secret synthetic ownership identifiers and remain in the ignored raw evidence directory. A sanitized report may include their hash/count without publishing private manifests. This correction supports future recovery audits; it cannot restore the missing identities for the earlier run. A separately justified strict recovery mechanism is still needed before more load.

## Failure and recovery behavior
Malformed, duplicate, oversized, noncanonical or stale/future timestamps fail closed. Existing receipts are never overwritten; symlink directories and unexpected paths are rejected. A partial evidence-write failure preserves available ownership and stops dispatch. Cleanup remains mandatory. No new paid scope is authorized by this change.

## Validation evidence
Pending at ADR creation. Record executed unit and affected runner tests after implementation. No cloud fixture, dispatch, or recovery reclassification is claimed.

Executed local validation: 337 affected tests passed in 41.52 seconds; 20 evidence tests passed after the test-style correction. Ruff passed. Three synthetic Compose models validated without starting containers. Read-only live checks found restored runtime unchanged, secondary empty, generator idle and every queue zero. Exact prior primary test containers were absent; scoped paid recovery remains unresolved. See [preparation evidence](../capacity/flash-sale-opening/background-service-separation-preparation-2026-10-06.json). No deployment, load, recovery reclassification or capacity improvement.
