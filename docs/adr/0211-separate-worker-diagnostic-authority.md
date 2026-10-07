# ADR0211: Separate worker diagnostic authority

## Status
Accepted for implementation. Supersedes ADR0198's requirement that the diagnostic username equal the application username. Uses the separately approved read-only administrator diagnostics in ADR0180; no new privileges or application credentials.

## Context
Fresh run adr0153-parents-875f09fa69fb passed private readiness, worker startup and all host-aware source/import/image/metrics inventory checks. It failed before PaidStage creation. DiagnosticActions compares its target user to saved PgBouncer DB_USER. Actual application identity is ticketing and the approved diagnostic identity is root. Prepared already permits a separate diagnostic principal, so this inconsistent constructor check escapes package validation. Synthetic connected fixtures used root for both identities and did not reproduce production. Exact restoration, zero booking duplication, queues, generator idle and cleanup passed with zero dispatch.

## Decision
Use one shared diagnostic-trust validator during local package preparation and diagnostic construction. Require the original host, port, database, verify-full setting and exact single read-only CA mount and content hash. Allow the already bound target user to be the application user or the explicitly approved root diagnostic user; reject any other principal. Scope/context hash binding, read-only verified TLS connection, privileges/visibility preflight, pinned observer sources, connection ceilings and exact credential cleanup remain mandatory.

Update connected fixtures to use ticketing for application connections and root for diagnostics, then exercise actual construction and guarded comparison paths before cloud deployment. Reproduce validation against the real protected snapshot/target offline. No application image, financial transaction, customer gate or cloud resource changes.

## Alternatives
Using the application identity for privileged diagnostics can yield incomplete measurements. Increasing its privileges changes production permissions. Disabling original endpoint/CA validation weakens trust. Retain exact shared endpoint trust while separating the already authorized principal.

## Consequences
Local preparation and live construction enforce the same authority contract. This is a harness integration correction; throughput benefit remains unmeasured.

## Failure and recovery behavior
Reject foreign role, endpoint, CA, writable/multiple mount or non-verified TLS before reservation/deployment and again during construction. Read-only connection/visibility failures still block paid traffic. Preserve failed result and consumed scope; no ambiguous replay. Clean only owned credentials/resources and retain ownership until restoration and all correctness/queue gates pass.

## Validation evidence
Run adr0153-parents-875f09fa69fb, inventory tmp/adr0153-parents-0ddad3019161/0056-bootstrap-inventory.json, final result FAILED_RESTORED and zero dispatch. Protected offline authority reproduction confirmed the old username predicate fails and the shared exact trust validator passes with the approved separate observer. All 114 affected diagnostic, profile and connected-runner tests passed in 204.32 seconds; Ruff and repository naming checks passed. Corrected live diagnostics remain pending. No paid load or capacity improvement claim.
