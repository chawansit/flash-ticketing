# ADR0307: Renew CCE admission from live identities

## Status

Accepted for explicitly bound ADR0306 short comparisons and future qualification. Historical snapshots and failed attempts remain unchanged.

## Context

The second ADR0306 control stopped before paid traffic because the pre-transition ECS inventory was more than 300 seconds old after native worker activation, safety expiry and observer staging. The 300-second gate correctly rejected it. ECS inventory describes the bootstrap deployment; CCE APIs and workers are subsequently observed separately. Extending the timestamp without fresh evidence would hide drift.

## Decision

Immediately before final observer qualification, re-observe all four CCE API pods against their admitted UID, image, resources, startup source/configuration and process identities. Re-observe all 14 workers against their admitted bundle and verify the remaining ECS background deployment is unchanged. Only after these live checks succeed, create a renewed inventory view with a new joint-validation timestamp. Retain the complete original inventory, its capture timestamp and digest as provenance, plus exact native API/worker receipt digests, run and binding identity.

The renewed timestamp refers to live admission revalidation, not a new observation of the intentionally stopped ECS APIs. The original snapshot is retained unchanged. Keep the existing 300-second qualification/dispatch window and every source, budget, readiness, latency, safety and correctness check. Apply the same renewal to both comparison arms. No additional database connection, altered observer cadence, paid workload or application image.

## Alternatives

Increase or bypass inventory freshness: rejected. Merely overwrite the timestamp: rejected. Recreate stopped ECS APIs to capture another bootstrap inventory: unnecessary mutation. Move observer startup before all safety checks: changes sampling/provenance timing and can expire the short fixture.

## Consequences

Long setup no longer makes an unchanged deployment appear stale at final admission, while any changed native identity or background deployment fails before paid dispatch. This is an admission timing correction, not a throughput improvement.

## Failure and recovery behavior

Any failed live observation prevents renewal and paid dispatch. Preserve stale/failed evidence, perform existing mandatory cleanup and exact restoration, and use a fresh reservation. Retain both original and renewed receipts for verification.

## Validation evidence

Second attempt adr0151-0e87334f0eb5: FAILED_RESTORED, zero paid stages, exact restoration and cleanup complete. Implemented; 45 focused renewal/customer-outcome checks passed, including changed UID, missing members, background drift and future timestamp rejection. Source compilation passed. Original and renewed receipts are both retained; no new capacity result.
