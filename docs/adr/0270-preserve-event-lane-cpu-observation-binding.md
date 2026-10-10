# ADR0270: Preserve event-lane CPU observation binding

## Status
Accepted narrow evidence correction to ADR0266. No runtime placement, customer workload, connection budget or qualification gate changes.

## Context
The retained primary CPU trace from adr0151-f80fef5b265c contains all 61 scheduled samples with maximum sampling deviation 0.00224 seconds. Its validation failed with "Explicit event-lane CPU binding required", rather than a sampling gap. The valid preflight specification contains event_lane_decision=ADR0266; collect and summarize omit this field when copying evidence. Consequently the strict role validator cannot admit the measured projection consumer.

## Decision
Preserve event_lane_decision through collection, summary specification reconstruction and returned summary, alongside the existing decision and inventory binding. Keep strict role, image, instance, container, sample and time-window checks. Add full collect/summary regression tests, including rejection when the explicit binding is absent or wrong. Refresh only the current overlay digest; preserve the historical archived blob.

## Alternatives
Relax projection-consumer validation: hides unbound roles. Repair the old failed evidence: would manufacture qualification. Ignore CPU attribution: obscures the measured resource bottleneck.

## Consequences
Future observations retain the already validated topology identity. The original failed run stays failed. Retained counters may inform diagnosis, explicitly as supplemental unqualified attribution, never as a reconstructed passing observation.

## Failure and recovery behavior
Missing or inconsistent topology binding still fails closed. Do not modify original traces or restart load as part of this correction. Slot-history and bounded log-capture gaps remain separately unresolved.

## Validation evidence
Pending focused regressions and reproduction hash verification. Supplemental counters show fulfillment consumers 0.754 cores, original PgBouncer 0.454, CCE pooler bridge 0.416, writers 0.379 and projection 0.151 average cores over 300 seconds. Pipeline host CPU p95 was 97.22%. No capacity qualification follows from these counters.

Executed validation: 54 affected CPU observer tests passed, including collection and summary binding retention and rejection of absent/wrong binding. Ruff passed. All 535 historical files and 20 declared current overlays verified; no cloud calls or customer dispatches occurred in these checks. The new observer has not been cloud-tested. Original failed trace and result digests remain preserved.
