# ADR0268: Validate explicit API flags before legacy projection

## Status

Accepted correction to the ADR0266 evidence compatibility adapter. Runtime architecture, image, budgets and workload are unchanged. Supersedes the adapter's assumption that observed API settings have exactly the historical key set.

## Context

Run adr0151-bf07b7b06bee stopped before fixtures, CCE resource creation or buyer dispatch. Retained inventory shows every API matches the current contract. Its only differences from the historical settings schema are EVENT_CONSUMER_SEPARATION=0 and RESERVATION_WRITE_PIPELINE=0. Both groups and durable queues were drained. The validator rejects the extra keys, although the runtime values preserve the historical behavior. Exact normal restoration succeeded; the accounting guard remains blocked because the incomplete qualification has no financial integrity receipt.

## Decision

Require four observed APIs with both new flags explicitly disabled before making a deep-copied historical view. Remove only those two already-validated keys from that copy. Retain original inventory, source/image proof, all other settings and existing legacy budget checks. Any missing, enabled or unexpected flag is rejected before projection. Exercise the complete contract validation with four APIs, and replay the retained failed inventory through the corrected validator before deploying again.

Close this exact zero-dispatch failure only with the original failed report preserved and a fresh independent read-only proof of exact restored runtime, idle generator, empty secondary host, drained normal queues and removed owned credentials. Do not classify the paid stage or qualification as passing. Then invoke the proven bounded runner once at 84 offered journeys/s for 300 seconds under the existing approved envelope.

## Alternatives

Remove arbitrary settings: hides drift. Relax historical exact equality: weakens all old profiles. Change production API flags or images: changes behavior to solve an evidence schema mismatch. Replace the runner: unnecessary for this isolated compatibility correction.

## Consequences

The adapter admits explicitly verified historical-equivalent settings while retaining immutable actual evidence. Extra keys and pool changes remain visible to strict validation. This correction provides no capacity evidence.

## Failure and recovery behavior

Stop before dispatch if actual flags, images, budgets, queue ownership or fresh restoration cannot be verified. Keep failed reports and accounting blocked if recovery is ambiguous. Never reset Kafka offsets or discard acknowledged work. A paid-stage failure must retain its own cohort and pass mandatory durability, drain and restoration checks before any escalation.

## Validation evidence

Observed inventory: four APIs match the current contract; two extra disabled flags; both event groups and all durable queues have zero backlog. The failed report records zero capacity stages and complete physical restoration. New regression tests and replay of retained inventory must execute before the next cloud attempt. No paid improvement measured.

Executed validation: all 32 focused event-lane tests passed, including the complete contract path, rejection of missing/enabled/malformed API flags and unrelated pool/extra-key drift. The retained live inventory passed the full validator at its original observation time; its original bytes remain unchanged. This replay does not qualify a fresh runtime. Ruff passed; 535 historical inputs and all 20 current overlays verified without cloud calls or customer dispatches.

Executed cloud result: ADR0266 run adr0151-f80fef5b265c offered 84 journeys/s for 300 seconds. All 24,604 dispatched journeys received tickets; 596 were undispatched. All 129 initial-error journeys recovered in 134 retry attempts; final customer failures were zero. Journey p95 was 7.007 seconds, compared with 7.593 seconds in failed ADR0263; paid-unfulfilled peak fell from 348 to 94. Initial errors and drops increased, so capacity improvement is not qualified. Primary CPU/slot capture was incomplete; pipeline samples observed host CPU p95 97.22%, with API CPU 1.572 cores across four CCE pods. Both Kafka lanes drained. ADR0269 independent read-only recovery executed in 89.094 seconds and verified all 24,604 unique durable paid-and-issued tickets after TTL, both safety cohorts, zero double-booking/payment loss, all 86 owned shows retired, queues empty, credentials/helpers/namespace removed and exact stable normal runtime. The original failed report remains unchanged; terminal status is FAILED_RESTORED. See [public result](../capacity/cce/event-lane-paid-result-2026-10-10.json). No hourly qualification or higher load followed.
