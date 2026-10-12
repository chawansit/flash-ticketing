# ADR0286: Hourly qualification with complete verification

## Status

Accepted under the user's standing work envelope and 12 October 2026 Continue instruction. One fresh hourly experiment, with the existing 90-minute ceiling, unchanged resource/spending boundaries and no automatic rerun. Extends ADR0285's short-only verification scope to this separately registered hour. Historical failures remain failed; ADR0284 pipelining remains default off and is not adopted.

## Context

ADR0283 produced more than 300,000 tickets inside an hour but failed its canonical audit and one statistics interval. ADR0285 corrected the bounded financial audit, independent SQL sampling and complete worker membership; both fresh five-minute arms passed all quality and restoration gates. Pipeline-on did not improve database occupancy or ticket throughput. Retained ADR0283 evidence contains 38 slot failures, dominated by commit-phase holders; it does not establish the underlying WAL/storage cause. No slot failures occurred in the fresh short pair.

## Decision

Use the already supported ADR0281 hourly application topology and immutable ADR0267 application image, the same application runtime as the last ADR0283 hourly experiment. Deliberately retain manifest sha256:d9d7cb17433782ca863d8b93ef987a670708d4f6f9719fdecc2c4c21125d23fe. The newer ADR0284 pipeline candidate image is not selected. Verify manifest/configuration, all actual imported sources and role settings before dispatch. This is a deliberate fixed baseline selection, not fallback to an unverified old image.

Opt in to the unchanged ADR0285 bounded financial audit and independent diagnostic connection ownership. The existing sampler supports the hourly 3,780-second observer window and 1,008-show audit. Reuse the proven hourly generator, admission observer and lifecycle; do not create another runner or change application code. Seal the verification contract, source identities and fresh scope before dispatch.

Offer 84 distinct one-ticket journeys/s for 3,600 seconds (302,400 scheduled). Keep four API pods at 1 vCPU/2 GiB, fourteen workers at 250m/512 MiB, 24 physical pooler connections, admission20, existing local pools, two generator shards with 500 total slots, payment gateway delay, polling and three-attempt recovery unchanged. Require at least 300,000 unique paid-and-issued tickets inside the hour, including the existing conservative two-second boundary guard. Post-window completions are reported separately.

Record account quota before preparation and after completion without account identifiers. The rounded difference is account-wide and can include other chats; cloud ticket requests do not themselves invoke the LLM.

## Alternatives

- Enable the pipeline candidate: rejected by the mixed ADR0285 comparison.
- Change slots, connection counts, worker placement or gateway timing: confounds the qualification.
- Repeat a short test indefinitely: cannot qualify a sustained hour or reproduce intermittent stalls reliably.
- Relax statistics, financial or issuance gates: conceals missing evidence or weakens the target.

## Consequences

This run validates duration and corrected verification rather than a performance optimization. It may expose intermittent payment errors, worker congestion or storage waits. Initial failures, retries, final failures, generator drops, occupancy and queues remain visible. It cannot establish maximum capacity, flash-opening burst capacity or the cause of an unobserved commit stall.

## Failure and recovery behavior

Stop escalation when any gate fails. Retain failure-time diagnostics and original results. Complete post-TTL financial relationships, zero double-booking/payment loss, full queue drain, exact topology restoration and credential/resource cleanup independently of the quality result. Never automatically replay the hour. Preserve ownership evidence before container cleanup. The original total experiment bound is 5,400 seconds, with mandatory safe cleanup after a deadline if necessary.

## Validation evidence

Executed 12 October 2026. The existing hourly scope plus unchanged ADR0285 verification passed 38 focused tests under an isolated pending hourly envelope. Cloud image/source/configuration and all fourteen worker receipts were verified before dispatch. All native cross-pod safety, replay, duplicate callback, authorization and post-TTL checks passed, including exactly one accepted hold from 100 simultaneous requests.

The offered window was 14:23:00–15:23:00 Bangkok time. All 302,400 scheduled journeys dispatched and completed; 302,280 unique paid-and-issued tickets were inside the measured hour (83.967/s), including 302,068 in the conservative inner window. There were no generator drops or final customer failures. Forty-eight initial payment503 errors (0.015873% of dispatched journeys) recovered in forty-eight retries. Payment p95 was99.489ms; total journey p95 was2660.230ms, reported as the worst shard p95 rather than a combined percentile.

The canonical full-cohort financial audit and independent database sampling passed. There were no duplicate bookings, lost payments or relationship errors; all queues drained. Four API processes used1.248 cores and fourteen workers1.448 cores in aggregate. One-second database snapshots observed WALWrite waits but do not establish the cause of intermittent commit stalls.

The original complete qualification failed: live admission log capture ended with188 samples and two ValueError entries; final600-second log retrieval reached the8MiB byte cap on allfour API pods. The earlier live capture error's exact reason is not retained, so it must not be attributed to this final cap without proof. Original diagnostic completeness and failure remain unchanged.

Strict reanalysis of immutable metrics plus validated partial live records reconstructed all72 slot-failure sequences and reconciled the settled terminal counters. Commit-phase holder observations dominated (129; holders can repeat across failures), with maximum phase age664.239ms. This supplemental reconstruction does not restore missing log coverage or prove WAL/storage causality.

Independent read-only terminal verification completed in151.265 seconds. It reverified302,400 paid tickets, both safety cohorts, zero global duplicate bookings, empty queues, fixture retirement, generator idleness, exact original runtime stability, namespace/helper removal and private credential/input cleanup. Ledger status is FAILED_RESTORED; no new load was started. The measured hourly ticket target is met, but complete production/hourly qualification and maximum/flash-opening capacity remain unclaimed.

Evidence: [hourly result](../capacity/cce/complete-verification-hourly-result-2026-10-12.json) and [independent terminal recovery](../capacity/cce/complete-verification-hourly-terminal-recovery-2026-10-12.json). Account-wide quota rose from26% to27% used (rounded1percentage point); this includes preparation/recovery and may include other chats.
