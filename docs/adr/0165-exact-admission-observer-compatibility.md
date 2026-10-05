# ADR0165: Exact admission observer compatibility and fresh qualification

- Status: Accepted for implementation and bounded qualification; performance unmeasured
- Date: 2026-10-06

## Context

ADR0164 cloud staging passed. Its control safety probe produced exactly one winner among 100 requests, one durable paid-and-issued ticket after TTL, zero duplicate bookings, and complete global drain. However, the pipeline observer rejected the profile before measured dispatch. The new profile contains explicit reclamation, event-refresh, polling and receipt settings beyond the historical API schema. Its compatibility view removed only two settings, so the historical exact settings comparison failed. The runner later reported observer startup timeout. Original services were restored and no paid load stage started. This is a harness compatibility defect; it is not evidence of backend saturation or a capacity improvement.

## Decision

Verify the complete admission API settings dictionary and exact profile marker first, including every original connection budget and fixed feature value. Only then project a deep copy to the historical settings keys for structural validation. Preserve the original inventory evidence and all source, topology, financial, observation, freshness and recovery gates. Add tests through the real observer installation boundary using both arms and the captured failed inventory; reject unknown settings and every fixed-setting drift.

Close the consumed ADR0164 qualification allowance without resetting its counters. Create a distinct bound ledger and authorization for one fresh off/on qualification protocol (at most two safety tickets). Only after qualification and restoration pass, permit one fixed 60 journeys/second, 300 seconds per arm comparison (at most two measured stages and two additional safety tickets). Images, source, machine sizes, aggregate connections, polling, synchronous confirmation and gates remain unchanged. Stop after failed control. This fresh scope follows the user's existing authorization to diagnose failures and continue; it does not reopen a failed protocol or authorize automatic replacements.

## Alternatives

Discarding unrecognized settings before validation could conceal drift. Relaxing exact comparison globally would weaken historical profiles. Changing backend code or increasing budgets would confound the experiment. Reusing the consumed ledger would obscure dispatch history. None is selected.

## Consequences

The adapter supports the exact extended admission schema while retaining the frozen historical structural checks. Qualification costs one additional explicitly bounded protocol. A passing safety check still does not prove throughput improvement or resolve the separate 18,000-seat projection issue.

## Failure and recovery

Reject missing, unknown or changed settings before adapting a copy. Retain the shared lock on ambiguous cleanup; reserve before potentially ambiguous dispatch. Preserve prior consumed counters and reports. Require exact image/source identity, post-TTL durability, zero double-booking, complete financial and Kafka queue drain, and original runtime restoration. No customer retries or deadline extensions are introduced.

## Relationships

Corrects ADR0164 observer integration; ADR0163 source/images and ADR0162 reclamation remain unchanged. Supersedes only the remaining unused ADR0164 experiment allowance with a fresh ledger; the failed qualification remains recorded.

## Validation evidence

Both new real-installation tests failed before correction with the same settings equality exception. After correction, 252 runner/observer regression tests passed. The actual failed cloud inventory passed a local captured-time replay through the real observer installation boundary with four endpoints and original evidence unchanged; no network calls were made. Fresh cloud qualification/comparison is pending. Prior failed qualification: `adr0151-6cf227629f33`. No measured capacity stage started.
