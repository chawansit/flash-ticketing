# ADR0242: Bind matched CCE transaction comparison

## Status
Accepted and locally qualified. No fresh cloud scope is registered yet.

## Context
ADR0239 removes redundant BEGIN. ADR0241 provides matched, registry-verified 22-module API images and bounded failure-time diagnostics. The proven CCE runner still requires the historical 21-module API. Its historical generator, worker images, budgets and financial audits must remain identifiable; replacing its API source map globally would misidentify the unchanged ECS workers.

## Decision
Extend the existing registered short CCE profile with an explicit ADR0242 discriminator and logical comparison arm. Keep the topology's historical internal candidate label distinct from the logical control/candidate. The control preserves explicit BEGIN; the candidate removes it. Both use the same diagnostics, four 1-vCPU/1-GiB API pods, four connections per API (two general/two payment), pooler 24, shared acquisition 20, wait 500ms, synchronous confirmation, existing simulator and generator, and 84 offered journeys/s for 300s. No retries or weakened gates.

Use immutable manifest and configuration digests from the verified public image receipt, and verify all 22 runtime sources. Preserve the original eight-role worker/restore contract separately using historical 21-module API identities. Bind all new helper/source/receipt hashes to the fresh experiment. Enable failure snapshots from the same admitted metrics response and require settled ring/counter completeness. Compress stopped observer traces using ADR0240, retain exact decompressed SHA/length, and keep all financial/drain/restoration checks independent of diagnostic failure.

Preserve ADR0238's original reproduction lock. Archive the exact original bytes of changed orchestration files and verify both historical hashes and an explicit working overlay manifest; never silently refresh historical digests. Historical generator and source artifacts remain unchanged. This is a versioned implementation extension, not a new traffic or resource allowance.

Register one fresh bounded scope per logical arm. A candidate requires a retained, fully passed and restored control receipt for the same image/source pair. Failed control, incomplete diagnostics or uncertain restoration blocks progression. Existing consumed scopes remain consumed. Hourly execution with these new images is not enabled by this extension; it requires successful short evidence and separate exact hourly binding.

## Alternatives
Replace the full backend image: includes unrelated worker changes. Weaken source admission checks: reintroduces image/version drift. Copy the entire runner: duplicates lifecycle and recovery logic. Attribute the historical timeout from aggregate metrics: lacks failure-time ownership proof.

## Consequences
API and worker identities can differ intentionally without confusing their receipts. Common diagnostics add equal overhead to both arms. A short pair measures one transaction change; it does not prove the historical timeout cause or qualify an hour. Compression fixes transfer limits, not cohort sampling gaps. Reproduction reports distinguish archived historical inputs from current approved overlays.

## Failure and recovery behavior
Reject unknown arms, changed receipts, changed helper/source hashes, mismatched configurations and incomplete failure counters before dispatch or at the relevant evidence gate. Do not retry ambiguous experiments. Preserve failed evidence; stop owned jobs, verify payment durability, zero double-booking and full queues, then restore captured topology and remove temporary credentials. A transport failure cannot skip financial verification or restoration. Escalate only unresolved recovery or changed business/infrastructure boundaries.

## Validation evidence
Executed local evidence: [transaction runner qualification](../capacity/cce/transaction-runner-local-2026-10-09.json). New binding/ownership regressions passed 28; combined runner and idle-boundary follow-up passed 268 with one Windows skip. Full unit suite passed 1,892, skipped two and failed one existing Windows idle timer assertion; this failed result is retained, not reported as a full pass. Ruff, repository names and reproduction verification passed: 535 historical inputs, six explicit current overlays, 78 frozen generator files. No cloud load or capacity improvement is claimed.

The first registered scope (`bounded_cce_paid_comparison__4206c3cffd9f`, run `adr0151-8b010c522d75`) failed before paid dispatch and restored completely in 301.484 seconds. Native common diagnostics were incorrectly used to validate the unchanged ECS API environment, requiring a flag absent from the historical containers. Explicitly validate the captured ECS settings and saved service against `legacy_contract()`, while native pod admission continues to require the new image/source/diagnostic contract. Complete capture, native activation, exact restoration and rejected legacy pod sources were then exercised for both logical arms. The corrected transition/profile/entry suite passed 110. No paid throughput was measured by the failed pre-dispatch attempt.
