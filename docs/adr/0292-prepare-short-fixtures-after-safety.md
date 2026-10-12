# ADR0292: Prepare short paid fixtures after expiry qualification

## Status

Accepted. Supersedes ADR0289's fixture-before-admission staging for five-minute profiles only. Retains ADR0289's hourly preparation order and ADR0185's freshness limits.

## Context

The corrected ADR0291 control (adr0151-eae70eed4853) passed native payment/replay/expiry safety but stopped before paid dispatch at fixture_identity: Fresh open fixture window required. The fixture was approximately263.53seconds old at failure-evidence capture, exceeding the unchanged120second short limit. All resources were restored and integrity verified.

ADR0289 moved bulk fixture preparation ahead of transition admission to prevent a large hourly fixture from aging machine inventory. Hourly fixtures allow600seconds of creation age; short fixtures allow120seconds. Preparing a short fixture before worker startup and mandatory hold-expiry safety necessarily risks exhausting that120second allowance. The earlier passing ADR0288 short stage created its paid fixture after safety.

## Decision

For hourly profiles, continue preparing the bulk paid fixture before refreshed transition admission. For short profiles, complete native transition and all safety gates, then create exactly one fresh isolated paid fixture and prepare dispatch inputs. Record which order executed. Do not change created_at, reuse expired fixtures or increase freshness limits.

Bind this staging distinction in the verification contract and preserve the existing image, application settings, resources, database budgets, customer recovery, workload and quality gates. Both ingress comparison arms use the corrected short staging order.

## Alternatives

- Extend or ignore the120second limit: rejected; it would conceal stale evidence.
- Change fixture timestamps: rejected; it would falsify provenance.
- Remove expiry verification: rejected; paid ticket durability must remain checked.
- Move hourly preparation after safety too: rejected; its bulk preparation caused the independently observed inventory-age failure.

## Consequences

Short preparation no longer includes worker provisioning and mandatory expiry time in fixture age. It still fails if actual post-creation preparation exceeds120seconds or the sale window closes. Hourly behavior remains unchanged. This scheduling correction does not improve application capacity.

## Failure and recovery behavior

Create each fixture once and retain exact show ownership before dispatch. Stop progression on unknown fixture identity, failed safety or freshness checks. Preserve the failed preparation evidence, audit safety payments and queues, retire owned fixtures and restore the original runtime before a fresh attempt.

Persistence, Redis atomicity, idempotency, seat expiry and payment correctness remain unchanged.

## Validation evidence

Executed before correction: the retained ADR0291 pre-dispatch failure established the120second guard/order conflict; native safety passed and paid_runs_started was0. Executed after correction:177 focused tests passed in33.33seconds, including a130second safety delay that preserves a fresh short fixture and still rejects121second-old evidence. Hourly bulk ordering and failed-admission cleanup regressions passed. Ruff passed after import/noqa cleanup. The fresh ADR0291 control and candidate both completed paid dispatch and passed all short-profile gates with exact restoration; see the [comparison](../capacity/cce/existing-ingress-comparison-2026-10-12.json). Both created their short paid fixture after native safety with the unchanged freshness limits. No performance gain is claimed. See the [retained fixture abort](../capacity/cce/existing-ingress-fixture-abort-2026-10-12.json).
