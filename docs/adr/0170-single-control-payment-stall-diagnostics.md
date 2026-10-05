# ADR0170: Single-control payment-stall diagnostics

- Status: Accepted for bounded orchestration implementation; cloud qualification pending
- Date: 2026-10-06

## Context

ADR0165 control fulfilled 18,000 journeys at 60/s for 300s with no customer errors, but two callback native-pool timeouts recovered through existing provider deliveries. Reclamation candidate failed one customer payment and was rejected. Adjacent samples show payment slots exhausted during commit spikes; they do not establish causality. ADR0166 collects bounded structured failure snapshots before teardown and passed 271 local tests. Its cloud capture has not been qualified. The user requested continued evidence-led work and fewer routine approvals within existing limits.

## Decision

Introduce one fresh control-only diagnostic scope using the unchanged ADR0163 immutable application images and synchronous intake, reclaim disabled, fixed 2+2 API topology, existing aggregate connection budgets, cache/polling controls and existing machines. First qualify one control safety protocol (one ticket). Only after all safety/identity/drain/restoration gates pass, run one 60 journeys/s, 300 s control stage and its one safety ticket. Maximum one qualification protocol, one paid stage and two safety tickets. Preserve the consumed ADR0164/0165 ledgers unchanged. No candidate, increased load, infrastructure purchase, automatic replacement of failed protocols or runtime optimization.

Generalize the shared runner's ordered arms into an explicit validated profile: historical pairs remain unchanged, while this exact ledger allows only control. Keep the shared deployment, strict identity proof, bounded generator, financial/post-TTL audit, failure retention, queue drain and exact restoration lifecycle. Bind source/configuration/images/adapter identities before cloud access. Use the canonical staging-directory factory from ADR0169. Capture fails closed if incomplete. Compare to the identified ADR0165 control only as a diagnostic baseline; this is not a performance optimization comparison.

## Alternatives

Repeating the rejected candidate adds work without addressing payment failures. Increasing slots or timeouts before attribution changes the bottleneck. Reopening consumed ledgers destroys auditability. Duplicating the cloud engine creates drift. None selected.

## Consequences

A smaller control-only protocol captures failure evidence with unchanged runtime conditions. A run with no timeout qualifies collection lifecycle but cannot explain an intermittent timeout. Source and images remain frozen; local harness changes do not demonstrate higher capacity. Performance summaries retain CPU, database waits, latency and completed tickets.

## Failure and recovery

Reserve counters before ambiguous dispatch. Stop on failed qualification or control; collect diagnostics, then continue independent financial/drain/owned cleanup and restore. Preserve failures and spent counters. Retain ownership lock when restoration is uncertain; do not replay. No higher-load or asynchronous stage follows automatically.

## Validation evidence

Executed 223 local unit tests passed in 29.29 seconds, covering single-arm lifecycle/counters/identity, historical pair behavior, retention and naming. Changed-file Ruff passed. Default preparation made zero cloud calls. [Local validation](../capacity/flash-sale-opening/payment-stall-diagnostics-local-validation-2026-10-06.json). Cloud qualification is pending. Direct user continuation authorizes preparing this bounded scope; qualification and measured execution occur only after local checks and strict preflight. No cloud calls or capacity improvement claimed by this ADR at creation.
