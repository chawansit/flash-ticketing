# ADR0333: Repeat 160 journeys/s with four generator processes

## Status
Executed; failed and operationally restored. One paid-stage allowance consumed. ADR0335 proposes a separately selected active-capacity comparison.

## Context
ADR0329 failed arrival timing and dropped125 journeys with two saturated generator processes. ADR0332 verified the sealed paid leaf in synthetic two/four-process160/s controls and a four-process200/s control. These do not qualify backend capacity. The user authorizes continuing to integrate four processes and repeat real160/s traffic.

## Decision
Run one fresh160 offered journeys/s300-second paid stage,48000 journeys across160 isolated shows, using four physical leaf processes instead of two. Divide total active journeys500, HTTP connections500 and HTTP clients16 into125/125/4 per process. Reuse the sealed leaf/recovery behavior, synchronized start and timeout evidence retention. Keep the ADR0324 image, four API/fifteen worker pods, database backends24, shared admission20, API2+2 slots, gateway timing, polling, retries, TTL and financial checks unchanged. Retain historical two-process profiles; require an explicit source-bound ADR0333 discriminator for every changed coordinator/accounting path. Report all four processes; do not disguise them as two shards.

Require the passed140/s reference and the retained failed160/s comparison. Allow only the declared generator process split (plus already qualified ADR0330 primary-only bootstrap) when comparing source/configuration bindings. Preserve final customer failure target0.01%, existing latency gates, zero drops, zero financial violations, complete queue drain and exact restoration. Require at least47400 conservative paid-issued tickets inside the300-second window. Bound the whole experiment to3600seconds. A failure stops escalation and preserves original evidence.

## Alternatives
CCE migration introduces placement and resource changes. Increasing concurrency first confounds occupancy with CPU effects. Leaving two processes would repeat the measured saturation. More application pods or database connections have no measured justification in this comparison.

## Consequences
Only generator CPU distribution changes. It can improve offered-load fidelity; it is not an application performance correction. Aggregate concurrency500 may still be insufficient if downstream latency increases. Do not silently raise it or weaken drop gates. Source-bound validation must distinguish four-process evidence from historical two-process results. No accepted persistence, locking, messaging, idempotency, TTL or application scaling decision is superseded.

## Failure and recovery behavior
Fail before mutation for source/image/configuration drift or unavailable dependencies. Stop load escalation on missing leaf results, invalid exit codes, drops, late dispatch, latency/SLO failure, payment/seat inconsistencies or queue backlog. Preserve failed evidence and consumed identity. Complete post-TTL reconciliation, remove owned helpers/pods/secrets and restore runtime. Never resend ambiguous payments without querying their operation under the same idempotency key.

## Validation evidence
Pending focused regressions, full160-show financial audit compilation, source binding comparison and live preflight. Append executed checks and cloud outcome; no new capacity claim at decision creation.

### Executed local admission

294 focused tests passed with unit fixtures isolated from the operator's live envelope. The full160-show/48000-ticket terminal audit compiled; complete four-process and primary-only source/configuration binding passed. The535-file historical reproduction check passed after adding the explicit generator helper and calibration receipt to the current overlay. The sealed leaf, recovery client, customer authorization, payment policy and historical two-process artifacts remain byte-identical.

Initial read-only cloud attempts correctly rejected a stopped dependency topology before any paid-stage reservation. Kafka, PgBouncer and the ECS load-balancer were restarted only after their container fingerprints matched the saved qualified baseline. No containers were recreated or resized. Primary disk headroom was approximately1.65GiB; an optional old build-cache purge was rejected by automatic approval review and was not executed. The protected wrapper records disk space immediately before dispatch and refuses traffic below1GiB. Preserve all raw evidence and application data.

The complete reservation path also passed using isolated copies of the envelope, journal and state. The standing resource-configuration digest was advanced from the historical three-host configuration to its already-qualified ADR0330 primary-only equivalent; the old digest is retained in configuration history. This registers removal of the retired host, not a machine resize or additional spending. Read-only registry/source/readiness checks passed with the fresh user-provided SWR login. No real reservation was consumed by the offline check.


## Live result, 2026-10-17
[Run evidence](../capacity/cce/four-generator-160-result-2026-10-17.json): `adr0151-89d76200cc84` offered160 journeys/s for300 seconds with four processes and the unchanged500 active-journey,500 HTTP-connection and16-client budgets. All native safety gates and observer startup passed. Four complete raw reports account for48000 scheduled,40095 dispatched/completed/paid/issued,7905 active-capacity drops, two initial payment503s (both recovered), and zero final customer failures. PostgreSQL timestamps show39650 issued within the offered window and39370 in the conservative inner window (131.23/s); this does not qualify160/s. Worst-shard dispatch p95 was below3ms; generator CPU was approximately0.55 cores per process. Mean successful journey residence was3.68s, implying roughly589 concurrent journeys at160/s before headroom, above the fixed500 cap. This estimates a generator-capacity constraint, not the effect of raising concurrency or a service capacity ceiling.

Both rejected payment requests captured two payment-slot holders in commit (approximately250-300ms). The deeper commit-delay cause remains unconfirmed. Complete one-second database observations include WAL/transaction lock waits, but unavailable WAL timing and a missing after-query snapshot prevent definitive query/storage attribution. The wrapper's failed dispatch path skipped that supplementary snapshot; do not claim complete SQL attribution.

Two coordinator completeness expressions still assumed two shards. This marked the original aggregate incomplete although all four final raw reports were retained. Correct the explicitly selected adapter's final/count/checkpoint checks to four. Tests execute the actual coordinator run with fake children for clean success, complete failed runs, missing results, valid checkpoints and invalid checkpoints.101 focused tests passed in5.96 seconds. This reporting correction is offline-tested and has not been cloud-retested; it changes neither the original result nor load behavior.

[Read-only operational recovery](../capacity/cce/four-generator-160-recovery-2026-10-17.json) independently verified all40095 dispatched payments/tickets, both safety cohorts, all financial relationships, zero double-booking, empty queues, stable restored runtime and absent owned CCE resources. The original result hash is unchanged. The ledger is FAILED_RESTORED; no recovery traffic or new paid stage was run. The single paid-stage allowance is consumed. Best qualified clean short throughput remains138.49/s fromADR0328; four processes alone did not demonstrate improvement.
