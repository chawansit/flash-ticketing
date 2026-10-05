# ADR0164: Bounded partial-timeout reclamation cloud comparison

- Status: Accepted for implementation and bounded qualification; performance unmeasured
- Date: 2026-10-06

## Context

User authorizes runner integration, controlled comparison and subsequent evidence-driven improvements, with stop after failed control and all financial gates retained. ADR0163 has pinned213-file/21-module source, six immutable images,32profile checks and272native passes. PriorADR0161scope is closed. Callback reserve and unqualified projection changes are excluded; previous18,000-seat projection failure remains unresolved.

## Decision

Reuse ADR0151/0161 orchestration through a private exact profile, shared run lock and a new bounded_partial_timeout_reclamation ledger. Bind exact source/image/config/adapter/observer/plan hashes. Keep fixed2+2API,all physical/shared/native connection/waiter ceilings,deadlines,polling,status cache/refresh,simulator10and idle confirmer2connections. Both API arms have synchronous intake and common diagnostic code; only reclamation0->1 changes. No customer retry.

One fresh qualification protocol contains off/on100-way safety,replay,authorization,post-TTL and full-global-drain checks (max2simulated tickets). Only if both pass and restoration succeeds, one matched60journeys/s300s control/candidate pair may start(max2paid stages plus2safety tickets). Stop after any failed control; unused allowance is not replay authorization. Prior ledgers/counters remain unchanged. The broader user plan authorizes later work, but higher-rate,async,hourly and opening-burst stages need their own exact profiles/qualification decisions before execution.

Require confirmation-receipt/counter/review drain even though intake is off; inspect migration009read-only without altering schema. Retain the idle worker until queues drain and remove it through existing restore hooks. Reuse protected password authentication and known-host pins;no new keys. Staging must preserve running container identities and clean exact owned archives.

Observer validates explicitADR0163factor/settings before adapting legacy structural assertions. Preserve original inventory evidence; copied compatibility views are validation-only. Add fixed role/reason acquisition-failure counters to API observations,with finite nonnegative values and reset checks;zero is valid when nofailureoccurred. Failure-time structured snapshots already shipped in both images provide rejection cause;coarse observer samples are not atomic proof. Preserve32existinggates plus receipt/confirmation coverage gates,latency,paid issuance,durability,zero-double-booking,post-TTL and complete global queues/Kafka.

## Alternatives

Reopening ADR0161violates consumed scope. Copying the runner duplicates recovery paths. Increasing budgets/retry or changing async intake confounds the factor. Skipping failed control,queue or durability gates would invalidate the result. None selected.

## Consequences

An updated synchronous control includes common diagnostics and need not reproduce historical throughput. A passing pair measures reclamation benefit at this fixed load only;nohourlycapacityclaim. A failed control triggers diagnosis and blocks candidate/increase. Existing historical profiles must remain compatible and need regression checks after generalizing the shared wrapper.

## Failure and recovery

Validate both real profile/stage constructors before reservation,lock or SSH. Reject stale images,missing settings,old/mismatched ledgers and consumed protocols. Conservatively reserve before ambiguous dispatch;never automaticallyreplay. Scripts own bounded waits,stop rules,per-armrestore and full global audits. Retain run lock on ambiguous cleanup and diagnose exact owned resources. Do notremove confirmationworkerbefore receipt drain or relax correctness gates to save time/tokens.

## Relationships

Activates ADR0163's exact admission-only profile through existing ADR0040/0090/0151/0161 bounded controls. Extends runner/observer supported profiles;does not supersede financial authority,idempotency,TTL,locking,messaging or priorfailedreports. ADR0162reclamation remainsdefaultoff outside this experiment.

## Validation evidence

Executed: 244 runner and observer regression tests passed, including historical profile compatibility, real constructors, exact factor and budget checks, stale scope rejection, synchronous receipt expectations, cleanup, and admission diagnostic coverage/reset checks. Changed-file Ruff and Git whitespace checks passed. An earlier test attempt had one mock-signature failure; it was corrected before the passing run. Cloud staging, safety and measured comparison are not yet executed. See [runner qualification](../capacity/flash-sale-opening/partial-timeout-runner-local-validation-2026-10-06.json). [Pinned profile evidence](../capacity/flash-sale-opening/partial-timeout-profile-local-validation-2026-10-06.json).
