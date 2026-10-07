# ADR0195: Guarded worker execution and exact restoration

## Status
Accepted for local implementation and fault qualification under ADR0172, ADR0184, ADR0186 and ADR0194. The worker profile remains unregistered. This component does not create connections, dispatch customers or qualify cloud capacity. No accepted placement decision is superseded.

## Context
Sealed configuration is available, but changing broker configuration and moving workers still need concrete ownership checks and rollback operations. The original Kafka volume must survive a broker restart. Lost responses can leave a partial worker start, and a retry can overlap processing. Compose also interpolates dollar signs even in JSON configuration, so serialized literal runtime settings require the existing literal-model escaping before use.

## Decision
Use the existing fixed-factor pair and original runtime snapshot. Serialize configuration with the established Compose literal escaping and require fresh seals; do not reuse older document hashes. Add a transport-bound execution component sharing the original ActionGuard, session, scope and private journal. Keep the profile unregistered until dependency readiness, full financial/queue audits, observers and complete lifecycle integration are qualified.

Require the ADR0188 original worker stop and independent absence acknowledgements before common infrastructure configuration. Apply only the four whitelisted infrastructure roles on the primary. Start only the seven exact worker roles and fixed replica counts on the selected host after proving both hosts have no workers. Preserve one broker, its exact external named volume, unchanged logical connection budgets and four primary APIs.

Before each mutation, validate complete observations of both hosts, generator idleness, and a fresh scope-bound dispatch-stop/full-queue/Kafka-drain receipt. Write the intent before the remote call. The remote action rechecks the target observation and sealed document, supplies configuration over standard input, uses immutable images with pull/build/dependency startup disabled, and never runs project-wide down, volume deletion or image pruning. Verify literal Compose settings before applying them and validate the resulting full observations independently.

After an ambiguous start, inspect partial worker layouts against the exact intended model before selecting immutable container IDs for cleanup. Reject foreign, replaced, oversized or overlapping resources. Recheck each target's full identity before exact-ID removal. An unknown outcome blocks forward actions and cannot be replayed. Owned cleanup remains possible during pause or deadline expiry, with exact scope/transport binding and mandatory audits supplied by the eventual lifecycle owner.

Restoration requires workers absent on both hosts, exact original bind hashes and broker volume, and only infrastructure matching either the saved original or prepared common model. Apply the sealed original restoration model and original replica counts, then independently verify complete original runtime semantics, the retained volume, unchanged bind files and an empty secondary. Configuration cleanup is deferred until restoration and all financial/queue checks pass.

## Alternatives
Generic Compose down or role-filter deletion loses exact ownership. Replaying an uncertain up may change replica identities. Restoring only the broker environment omits actual API/worker runtime overrides. Moving Kafka storage adds a separate migration and recovery risk. Registering deployment before lifecycle qualification bypasses the mandatory readiness and customer/financial gates.

## Consequences
The component implements the missing mutation and restoration operations, but cannot certify private connectivity, broker readiness, payment durability or capacity by itself. Partial or mixed infrastructure observations must fail safely unless they match the exact saved/prepared alternatives. Local fake Docker and isolated Linux tests do not establish cloud qualification. Private documents and observations remain private; public evidence contains selected results and hashes.

## Failure and recovery behavior
Every action is single-use. A failed control stops progression. Lost acknowledgements preserve intents and prevent replay, while independently verified owned cleanup is attempted by the lifecycle. Any uncertain worker absence, volume identity, bind content, restored semantics or journal persistence retains recovery ownership and blocks more load. Financial and full queue audits cannot be replaced by a successful Compose command.

## Validation evidence
390 affected tests passed in 45.76 seconds with no skips. Eleven generated-operation cases ran on real Linux with simulated Docker; fourteen existing configuration cases exercised real Linux file operations. A separate real local Compose probe preserved dollar signs exactly in both environment and command values and removed its owned container. Ruff, repository naming and whitespace checks passed. No owned test containers remained.

The initial execution fixture omitted materialized image environment defaults (33 failures, 38 passes); correcting the fixture produced the passing component and regression runs. Real Compose configuration retained dollar escaping in its serialized output; the verifier now checks that representation while the actual container probe verifies the unescaped runtime values. Previous ADR0194 evidence and hashes remain historical; new documents require new seals.

Partial starts, lost responses, model/seal/identity/volume drift, journal failure, pause and exact restoration were tested. Original-stop acknowledgements in the execution unit tests were emulated; these checks do not constitute full live lifecycle integration or financial qualification. The worker profile remains unregistered. No cloud mutation, load or capacity improvement was measured. See [local execution evidence](../capacity/flash-sale-opening/background-service-separation-execution-2026-10-07.json).
