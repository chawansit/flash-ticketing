# 0127 — Phase classification for bounded API stack diagnostics

- **Status:** Accepted on 2026-10-04 for offline implementation and one specifically approved diagnostic at unchanged load.
- **Related decisions:** Complements ADR0118 and ADR0126. Supersedes no accepted persistence, locking, messaging, idempotency, TTL or scaling decision.

## Context

ADR0126 passed all gates at 60 buyers/s for 300 seconds with the advisory order-status cache disabled. Sampled API CPU was 1.277 cores and host CPU was 80.14%. API was the largest application role. In the retained ADR0118 profiles with cache3000, 70.81% of 4,066 observations lacked endpoint frames. Shared framework, dependency and response paths could not safely be assigned to endpoints. No single hidden leaf dominated this gap.

## Decision

Extend the offline speedscope summarizer with one exclusive phase per stack, using the nearest recognized owning frame and generic runtime helpers as fallbacks. Recognize authentication, database, Redis, dependency resolution, response encoding, HTTP transport, request dispatch, thread dispatch, event-loop, instrumentation, logging and application contexts. Retain unclassified stacks. Preserve unknown endpoints and add the verified hold handler to the existing endpoint vocabulary.

Report phase weights, the phase breakdown of endpoint-unattributed stacks, and conservation checks. Labels describe stack context; they do not measure charged CPU or elapsed request time.

Reuse the unchanged ADR0118 collector: four verified uvicorn processes, 25 Hz, 120 seconds, GIL-only and nonblocking, without locals or native capture. Run one diagnostic at 60 buyers/s for 300 seconds with cache0 against the passing ADR0126 baseline. Generator, application, worker, connection, observer and TTL settings remain unchanged. Measure profiler child CPU and verify owned process and scratch cleanup.

## Persistence, locking, messaging and idempotency

Sampling reads process state and writes offline evidence. No schema, index, transaction, row-lock, connection-budget or reservation-writer fairness changes. Kafka, outbox, payment and callback authority, idempotency and replay behavior remain unchanged.

Hold and command TTLs, callback multiplicity1 and cache0 remain unchanged. Public evidence contains static symbols and redacted paths; raw profiles remain private.

## Scaling and alternatives

No higher load or additional service resources. Profiler overhead and fixture/background variation confound CPU and latency differences. Sample weights cannot be converted to charged per-route or per-phase cores. Inclusive frame-presence percentages overlap and must not be added.

In-process CPU spans would require careful async and worker accounting and would change request execution. Native or all-thread capture would change the sampling contract. Guessing endpoint identity from generic frames would misattribute work. These alternatives, and optimizations selected without sufficient evidence, remain future scope.

## Failure and recovery

Reject malformed, empty or mixed-unit profiles, invalid weights, conservation failures, missing or replaced targets, and collector failures. Retain unknown categories instead of inventing attribution.

No customer-load retries or observer-error waivers. Require exact financial correctness, durability, zero double booking, post-TTL checks, full-keyspace queue drain, Kafka drain, observers, CPU collection, source verification, service restoration, generator idle and private scratch cleanup. Reap owned profiler processes. A failed gate stops further cloud experiments.

## Access and authorization

The user's Continue approved the concrete saved-profile investigation and one unchanged-load diagnostic. The previous temporary key had been removed. Automatic approval review rejected a fresh root-key installation before execution because the earlier approval covered ADR0126 only.

The user subsequently replied Approved to the exact ADR0127 key request: both ECS root accounts, forwarding and PTY disabled, a 45-minute expiry, and removal after audits. Installation used protected terminal password input and verified host keys. No credentials were saved in files or environment variables.

The key uses restrict and a UTC expiry after verifying OpenSSH7.7 or later. Remove the entry after audits regardless of expiry. Other keys, SSH configuration and datastore settings remain unchanged. See [OpenSSH7.7 expiry support](https://www.openssh.org/txt/release-7.7) and the [authorized-key options](https://man.openbsd.org/sshd.8#AUTHORIZED_KEYS_FILE_FORMAT).

This scope includes no recurring work, second run, higher load, external publication or main merge.

## Validation evidence

Executed 64 focused profiling, phase, CPU and startup checks: passed in 0.62 seconds. After an equivalent startswith-tuple fix for Ruff PIE810, all 34 profiling tests passed in 0.09 seconds; lint and whitespace checks passed. Regressions cover owning-frame precedence, shared/auth/database/encoding/logging ambiguity, unknown and fileless frames, conservation, route preservation, redaction and malformed inputs.

All four retained historical profiles were reanalyzed without dropping samples. Their 4,066 observations conserve phase weights. Correcting the verified hold handler reduces missing endpoint labels slightly; shared endpoint attribution remains unavailable. Historical cache3000 results do not qualify current cache0 behavior.

Read-only cloud preflight confirmed all ten runtime hashes and restored service budgets match the passing cache0 baseline. The single bounded diagnostic completed; final gates and cleanup are recorded below.

- [Passing cache-disabled baseline](../capacity/flash-sale-opening/cache-disabled-control-2026-10-04.json)
- [Saved-profile phase evidence](../capacity/flash-sale-opening/api-phase-saved-profile-reanalysis-2026-10-04.json)
- [Bounded diagnostic plan](../capacity/flash-sale-opening/api-phase-diagnostic-plan-2026-10-04.json)

Cloud validation completed: checkout-20261004T020835Z-a401c0, unchanged60buyers/s300s cache0. All16 required diagnostic gates passed, including exact financial correctness, zero double booking, full queue/Kafka drain, observer and source verification, profiler/weight conservation, restored services and idle generator. Both temporary keys and local key files removed. [Result](../capacity/flash-sale-opening/api-phase-diagnostic-2026-10-04.json). Phase labels remain stack context rather than charged CPU; no production capacity qualification.

Current cache0 findings: 2,135 observations; database22.81%, request dispatch18.45% and HTTP transport15.60% of exclusive stack-context weight. Endpoint identity remains unknown for68.62%. Sampled API CPU1.2765to1.2781cores (+0.125%); profiler CPU1.746867seconds over119.984seconds. These observations select no optimization and do not establish a capacity improvement.
