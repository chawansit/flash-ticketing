# ADR0213: Bounded worker manifest and zero-dispatch recovery

## Status
Accepted and executed for independent zero-dispatch recovery only. The manifest-verifier correction and automatic new-runner abort recovery are deferred; their proposed supersession of ADR0198 is not implemented. All paid qualification gates remain unchanged.

## Context
Run adr0153-parents-c6ab818e9ef8 passed live diagnostics and retained an exact 60-show fixture, but transfer verification rejected the manifest before any customer claim or generator launch. The verifier caps every file at one MiB although the fixed 18,000-token manifest read and owned cleanup already permit 16 MiB. Recovery restored the runtime and drained queues, but full expected 18,000 payments could not pass for zero dispatch, so private artifacts and ownership correctly remain blocked.

## Decision
Keep helper-source transfer capped at one MiB and permit only manifest.private.json up to the existing 16-MiB manifest limit. Bind expected exact bytes, size and SHA before transfer; preserve owner, mode, link, file-type and bounded-read checks. Exercise the generated verifier with a realistically sized 18,000-token manifest.

For failed preparation with a retained fixture and provably unconsumed paid allowance, audit that exact fixture for zero financial rows and post-TTL validity. Require original binding, unchanged fixture receipt, no attempted paid arms or customer launch, and all paid jobs stopped. Use a distinct receipt; never turn a pre-dispatch abort into a passing paid experiment. Any consumed allowance or uncertain launch still requires the full expected paid audit. Retain full duplicate-booking, queue, original-runtime, generator-idle, exact artifact and configuration cleanup gates. Extend independently verified recovery registration for this exact zero-dispatch case without rewriting original results or resetting counters.

## Alternatives
Raising every source limit weakens unrelated bounds. Reducing the manifest buyer count changes the workload. Treating no customers as a successful paid stage invents capacity evidence. Ignoring financial recovery or clearing state manually would weaken correctness and ownership.

## Consequences
This repairs harness preparation and mandatory abort recovery only. No backend images, resource sizes, customer SLO, transaction or financial guarantee changes. Fresh experiment identities remain mandatory.

## Failure and recovery behavior
Oversized or changed manifests fail before traffic. Unknown dispatch or nonzero fixture financial rows block cleanup certification and more load. Preserve private evidence until all independent recovery gates pass. Original failure remains immutable and no capacity improvement is claimed.

## Validation evidence
Original result RECOVERY_REQUIRED, restored runtime, zero paid-run counter, duplicate bookings zero and queues drained. Generator failure is the generated verifier's size assertion. Corrected generated-transfer, zero-dispatch financial/recovery and connected-runner tests pending.

## Implementation scope after performance-focus instruction
On 2026-10-07 the user directed use of the proven paid-load runner and deferred further worker-separation runner work. Implement only independent verification and cleanup of the already owned zero-dispatch fixture abort. Manifest correction and automatic new-runner preparation recovery are deferred. Recovery receipts preserve the original failed result and all consumed counters; they authorize no paid experiment.

## Executed independent recovery

The retained preparation abort was independently reconciled with zero fixture holds, orders, payments and tickets, zero global duplicate bookings, complete queues/Kafka drain, exact original service restoration and sealed private-artifact cleanup. Recovery completed in 75.265 seconds. The original failed receipt and zero paid counters remain unchanged. The 78 recovery unit tests passed in 0.67 seconds. The manifest/new worker-runner fix remains deferred. [Sanitized recovery evidence](../capacity/flash-sale-opening/zero-dispatch-fixture-recovery-2026-10-07.json).
