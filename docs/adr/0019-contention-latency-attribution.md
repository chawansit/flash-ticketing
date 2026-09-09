# ADR 0019: Contention latency attribution

Status: Accepted; cloud experiment completed.

## Context
ADR 0018 found one durable winner at 100/1,000 contenders but client failed-response p95 of 317/3,636ms. Server 409 p95 across the waves was 26ms. Client dispatch, connection setup, pool scheduling and time outside middleware are confounded. Uvicorn counts open connections toward its unchanged concurrency limit of 256, so prewarming 1,000 connections would itself change admission behavior.

## Decision
Keep application source and all backend settings unchanged. Use the existing Server-Timing header and HTTP client's installed trace hooks, recording timestamps only, never trace payloads or authentication. Compare cold versus verified prewarmed connections and one versus four generator processes, holding total connection lanes at min(contenders,128). Each lane owns one HTTP/1.1 client/connection and serializes its requests with a measured lock wait. All contenders are released at a shared future start; record coordinator skew, dispatch spread, client queue time, TCP setup, send-to-response-header time, application duration, total release-to-completion time, event-loop lag and process CPU. Per-request residual outside application middleware includes networking and client scheduling; it is not a direct measurement of server ingress queue time.

Use 100 and 1,000 distinct viewers, two repetitions of each cold/warm x one/four-process combination, reversing order for repetition two. One fresh seat per wave; no retries. Warm up with /health/live, then coordinate start promptly. Trace absence of new connects on every warm request proves reuse for the measured wave; reconnects invalidate the warm comparison and remain in evidence. Cold means each lane starts disconnected; later queued requests may reuse its connection. Retain background reconciliation for the same 800-show fixture. Capture host TCP counter deltas as shared-host context, not request-specific proof.

## Alternatives
Raising server limits or changing admission would confound attribution. Calling every client-minus-app duration ingress wait would overstate evidence. A 1,000-connection prewarm can exceed the current server limit. Packet payload capture risks collecting authentication and is unnecessary for this first attribution step.

## Consequences
This controlled bounded-lane workload is not identical to the earlier 1,000-connection wave; report that difference explicitly. Queue time is counted in release-to-completion latency, never hidden to claim an SLO improvement. One versus four processes tests generator scheduling effects at equal total lanes. Two repetitions show variability, not statistical certification or a production maximum. No runtime persistence, messaging, idempotency, TTL or scaling decision is superseded.

## Failure and recovery
Retain all response and transport categories; distinguish one-winner correctness from admission and latency gates. Require complete request accounting and <=100ms worker start skew. Verify persisted counts for every worker run ID, including workers with zero acknowledgements, and verify each winning seat owner before hold expiry. Use expiring private manifests; stop benchmark services and remove credentials and temporary firewall rule after capture. Preserve synthetic volumes.

## Validation evidence
Two real-local-HTTP tests passed and Ruff passed. Executed 16 cloud waves (8,800 requests), all with one HTTP and live durable owner. All warm waves verified reuse with zero new TCP connects; worker start skew was below 1ms. Client and server timings/statuses matched all 8,800 request IDs. Final expiry validation covered all 40 worker IDs and 16 acknowledgements, with zero pending queues or active holds.

At 1,000 warm contenders, total p95 was 1,220–1,230ms with one generator process and 469–480ms with four; admission rejection increased from 774–811 to 944–950 requests. Client lane waits and event-loop lag decreased, showing generator contribution, while all cases failed the no-admission-rejection gate. No backend capacity improvement or pure ingress-queue timing is established. Host TCP counters cannot isolate API-container network behavior. See [full results and evidence](../capacity/contention-timing/README.md). Services/observer stopped and temporary credentials/network guard removed.
