# ADR 0121: Bounded payment callback connection reuse

Date: 2026-10-03
Status: Accepted for an isolated implementation experiment; capacity validation pending

## Context

ADR 0120 improved durability p95 from 4.57s to 2.44s without materially changing paid throughput. The matched cloud window measured callback delivery mean 174.12ms, acknowledgement 15.36ms and due-to-claim 8.90s; the host used 96.95% CPU. Repeated order polling also remains expensive. Neither observation establishes the network share of callback latency.

The current development simulator uses urllib.request.urlopen for each signed callback. An isolated HTTP/1.1 diagnostic confirmed six callbacks created six TCP connections and all carried Connection: close. Connection reuse is an independently testable transport correction; any capacity gain must be measured.

## Decision

The simulator worker owns a synchronous HTTP/1.1 connection pool sized exactly to its existing delivery concurrency. Use Python standard-library HTTPConnection/HTTPSConnection with default certificate verification for HTTPS; no new dependencies. Construct connection objects without opening sockets; borrow one per callback, send the unchanged signed raw bytes and headers, fully consume the response, and return it after use. Requests remain bounded by the existing five-second socket timeout; pool acquisition is also bounded at five seconds.

Initialize the pool only in the development simulator, pass it through batch/refill dispatch, shut down the existing executor before closing the pool. Direct simulate_one calls retain the existing injectable legacy sender when no transport is supplied, so existing unit/integration seams remain available; the worker main path uses the pool. Callback signing, database claims, fifteen-second leases, delivery IDs, acknowledgement, idempotency and slot/connection/database budgets are unchanged. Source checks must include the new transport module in every relevant image.

This supersedes only ADR 0111's per-call callback transport detail; continuously refilled bounded dispatch and existing at-least-once lease recovery remain accepted. It does not change real payment-provider transport, payment authority or the booking architecture.

## Alternatives

- Increase delivery slots: does not reduce per-request cost and confounds the existing budgets.
- Retry a stale connection immediately: may replay an ambiguous successful payment without exposing the first failure; not part of this experiment.
- Add httpx/requests: capable pooling but introduces dependency/lockfile changes for a narrow standard-library transport.
- Change order polling simultaneously: prevents attributing results to one factor.
- Share one connection across concurrent callbacks: response/body ownership is unsafe and serializes delivery.

## Consequences

At most the existing delivery-slot count of connections can be borrowed concurrently. A complete response permits reuse; server-driven connection close reconnects on a later independent delivery. Failed sockets are closed. Actual benefit may be small because measured callback wall time includes server processing and database contention. XPENDING collection diagnostics from ADR 0120 will now be retained by the corrected observer; this measurement-only difference is disclosed.

The endpoint is fixed trusted configuration: require HTTP/HTTPS with a host and no userinfo/query/fragment. Reject redirects and every non-2xx response rather than following a changed destination with signed payment data. The current webhook endpoint returns a direct response; redirect rejection is an explicit tightening of transport behavior.

## Failure and recovery behavior

Send, response, timeout and non-2xx failures close the connection and raise without an immediate resend or database delivery acknowledgement. An ambiguous accepted callback remains eligible for the existing fifteen-second lease recovery with the same payment/callback ID and fresh signature; backend idempotency prevents duplicate payment or tickets. Pool slots are returned even after failures. Shutdown drains existing tasks before closing sockets; closed pools reject new work. DNS/TLS are resolved on a new connection when reconnecting, with default HTTPS validation. No durability, TTL, callback or queue gate is relaxed.

## Validation evidence

Executed six-request isolated urllib diagnostic: six accepted TCP connections, six Connection: close headers; owned server stopped. No cloud capacity claim.

Planned real HTTP tests: connection reuse, exact bytes/signatures, forced server close, non-2xx/redirect rejection, dropped response without immediate retry, bounded concurrent ownership, failure slot recovery and shutdown. Integration must prove pooled failures retain the existing lease and later idempotent delivery, plus existing booking/payment/replay correctness. Then one same-load 60 buyers/s, 300s, eight-slot candidate versus ADR 0120; all exact post-TTL paid/unpaid, zero-double-booking, full queues/Kafka/source/restoration/readiness/idle/private cleanup gates remain mandatory. Failed load prohibits escalation/main merge/promotion. Executed evidence will be appended.

Executed local validation: 467 unit/integration tests passed in 79.47s with two existing dependency warnings. Twenty focused transport/dispatch tests passed in 7.84s. Real HTTP tests verified six callbacks reuse one connection, forced close reconnects only for a later call, redirects/429/503 and dropped responses never immediately resend, failure returns the pool slot, concurrent requests do not share or exceed two connections, acquisition is bounded and shutdown rejects new work. A real PostgreSQL pooled-callback test proved a failed send leaves deliveries zero and its lease intact; later three deliveries retain one callback ID, one successful payment, one booking and one OrderPaid event. Existing full-suite duplicate fulfillment, transaction safety, Redis recovery and 100-way seat race passed. Owned isolated test containers removed. Cloud control pending.
