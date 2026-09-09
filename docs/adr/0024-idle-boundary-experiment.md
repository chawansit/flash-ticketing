# ADR 0024: Read-only idle-boundary experiment

Status: Accepted; diagnostic experiment completed. No production pooling change adopted.

## Context
Two traced 400 RPS baseline runs did not reproduce the prior ReadError. The configured server keep-alive timeout and default HTTPX pool expiry are both five seconds. A connection-expiry race is a hypothesis, not an established cause.

## Decision
Add a separate bounded read-only probe. Each independent client seeds an availability read, waits around the declared server idle timeout, and issues one probe read, optionally conditional. Record actual client idle duration, connect attempts, local ports, response request IDs/status, bounded transport phases and safe exception chains. Compare generator-only pool expiry policies 5s (current), 30s (server-close observation), and 2s (early client expiry), with 16 independent clients, five delays (4.95/4.995/5/5.005/5.05s), three repetitions per policy. No retries, holds, payments or production pooling changes. Full responses are consumed. Keep server timeout at five seconds.

## Alternatives
Longer load soak may catch the intermittent failure but mixes many factors. Immediately changing production keep-alive could hide the symptom without causal evidence. Raw packet capture is deferred because the bounded trace/port evidence may suffice and avoids collecting HTTP credentials.

## Consequences
This is low-rate diagnostic traffic, not capacity measurement. Client idle duration differs from server idle duration due to network and response processing. A newly opened connection is normal recovery, not a failure. A synthetic or controlled idle-boundary error does not retroactively prove the cause of old errors. Only a positive correlated failure can support that hypothesis; absence of failures remains inconclusive.

## Failure and recovery
Retain seed and probe failures, no automatic retry. Bound lanes, repeats, delays and output; accept only explicit HTTP(S) origins without credentials and a UUID event. Validate the harness against a real local server with a shortened timeout and both connection reuse and reconnect paths. Stop cloud services and remove the temporary firewall rule after capture. No manifest is needed for the public availability read. No accepted booking, locking, idempotency, TTL, messaging or scaling ADR is superseded.

## Validation evidence
Local real-server boundary/trace tests: 3 passed in 1.95s; Ruff passed. Cloud: 1,440 measured reads, 22 transport failures (20 RemoteProtocolError, 2 ReadError), all on reused connections near requested idle 4.995s. Policies 5s/30s/2s produced 10/12/0 failures respectively. All 1,418 successful responses matched server request IDs. Failed probes have no response ID. The experiment exited 1 as designed, preserving failures. Sequential policy order and limited sample prevent a production-fix claim. Server configuration and application baseline were unchanged. Services stopped and temporary firewall removed. [Full evidence](../capacity/idle-boundary/README.md). Candidate from ADR 0022 remains unadopted.
