# ADR 0021: Opt-in worker dispatch diagnostics

Status: Accepted — diagnostic implementation and bounded cloud validation completed.

## Context
The 4,000-request analysis located slow admitted conflicts before the hold handler, but the existing dispatch timer combines authentication, dependency work and scheduling. Database and Redis timings cannot explain that interval. A new execution model must not be chosen from this aggregate alone.

## Decision
Extend only the optional benchmark entry point with probes around the pinned FastAPI thread-pool dispatch references. Preserve the original dispatcher, dependency functions, arguments, return values and exceptions. For hold requests, measure submission-to-worker-entry, worker execution and worker-exit-to-await-resumption separately; record limiter occupancy/waiters at submission. Classify only fixed stages authentication, service dependency, hold handler and other. Time hold idempotency separately as a nested database interval. Use separate structured records and fixed-label histograms, never add nested intervals to the existing disjoint hold phases. No tokens, arguments or bodies are logged.

## Alternatives
Changing synchronous dependencies to async or adding an early conflict path would change the workload before identifying the delay. Total handler latency alone cannot distinguish scheduling from authentication. Broad process profiling is a later option if bounded probes leave an unexplained interval.

## Consequences
The diagnostic adapter depends on pinned FastAPI dispatcher references and must fail on an unexpected layout. It remains opt-in and adds overhead; any future comparison must instrument both controls equally. Thread submission-to-entry includes limiter and OS scheduling, not solely limiter waiting. Occupancy is a snapshot. No booking, locking, TTL, messaging, idempotency semantics or scaling policy changes; no accepted ownership ADR is superseded.

## Failure and recovery
Preserve exception propagation and dispatcher cancellation policy. Restore original functions when disabling the probe. Missing phases remain absent. Keep ordinary Compose unchanged. On diagnostic incompatibility stop the experiment rather than silently omit timing.

## Validation evidence
Real dispatcher/protocol/admission tests: 7 passed in 5.76s; existing cloud suite: 84 passed in 12.25s. Two 1,000-contender cloud waves at admission eight each retained exactly one live durable owner and correct expiry. All 88 admitted requests have complete three-stage probe records. Authentication execution p95 was 0.131ms; submission/resumption delays were tens of milliseconds with at most 7/40 tokens borrowed and no waiting tasks at submission snapshots. See [report and raw evidence](../capacity/dispatch-probe/README.md). No optimization or sustained-capacity improvement is claimed. Services stopped and temporary credentials/firewall removed after collection.
