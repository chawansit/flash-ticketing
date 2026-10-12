# ADR0295: Bind the hourly generator to the existing CCE ingress

## Status

Accepted under standing Continue authorization. Supersedes ADR0293's inherited ECS-only hourly coordinator origin for the explicitly bound CCE hourly route. Retains ADR0294 observer ordering, every workload and quality gate, and the sealed ADR0256 coordinator/leaf bytes.

## Context

ADR0294 run adr0151-404948d8e6c9 passed observer startup and native safety, then the hourly coordinator rejected the CCE ingress address before spawning customer shards. Its retained generator log reports the exact bounded coordinator setting error; the original audit finds zero paid-cohort orders, payments, bookings and tickets. The reserved launch remains consumed. Runtime restoration and owned cleanup succeeded, but the scheduled-count financial gate failed, requiring independent closure. No hourly capacity was measured.

## Decision

Use a small, source-bound ingress-only adapter around the unchanged sealed hourly coordinator. Require the exact existing CCE ingress origin and a matching manifest. Validate a copied structural view against the original ECS-only coordinator checks, then use the original arguments and manifest for actual shard dispatch. Do not change rate, concurrency, allocation, retry, completion deadlines, polling, leaf execution or aggregation.

Select and transfer this adapter only for the registered ADR0293 hourly ingress route; default ECS hourly and short paths remain unchanged. Bind its bytes in the route contract and paid-stage source identity. Validate the same generated launch arguments and complete manifest locally before consuming a paid launch. Reject any unknown or mismatched origin. Retain bounded generator exit/log evidence when a customer report is missing.

Independently close this exact failed, stopped scope after a read-only zero-cohort relationship audit, both safety-payment durability audits, global duplicate/queue checks, runtime stability, absent namespace/helpers, idle generator, retired shows and removed private inputs. Preserve the original failed report and consumed allowance. Closure means FAILED_RESTORED, never a passing benchmark or authorization to replay the scope.

## Alternatives

- Relax the sealed coordinator to arbitrary origins: rejected.
- Change the frozen hourly recovery manifest or tests to reinterpret its old result: rejected.
- Route the hourly candidate through ECS: would test a different entry point.
- Rebuild the load generator: unnecessary.
- Infer payment integrity solely from a missing customer report: rejected; independently audit the owned cohort.

## Consequences

The adapter translates one structural origin assertion; network requests retain the CCE address. It adds no pods, connections, infrastructure spending allowance or application changes. Local launch validation prevents this failure before a paid reservation is consumed. Empty-cohort recovery does not demonstrate customer performance.

## Failure and recovery behavior

Block unknown origins, mismatched manifests, stale starts or changed sealed coordinator identities before dispatch. A launched scope remains consumed even if the coordinator exits before journeys. Preserve failed evidence and perform all financial, queue and restoration checks. Do not increase load on failure.

## Validation evidence

Executed evidence before implementation: retained coordinator exit code 1 and origin-validation error; paid-cohort counts zero; runtime restored and owned resources removed. Adapter regression tests, independent recovery and fresh hourly qualification are pending.

Independent read-only recovery executed successfully in 69.75 seconds against the exact failed scope. It verified zero paid-cohort orders/payments/bookings/tickets, all relationship predicates, both safety payments and duplicate callbacks, zero global double-booking, drained queues, stable restored runtime, absent namespace/helpers, idle generator, removed private inputs and 1,010 retired owned fixture events. The original result remains failed and its paid launch consumed. See [abort](../capacity/cce/existing-ingress-hourly-generator-abort-2026-10-13.json) and [independent recovery](../capacity/cce/existing-ingress-hourly-generator-abort-recovery-2026-10-13.json).

Fourteen ingress adapter and composed launch-preflight tests passed in 0.53 seconds, including unchanged sealed adapter identity, legacy ECS behavior, exact CCE target, manifest mismatch, unknown origins, altered workload and rejection before paid allowance access. The broader regression run and fresh cloud hourly qualification are pending.

The final composed regression run passed all 174 tests in 139.25 seconds. Ruff and whitespace checks passed; the complete fresh hourly registration/plan was executed locally without writes and passed. The paid hourly qualification remains pending.


The fresh hourly run adr0151-d15d25621ac5 executed on 13 October 2026, 01:03:17–02:03:17 Bangkok, using the same four APIs, fourteen workers, 24 database connections and 500 active generator journeys. The generator preflight passed; 302,400 journeys were scheduled, 300,235 dispatched and fulfilled, and 2,165 were undispatched because active-journey capacity was full. There were 380 initial-error journeys, all recovered, with zero final customer failures. The original post-TTL scheduled-total gate failed because undispatched journeys produced no orders. Independent recovery verified the actual 300,235 unique paid tickets, all relationships, both safety payments, zero double-booking, drained queues, restored runtime and removed private inputs. The result remains FAILED_RESTORED.

Timestamp audit found 300,093 unique paid-and-issued tickets inside the offered hour, but only 299,899 inside the required two-second guard; the hourly target gate failed. Observer diagnostics also failed: statistics ended about 17 seconds before the offered hour ended, one gap was 2.302 seconds, and slot-failure and native CPU coverage was incomplete. Available pooler evidence showed 24 active server connections, 16 waiting clients and maximum wait 384.954 ms; worst-shard journey p95 was 11,424.35 ms, dominated by ticket wait. These measurements support further investigation of fulfillment/database pressure but do not establish complete failure-time causality.

Total experiment and independent recovery time was 4,922 seconds, within the 5,400-second ceiling. See [hourly result](../capacity/cce/existing-ingress-hourly-result-2026-10-13.json) and [actual-cohort recovery](../capacity/cce/existing-ingress-hourly-actual-cohort-recovery-2026-10-13.json). No hourly production qualification, maximum capacity, or application performance gain is claimed; no additional paid stage was started.
