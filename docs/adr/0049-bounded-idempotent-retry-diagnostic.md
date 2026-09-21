# ADR 0049: Bounded idempotent retry and late-delivery diagnostic

Status: Accepted for explicitly labeled recovery diagnostics. ADR 0048 remains the authority for no-retry capacity certification and candidate service-SLO reporting.

## Context

The measured 800 RPS workload has produced rare generator scheduling misses while all dispatched requests, durability checks, overlap checks, queues, and rollback remained healthy. A scheduling miss occurs before an HTTP attempt and therefore cannot truthfully be called an HTTP retry. Production clients also need a bounded recovery contract for uncertain transport failures and transient overload responses. A retry must not create a second hold or conceal the first-attempt failure rate.

## Decision

Add an opt-in recovery diagnostic with two separately measured mechanisms:

1. A scheduled request that is more than 50 ms late may be delivered once if it is still within a configured late-delivery window and the generator has an available in-flight slot. This is recorded as late delivery, not retry. Requests outside the window or without a slot remain generator drops.
2. A dispatched HTTP request may make a bounded second attempt after exponential backoff with jitter. Holds reuse the exact request body, actor token, and idempotency key. Reads retry transient transport failures, 429, and selected 503 responses. Holds additionally retry `409 RESOURCE_BUSY`; business conflicts such as `SEAT_BUSY`, `SEAT_UNAVAILABLE`, expiry, validation, and idempotency mismatch are final.

Record first-attempt failures, retry reasons, attempts, recovered operations, exhausted operations, final responses, late deliveries, and unrecovered generator drops separately. Latency is end-to-end from the first attempt through final outcome. The recovery diagnostic passes only when every scheduled request is accounted for, final unexpected API and transport errors are zero, latency gates pass, acknowledged writes are exactly durable, double-booking is zero, queues drain, and rollback succeeds.

The recovery result is not a strict raw-capacity result and cannot replace the no-retry stage. The offered primary arrival rate remains the stated RPS; retries and late delivery are additional recovery traffic and must be reported.

## Alternatives considered

- Retry with a new idempotency key: rejected because an uncertain committed hold could be duplicated.
- Count a never-dispatched late arrival as an HTTP retry: rejected because it misstates what occurred.
- Retry every non-success response: rejected because business conflicts and invalid requests are final and retries amplify contention.
- Retry without recording first-attempt failures: rejected because it hides reliability and capacity signals.
- Use unbounded retries: rejected because they cause retry storms and can outlive the hold or client deadline.

## Consequences

The load tooling gains opt-in retry and late-delivery parameters; defaults remain no retry and no late delivery. Recovery tests create some traffic above the primary scheduled rate, so reports must include physical HTTP attempts in addition to logical scheduled requests. Reservation correctness remains guarded by PostgreSQL and idempotency records. Results apply only to the measured 95% read and 5% hold workload.

## Failure and recovery behavior

If the recovery attempt fails, the final error remains visible and the stage fails. If the generator exceeds its late-delivery window or in-flight bound, it records a drop and does not build an unbounded queue. Cleanup, post-TTL durability audit, admission restoration, and private-manifest deletion remain mandatory even when the load exits nonzero. A retry never extends a hold deadline.

## Validation evidence

Unit tests must prove that a hold retry reuses its idempotency key, transient failures are bounded, permanent business conflicts are not retried, first-attempt failures remain observable, and late delivery does not erase scheduling-lag evidence. Cloud validation must begin with a ten-minute safety stage and proceed to 30 minutes only after all safety gates pass.
