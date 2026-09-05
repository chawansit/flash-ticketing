# Acceptance and measurement

Correctness gates:

- Same-seat concurrent attempts produce one active owner, including tests without Redis shielding.
- Multi-seat failures leave no partial state.
- Duplicate callbacks and duplicate OrderPaid events create one booking/ticket per seat.
- Expired holds can be reclaimed before cleanup; old cleanup/payment cannot clear a new owner.
- Late captured payment produces exactly one refund request.
- Idempotent checkout resolves to one order; changed payloads conflict.
- Outbox and inbox recovery preserves effects across duplicate deliveries.

Reference-deck performance targets (to measure, not assume): seat-map p95 <100–150 ms,
reservation p95 <200–300 ms, rejected-seat response <100–200 ms, transaction time <100–200 ms,
zero oversell and shared-memory errors. At 5–10x attempted traffic, connections and lock waits
must remain bounded rather than scale proportionally. The app intentionally uses a stricter
75 ms lock timeout than the deck's 300–500 ms upper bound.

The load script reports status distribution, total p95 and conflict p95 for hot-seat traffic.
Repeat at 100/500/1000 attempts with declared concurrency, using a fresh seat each time. Capture
CPU/RAM, API replica count, connection limits, network placement and Prometheus data with the
results. Admission failures are not successful throughput. Multi-process deployment can use
additional API containers behind a local proxy; the DB tests use concurrent independent connections.

Local test results and any environment limitations are recorded in `docs/validation.md`.

