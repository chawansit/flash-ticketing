# ADR 0010: Separate layout and conditional availability reads

Status: Accepted; implemented and validated locally on 2026-09-07.

## Context
Each cinema show typically contains 300 seats. Most browsing polls repeat unchanged
availability. The current endpoint loads and serializes the full map on every request.
The model currently calls a scheduled inventory an event; it has seat IDs and prices,
not physical coordinates, location IDs or screen geometry.

## Decision
Add `/v1/events/{id}/layout` and `/v1/events/{id}/availability`. Layout exposes existing
seat IDs and prices, with a content-hash ETag and public revalidation after one hour.
Do not invent seat coordinates. Availability omits price and source-version internals,
uses private no-cache (always revalidate), and accepts If-None-Match. Redis checks the
ETag and reads the body within one Lua invocation, returning no seat fields for 304.
The availability validator includes event ID, representation, cache incarnation and
version, preventing reuse across cache loss. Support wildcard, weak and multiple tags.
Full snapshots populate layout and initialize incarnation metadata; patches retain it.
Keep existing seats/delta endpoints compatible. No frontend, SSE, change index or new
location/screen domain model in this step. Clients fetch layout once, availability next,
and use conditional polling with backoff. SQL reservation authority is unchanged.

## Alternatives
CDN caching of live availability adds staleness; SSE adds connection/fanout operations;
indexed deltas require another versioned index. First optimize unchanged reads, then
measure before adding those patterns. Existing full/delta endpoints remain available.

## Consequences
Unchanged reads still consume one request and one Redis script, but avoid loading seat
fields and serializing a map. Layout changes may take up to the HTTP freshness interval
to appear, appropriate only to this MVP's static inventory. Full reconciliation remains
O(inventory); no 200-location scale claim follows from local tests. This supplements
ADR 0009 without changing write fencing or lease recovery.

## Failure and recovery
Missing cache, missing metadata or interrupted-write marker returns warming, never 304.
Redis outage returns 503. Full reconciliation restores metadata. A new incarnation
forces a fresh body after cache recreation. No database fallback on browse requests.
ETags are checked atomically with the returned representation, avoiding version/body
races. Roll out worker warming before new clients; legacy clients continue unchanged.

## Validation evidence
56 container tests passed (14.88 seconds, zero skipped). New real Redis and HTTP
coverage verifies conditional headers/body, layout stability, hold/release changes,
atomic body validators, cache recreation, interrupted writes and outage responses.
Six read-heavy stages used 100 shows of 300 seats and 1,000 viewer validators. At 50
total RPS (95% reads), p95 fell from 79.0 to 37.2 ms and read body bytes fell about 88%,
with all reads and holds succeeding and no drops. Higher stages fail the local read
gate; retain their generator drops and transport failure. See the
[report](../capacity/seatmap/README.md). Production capacity remains unverified.
