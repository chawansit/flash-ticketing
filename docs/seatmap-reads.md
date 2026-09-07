# Reading cinema seat maps efficiently

A scheduled show's inventory currently uses an `event_id`. This change does not add
location/screen/show relationships or physical screen coordinates. A typical deployment
of 200 locations with 4-10 screens has 800-2,000 screens and 240,000-600,000 physical
seats at 300 seats/screen; each scheduled show needs its own availability inventory.

## API contract

1. `GET /v1/events/{event_id}/layout`: static seat IDs and prices in integer minor units.
   Public HTTP caching for one hour, with an ETag for revalidation. No positions or
   row geometry are invented. Prices/layout remain immutable in the current MVP.
2. `GET /v1/events/{event_id}/availability`: seat ID, AVAILABLE/HELD/SOLD, and hold expiry.
   The response includes a map version and ETag. Cache-Control is `private, no-cache`,
   which permits storing the response but requires revalidation before reuse.
3. Send the returned ETag in `If-None-Match` on subsequent availability polls. A **304**
   has no body: retain the previous successful body. A **200** replaces it and its ETag.
   A **503** indicates warming/unavailability; it must not be treated as unchanged.

```http
GET /v1/events/{event_id}/availability
If-None-Match: W/"availability:{event_id}:{cache-incarnation}:{version}"
```

Validators are opaque: copy them exactly rather than constructing them. The API accepts
weak/strong forms, lists and wildcard If-None-Match. Cache incarnation prevents an old
validator matching a recreated map solely because the numeric version is equal.

Redis checks metadata and returns the representation in one atomic script. Matching
validators do not fetch seat fields. Nonmatching reads return compact availability;
the server still loads all seats for that show. Existing `/seats` and `/seat-deltas`
remain compatible. The old delta endpoint still scans the full map before filtering.

## Polling guidance for a future client

Fetch layout once, and begin availability polling at about two seconds with jitter.
Back off repeated 304s toward five or ten seconds, reset after a change, and pause
hidden/offline clients. Honor Retry-After on 503. There is no frontend or implemented
client scheduler in this repository. Faster polling increases request volume even with
304 responses. SSE, indexed deltas and CDN caching of live availability are future work.

A held seat's deadline allows the UI to show expiry without requiring the server to
change the HTTP representation every second. Availability is advisory; only a successful
PostgreSQL reservation establishes ownership. Never infer guaranteed availability from
an old map, a 304 or a locally expired timer.

## Rollout and limits

No migration is required. Full reconciliation populates the additive layout/validator
metadata in existing Redis hashes. Warm workers before enabling clients; older cached
maps lacking metadata return 503 on the new endpoints until refreshed. Legacy reads
continue to work. The 30-second cache TTL remains. ADR 0011 replaces the full sweep with bounded
active-window scheduling at a default 20-second target interval; the one-hour HTTP layout freshness is a separate, static-resource policy.

Full snapshots remain proportional to each event inventory, while schedule discovery
can still scan retained event metadata, and large single-show snapshots
have known timeout limits. This optimization targets many small cinema maps, but does
not prove throughput for all 800-2,000 screens or multiple daily showtimes. Location
count alone does not determine RPS; concurrent viewers and polling frequency do.

See [ADR 0010](adr/0010-conditional-seatmap-reads.md) and [measured comparison](capacity/seatmap/README.md).
