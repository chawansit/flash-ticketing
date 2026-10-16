# ADR0331: Add customer and engine demonstration views

## Status
Accepted; implemented, browser-checked and privately published in sample mode. Real-engine integration qualification is pending.

## Context
The user approved a customer demonstration with a booking view and an engine dashboard. The reference has browser APIs but no customer frontend. Backend images and financial/load evidence must retain their existing identities. Hosted static Sites cannot directly reach private Huawei services.

## Decision
Add a buildless HTML CSS and JavaScript frontend under web/demo with two views, explicit browser-sample and live-engine adapters, and a loopback-only Python bridge for same-origin development integration. The bridge forwards only documented booking/read endpoints to a fixed operator-configured upstream. Customer bearer tokens stay in page memory and are not logged or stored. Metrics are read-only, loopback-operator data and never inferred from sample activity. Sample journeys, tickets and engine indicators are prominently labeled simulated; they make no engine-capacity claim.

Keep the backend application unchanged. Use existing hold/command/order/payment-operation contracts, stable per-operation idempotency keys, inquiry before ambiguous payment replay, original expiry, bounded jittered polling and pause passive reads when the page is hidden. Payment simulation is development-only. Do not send webhooks directly or hold database connections from this UI. Browser sample state is temporary presentation data; real persistence, locking and payment authority remain in PostgreSQL and Redis through the API. No new broker, persistence system, TTL extension, replica/connection-budget change or cloud load test is introduced.

Publish a private Sites sample preview from an isolated ignored checkout, retaining the canonical frontend in the project Git repository. Live mode uses the local bridge and a configured existing engine; it is not available on static hosting without an approved authenticated server bridge. Public or production hosting, real-bank integration and an operator authorization system require separate decisions.

## Alternatives
Mount the frontend in the tested backend image, introduce a full frontend framework, or provide only an unconnected mockup. Separate static assets and a small development bridge avoid image drift and new runtime dependencies while preserving an executable real-API path.

## Consequences
Customer selection, reservation, simulated checkout, processing/recovery and ticket display can be demonstrated alongside a technical flow view. Read-only live metrics are limited to available API process counters, not cluster-wide Kafka lag or RDS utilization. No fabricated live measurements. One shared browser-sample store lets both views show the same illustrative session. The bridge is a developer tool bound to loopback, not a production gateway.

## Failure and recovery behavior
Fail closed when live configuration, authentication or required endpoints are unavailable. Never silently switch a failed live transaction to sample mode. Disable payment until a reservation is durable. Inquire before replaying an ambiguous initiation and reuse the original key/payload. Preserve pending operation identity across view changes; block retries after expiry except inquiry of existing payments. Stop polling on terminal state or deadline and retain a processing/retrieval path. Do not expose upstream bodies containing operational secrets, accept arbitrary proxy destinations or forward redirects. Browser refresh recovery must not persist bearer credentials. Treat cache availability as advisory and display conflicts explicitly.

## Validation evidence
Executed 11 bridge tests against a real local HTTP stub (10.05 seconds), nine JavaScript recovery tests, sample browser journeys and live-adapter browser contract scenarios. Checks covered multi-seat completion, lost payment response, original-key inquiry/replay, refresh retrieval, ambiguous hold identity across refresh, failed durability blocking payment, tokens excluded from session storage, same-origin/route guards and no silent sample fallback. Desktop and mobile screenshots were inspected; responsive checks found no horizontal overflow. Python F/I checks passed.

The private static sample was deployed successfully from Site source commit `e103d87ce8993f66f895565b9003cb04282d643f`. Canonical static asset bytes match that source. [Frontend evidence](../capacity/demo/two-view-frontend-2026-10-16.json) records the deployment and exact verification scope. No actual engine/database/broker or bank transaction was executed; Docker Desktop was stopped, so financial integration is not qualified in this round. Backend images and capacity evidence are unchanged.
