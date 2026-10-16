# Encore customer and engine demo

Two views share one journey: **Customer journey** (seats, reservation, payment,
processing and ticket) and **Engine dashboard** (milestones, recovery trace and
browser-session counters).

Private sample preview: <https://encore-ticketing-engine-demo.robust-tang-9828.chatgpt.site>

## Run locally

From the repository root, with Python 3.9 or newer:

```powershell
python web/demo/serve.py --port 4080
```

Open `http://127.0.0.1:4080`. No frontend build or npm install is required.
Choose a presentation scenario on the Engine dashboard before booking:

- Standard booking.
- Lost payment response: retrieve the original payment before any replay.
- Seat taken before reservation: display the conflict and refresh availability.

Sample seats, payments and tickets live in browser session storage. They are
illustrations, not PostgreSQL durability evidence, real charges or admission
credentials. Reset sample session clears only that browser's sample data.

## Connect an existing development engine

Start a separate bridge with a fixed, operator-selected API origin:

```powershell
$env:DEMO_ENGINE_URL='http://127.0.0.1:8000'
python web/demo/serve.py --port 4080
```

Select **Connect engine**, enter a short-lived customer bearer token and,
optionally, the UUID of an isolated event with an open sale window. Supply only
an origin, with no credentials, path or query, in `DEMO_ENGINE_URL`. Use a
development engine where the payment simulator is enabled. This UI does not
provision fixtures or enable simulator flags.

The bridge forwards existing hold, command, order, payment-operation and browse
contracts. Payment stays disabled until the hold is durable. Ambiguous payment
responses trigger inquiry before any replay; the key, body, customer and original
expiry remain unchanged. Retries are bounded. Pending identities survive a page
refresh; reconnect with the same customer to retrieve them. Bearer credentials
remain in memory and must be entered again after refresh. An unresolved operation
blocks changing modes or event; live failures never fall back silently to samples.

The Python bridge binds to loopback only. It checks Host, Origin and supported
routes, rejects redirects and arbitrary proxy destinations, and filters error
bodies and metrics. It is a local developer tool, not a production gateway or
management console. Do not expose its port through a public tunnel. The static
hosted preview runs samples only; it has no private Huawei connection.

## Dashboard scope

The counters describe activity observed by this browser session. In live mode,
the optional metrics snapshot describes one upstream API process, which may vary
between replicas. It does not establish cluster throughput. Kafka lag, RDS CPU,
database waits and global queue drain are explicitly unconnected. The historical
138.49 tickets/s badge links to the earlier five-minute backend evidence; it is
not a measurement of this frontend or an hourly qualification.

## Verification

```powershell
python -m pytest web/demo/tests/test_bridge.py -q
node --test web/demo/tests/recovery.test.mjs
```

With the local preview running and Playwright plus a browser available:

```powershell
# Optional paths when these are not installed in their default locations:
$env:ENCORE_PLAYWRIGHT_MODULE='<absolute path to playwright package>'
$env:ENCORE_BROWSER_EXECUTABLE='<absolute path to browser executable>'
$env:ENCORE_BASE_URL='http://127.0.0.1:4080'
node web/demo/tests/browser-check.mjs
node web/demo/tests/live-browser-check.mjs
```

The first browser suite verifies normal booking, lost-response recovery, shared
trace, mobile layout and unconfigured live-mode rejection. The second uses local
HTTP contract doubles to verify live-adapter recovery, original-key replay after
a lost hold response and reload, failed durability, expiry, customer-token
handling and no sample fallback. Neither proves real backend/payment durability.
Receipts and screenshots are written under ignored `tmp/`.

Executed checks and exact asset hashes:
[`demo-validation-2026-10-17.json`](../../docs/capacity/frontend/demo-validation-2026-10-17.json).
No full real-engine journey or cloud load was run for this frontend change.

## Source and private publication

Canonical source is `web/demo/dist/`; the bridge and tests remain outside the
hosted static assets. Site ID: `appgprj_6ad10fac4c7081919c5356f107103e47`.
Verified private version 1 source commit:
`e103d87ce8993f66f895565b9003cb04282d643f` (the separate Sites source repository).
The four canonical assets match that commit byte for byte.

Use the Sites source helper to open that existing Site before an update, preserve
its identity and audience, and package the checked source. Do not recreate the
Site or publish from a stale checkout. Source credentials, browser tokens,
private fixture manifests and preview Git metadata must never be committed here.

Architecture decision: [ADR0331](../../docs/adr/0331-add-customer-and-engine-demonstration-ui.md).
