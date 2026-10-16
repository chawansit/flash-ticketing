# Encore customer and engine demo

Two views share one booking journey: **Customer journey** shows selection, provisional and durable holds, simulated payment, processing and ticket retrieval; **Engine dashboard** shows observed browser activity and customer milestones.

## Sample preview

[Open the private preview](https://encore-ticketing-engine-demo.robust-tang-9828.chatgpt.site).

The private hosted preview uses clearly labeled browser sample data. It does not contact Redis, Kafka, PostgreSQL or a bank. Sample tickets and their decorative barcodes are not admission credentials. See `preview.json` for the persisted Site identity and selected checkout. Do not create another Site when editing this preview.

In Engine dashboard, select Standard booking, Lost payment response or Seat taken before reservation. Switch to Customer journey, select seats and complete a booking. Lost-response mode creates one sample payment, drops its response and retrieves that existing operation instead of submitting another charge. Reset sample session affects only this browser's illustrative data.

## Local preview

From the repository root, with Python 3.12 or newer:

```sh
python web/demo/serve.py --port 4080
```

Open http://127.0.0.1:4080. No frontend build, package installation or application-image rebuild is required. The browser imports modules from `dist`. Google Fonts are optional presentation assets; system fonts provide fallback.

## Live engine mode

Start the development bridge with a fixed upstream engine origin, for example in PowerShell:

```powershell
$env:DEMO_ENGINE_URL = 'http://127.0.0.1:8000'
python web/demo/serve.py --port 4080
```

Use the existing engine's actual origin instead when appropriate. Keep the bridge bound to loopback. In Connect engine, enter a short-lived bearer token for an isolated demo customer and optionally an open fixture event UUID. Token issuance and live-fixture preparation remain operator responsibilities. Do not use a signing secret or cloud credential as the bearer token.

The browser calls only the same-origin `/engine` bridge. The bridge permits an explicit route/method list, rejects foreign Origin/Host values, does not follow redirects, and forwards the original customer authorization and idempotency key. Customer tokens remain in page memory and are never stored or logged. Only non-secret pending operation identities are retained in session storage; after refresh reconnect with the same customer to retrieve the existing order or replay the original ambiguous hold. A failed live connection never becomes a sample transaction.

The event list is limited by the existing API's first 100 results. Supply the exact fixture UUID when necessary. Geometry is illustrative because the engine models seat IDs and prices, not venue coordinates. Live payment initiation uses the engine's development simulator endpoint and fails when that endpoint is disabled. No card details or signed callbacks are manufactured by this frontend.

Live engine metrics are an allowlisted, read-only snapshot from one upstream API process. They are not cluster-wide counters, a rate calculation or a financial audit. Kafka lag, RDS CPU and global queue drain stay marked unavailable. The 138.49 tickets/s reference is historical five-minute evidence, not a measurement from this page.

## Verification

```sh
python -m pytest -q -p no:cacheprovider web/demo/tests/test_bridge.py
node --test web/demo/tests/recovery.test.mjs
```

Browser checks require Playwright and an installed compatible browser. Start the local preview, then run:

```sh
node web/demo/tests/browser-check.mjs
node web/demo/tests/live-contract-browser.mjs
```

Optional environment variables: `ENCORE_BASE_URL` selects a loopback preview; `ENCORE_PLAYWRIGHT_MODULE` selects an installed Playwright module; `ENCORE_BROWSER_EXECUTABLE` selects an installed browser. Checks write screenshots and compact results under ignored `tmp`. The live-contract browser test intercepts API responses and checks protocol behavior; it does not qualify real database or gateway correctness.

## Scope and deployment

[ADR0331](../../docs/adr/0331-add-customer-and-engine-demonstration-ui.md) records the boundary. The existing backend and measured images are unchanged. This bridge is a loopback development tool, not a public production proxy or operator authorization system. Private Sites serves the static sample; connecting a hosted frontend to private cloud services requires an approved authenticated server bridge, TLS and operational access design. No cloud traffic generator is launched here.

For private preview updates, copy reviewed `dist` files to the existing isolated Site checkout, use the Sites source workflow with a short-lived credential through stdin, package the pushed commit, then save and deploy the same archive to the same private Site. Never persist source credentials or include private engine configuration in the static bundle.
