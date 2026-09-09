# Local validation

Executed before cloud load on the updated shared generator:

- `python -m pytest tests/unit/test_client_expiry.py tests/unit/test_transport_trace.py tests/unit/test_idle_boundary.py -q -p no:cacheprovider`: 9 passed in 6.04s.
- `python -m pytest tests/unit -q -p no:cacheprovider`: 52 passed in 18.23s; two dependency deprecation warnings (Starlette HTTPX TestClient and AnyIO BlockingPortal alias).
- Ruff passed for the changed harness, test, CPU observer and report scripts.

The real HTTP server test verifies bootstrap connection reuse, early-expiry reconnects, exact measured connection completion accounting, and preserved request gates. Invalid nonpositive, nonfinite and over-limit expiry values are rejected. Existing trace tests exercise a real TCP reset and bounded redaction.

Application code and dependencies are unchanged. No new application integration-suite execution is claimed.
