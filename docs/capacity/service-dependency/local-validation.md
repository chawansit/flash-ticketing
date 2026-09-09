# Executed local validation

- Real-route service dependency regression and existing admission test: **2 passed in 1.25s**.
- Complete local unit suite after the change: **44 passed in 9.45s**, with two existing FastAPI/Starlette deprecation warnings.
- Ruff passed for the changed API, regression and comparison script.

The route regression resolves the real service dependency, verifies its value reaches the synchronous handler, verifies no dependency thread dispatch occurs for service, and verifies the handler still runs off the event-loop thread. Authentication is replaced only in this isolated dependency unit test; existing application authentication tests remain in the full suite. Cloud integration evidence is recorded separately.
