# Local validation executed

- Real httptools socket tests plus existing admission/hold diagnostics: **10 passed in 8.49s**. Existing FastAPI/Starlette deprecation warnings were reported.
- Updated contention timing harness real-socket tests: **2 passed in 4.77s**.
- Ruff passed for the adapter, protocol tests, generator and metrics exporter.

The opt-in adapter preserves app Server-Timing, stamps fragmented headers only once complete, uses a fresh stamp on keep-alive, and records queued pipelined requests before the next app entry.

After adding the missing-stamp regression, the protocol module passed **3 tests in 0.78s**. Missing timestamps are omitted rather than fabricated as zero.
