# Executed local validation

Command: `.venv/Scripts/python.exe -m pytest tests/unit/test_contention_timing.py -q -p no:cacheprovider`

Result: **2 passed in 7.11s**. Tests used real local HTTP/1.1 sockets, verified cold connect counts, warm reuse across four processes, global request accounting, a single fixture winner and no fake token leakage into artifacts. This is harness validation, not application/booking correctness validation.

Ruff passed for the new harness, matrix ownership verifier and tests. The full application suite was not rerun because application source was unchanged.
