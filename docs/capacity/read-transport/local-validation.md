# Executed local validation

- Real TCP reset, bounded/redacted trace, and existing contention harness tests: **4 passed in 5.53s**.
- Complete unit suite with generator diagnostics: **45 passed in 14.23s**, with two existing deprecation warnings.
- Ruff passed for both generator scripts and the new tests.

The reset test uses a real local socket with reset-on-close and verifies a failed HTTP phase is retained. It does not claim that the cloud error has the same cause. Diagnostics retain class/errno only, not exception messages or request data.
