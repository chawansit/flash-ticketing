# ADR0240: Bounded compressed diagnostic traces

## Status
Accepted for isolated local implementation and byte-preservation validation. Cloud runner integration and sampling-coverage correction are pending.

## Context
The historical hourly pipeline trace is 264435078 bytes, exceeding the 128 MiB transport ceiling. Its original raw SHA256 is 644e54f07607053beb0591be7bc635693a6ba96d9e92690113c11f57f4102a06. Retained evidence also has 77 sample gaps over 2 s; compression cannot close those gaps or establish payment slot ownership.

## Decision
Compress a stopped, ownership-verified diagnostic trace for bounded transfer, preserving every original byte. Use deterministic gzip, a 512 MiB raw ceiling, 128 MiB compressed ceiling, 1 MiB buffers and a 25 s local transform deadline. Verify source size/hash and unchanged file identity; create destination files exclusively with POSIX mode 0600 (Windows retains workspace ACLs) and reject symlinks or nonregular inputs. Decompression enforces both ceilings, exact length, checksum and gzip integrity, including truncated and trailing input rejection. Failures remove only the exact newly created output file. No filtered or sampled subset may replace the complete trace.

The application correction remains solely ADR0239. Qualify diagnostic transport identically for control/candidate before registering fresh cloud scopes; do not replay old consumed scopes. Existing historical lock and images remain unchanged until a new extension is explicitly registered and bound. Financial audits, zero double-booking, post-TTL verification, queue drain and restoration remain mandatory despite trace errors.

## Alternatives
Raise transport ceiling without compression: transfers hundreds of MiB and retains expensive in-memory transport. Drop fields/samples or keep only successful windows: loses verification evidence. Disable observation gates: rejected.

## Consequences
Compression occurs only after owned observers stop, so it adds no database connections or application work during offered traffic. The compression ratio is not guaranteed; incompressible/oversized evidence fails closed. Sampling gaps and incomplete log capture require separate correction. This helper alone does not activate a cloud runner capability.

## Failure and recovery behavior
Reject stale destinations, altered input, bad metadata, expansion above declared bounds, checksum failures and incomplete gzip streams. Preserve the original trace and failed experiment. Trace failure never skips financial reconciliation, full drain or owned cleanup. A fresh scope is required for any new test.

## Validation evidence
Executed: 24 transport tests passed; one Windows symlink case skipped. Coverage includes byte-identical deterministic round-trip, stale destination preservation, CRC/hash/length corruption, truncation, trailing data and additional-member rejection, expansion/size/deadline bounds, source mutation and exact created-output cleanup. The full 264,435,078-byte historical trace compressed to 26,625,958 bytes in 12.172 s and decoded in 1.985 s with the original SHA256 unchanged. Windows path and open-handle timestamps differ in ctime semantics; file identity and mtime are checked across both, and path ctime is checked before/after. The complete unit suite passed 1,827 tests with two Windows skips. The helper is not yet integrated into cloud collection and does not fix the historical 77 sampling gaps or missing failure snapshots. No cloud transport, paid load, capacity improvement or hourly qualification has been performed.
