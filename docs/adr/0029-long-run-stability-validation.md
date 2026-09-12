# ADR 0029: Long-run stability and slow-commit isolation workflow

Status: Accepted

## Context

The backend had repeated production-style soak attempts where transport resets and occasional
admission pressure appeared without clear attribution. Existing diagnostics already tracked
basic hold timings and some pool metrics but did not make the failure-cause loop explicit for
connection lifetime, long-run commit spikes, or worker scaling comparisons.

The immediate request is to:

- isolate read-connection reset causes and compare keep-alive settings without adding request retries,
- separate and attribute commit and pool-return latency spikes while observing WAL/checkpoint/disk statistics,
- run a deterministic 400 then 500 RPS long-run sequence with post-run admission/error/queue validation,
- compare API worker counts under the same DB connection pool limit.

## Decision

1. **Keep connection reset diagnostics in first-party server middleware**

   In request middleware, collect `connection_key` reuse, `connection_age_ms`, and reasoned close-path
   telemetry for every completed request:

   - `ticketing_http_connection_age_seconds`
   - `ticketing_http_connection_close_total`

   Keep the middleware behavior unchanged for business logic (no automatic retry paths). Count only close
   paths the ASGI layer can actually observe (`client_disconnect`, `server_send_error`); ordinary response
   completion is not a socket-close event. Bound the approximate connection-age state by idle age and entry
   count so diagnostic state cannot grow without limit during a soak.

2. **Correlate app-level DB timing with PostgreSQL storage pressure in one observer loop**

   Extend observer collection to include selected `/metrics` series (`ticketing_db_*` commit/pool counters and
   connection-close counters) together with `pg_stat_wal`, `pg_stat_bgwriter`, `pg_stat_io` samples and Redis
   seat-map health.

   For each long run, compute deltas of these counters across the measured window and pair them with
   lock-wait and checkpoint/WAL deltas to distinguish:
   
   - commit inflation caused by DB/transaction work,
   - return-to-pool inflation caused by app scheduling/contention,
   - checkpoint/WAL stalls that are likely I/O-bound.

3. **Formalize the 400->500 soak gate before any production sizing increase**

   Add an explicit orchestrated routine that runs 400 RPS then 500 RPS (sustained windows), with:

   - no client-side retries in the generator,
   - transport diagnostics enabled,
   - mandatory post-run durability verification (`verify_cloud_holds.py`) after each stage,
   - optional fallback runs for the previous passing rate only.

4. **Compare API workers with fixed DB pool bounds in a controlled pattern**

   Compare API worker counts (`--workers` per API process) by reconfiguring API with compose overrides and
   a fixed `DB_POOL_MAX` for each run. This allows us to assess whether CPU/thread scaling or DB pool
   saturation is the binding constraint before considering service separation.

## Alternatives considered

- **Measure commit and connection resets only from load-generator output.**
  Rejected because generator transport errors do not attribute server-side connection lifecycle or close
  reasons, which were specifically requested for reset root-cause isolation.

- **Use only PostgreSQL internal metrics for commit attribution.**
  Rejected because slow commit symptoms were requested to be separated into app commit and return timing,
  and application-side timers already existed for that split.

- **Jump directly to worker-scaling experiments.**
  Rejected because scaling decisions must be made after a stable 400->500 soak gate with durable verification.

## Consequences

- Additional metric cardinality and file artifacts are introduced during stability windows.
- Keep-alive comparison and worker-pool comparisons are now explicit, reproducible experiments
  and not ad-hoc load commands.
- The suite creates more evidence (preflight, observer, verify, fallback artifacts) and should be used as
  the canonical production-sizing input.

## Failure/recovery behavior

If any 400/500 stage fails throughput/error gates:

- stop escalation for that worker/pool configuration,
- preserve artifacts and worker logs,
- optionally run one fallback stage at the previous passing rate,
- return API service configuration to compose defaults after each API worker block.

No rollback of application code is performed by this workflow; only operational reconfiguration of API
worker count and pool limit is automated.

## Validation evidence

Planned evidence for each run:

- per-stage generator `summary.json` with transport error taxonomy,
- observer samples with PostgreSQL and API metric deltas,
- durability/queue verification output from `verify_cloud_holds.py`,
- stage report JSON containing worker/pool/keep-alive decisions and gate outcomes.

Local implementation validation on 2026-09-12: Ruff passed across `src`, `tests`, and `scripts`;
68 unit tests passed, 57 PostgreSQL/Redis integration tests passed, and the real HTTP 100-contender
test passed with exactly one durable winner. Cloud soak evidence remains the next target and will be
stored with the capacity report; these local checks do not establish production capacity.
