# Separate-generator validation

## Local 800-show diagnostic

[Raw results](local-800-shows.json): 800 shows x 300 seats, 8,000 viewer validators,
180 seconds at 50 total requests/second, 95% conditional availability reads and 5% holds.
All services and the generator shared the same machine (8 Docker CPUs, 7.74 GiB RAM).
Fixtures and initial cache warming were outside the measured phase.

- 8,550 reads: 6,937 HTTP 304 and 1,613 HTTP 200; 450 holds returned HTTP 201.
- Read p95 29.76 ms; hold p95 58.50 ms; zero generator drops or unexpected statuses.
- 88 observation samples: zero missing maps or maps without expiry; minimum TTL 4 seconds.
- Maximum reconciliation age 26.30 seconds; maximum overdue active holds 2, oldest 0.633 seconds.
- 149 holds expired during the measured phase; maximum dirty events 4.

The observer issued a pipelined TTL check for all 800 maps approximately every two
seconds plus SQL queries. This overhead is included. Sampled observations cannot exclude
brief failures between samples. This is a three-minute same-host diagnostic, not maximum
production capacity or proof for 200 locations. The 4-second TTL margin warrants a longer
soak before increasing active inventory. No post-run database integrity claim is made here.

## Portable HTTP generator

Install Python 3.12 and the HTTP-only dependency on the generator:

```sh
python -m pip install -r scripts/http_load_requirements.txt
```

On the application development machine, prepare temporary credentials using the existing
fixture result and the actual test API origin:

```sh
python scripts/export_load_manifest.py --results docs/capacity/distributed/local-800-shows.json --origin https://TEST-API --output tmp/load-manifest.json --seat-offset 100
```

Transfer the manifest privately. It contains one-hour bearer credentials and must never
be committed. It contains no signing secret or database credentials. Choose unused seats
for each run; regenerating credentials alone does not free previously held seats. Existing
fixtures may have closed sale windows by the time a later run starts; seed fresh fixtures
with scripts/seatmap_load.py when needed.

On the separate machine:

```sh
python scripts/http_load_generator.py --manifest load-manifest.json --origin https://TEST-API --rate 50 --seconds 180 --topology separate-host --output results.json
```

The generator checks readiness, warms validators outside timing, drops arrivals it cannot
schedule, and exits nonzero on read latency above 150 ms, drops or unexpected responses.
Topology is operator-declared and must be checked against actual deployment records.
Backend lock/pool/worker metrics, expiry recovery and durable state must be observed from
the application side during external runs; the HTTP generator does not verify them.
Increase rates in separate stages with fresh seat allocations and inspect errors, latency,
generator CPU/scheduling lag and backend metrics before proceeding.

External execution is pending a supplied generator host and reachable test API. The current
API binds localhost; this work has not exposed it publicly or purchased infrastructure.
The local smoke result, when present, is explicitly same-host and is only harness validation.

## Executed smoke validation

[HTTP-only local smoke](http-generator-smoke.json): 10 RPS for 10 seconds, 95 HTTP 304
reads and 5 HTTP 201 holds, zero drops/errors. Read p95 23.08 ms, read p99 296.15 ms;
the short run is not a tail-latency capacity qualification. Two earlier attempts stopped
at readiness HTTP 503 while local dependencies were down/restarting; neither entered
the measured phase. Ruff and whitespace checks passed. Application tests were not rerun.
