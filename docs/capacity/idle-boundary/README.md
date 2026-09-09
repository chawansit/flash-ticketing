# Cloud read connection idle-boundary experiment

On 2026-09-09, a controlled read-only experiment reproduced transport failures near the five-second server keep-alive boundary. Early client expiry (2s) had no failures in this sample. This supports an idle connection race hypothesis; it does not establish the cause of the earlier load-test errors or validate a production fix.

| Client keep-alive expiry | Reads | Successful | Transport errors | Probe reuse / new TCP connections |
|---|---:|---:|---:|---:|
| 5s | 480 | 470 | 10 RemoteProtocolError | 81 / 149 |
| 30s | 480 | 468 | 10 RemoteProtocolError + 2 ReadError | 66 / 162 |
| 2s | 480 | 480 | 0 | 0 / 240 |

Each policy had 240 seed/probe pairs. All seeds succeeded. The 22 failed probes attempted no new TCP connection; successful reuse plus new connections plus failures accounts for all 240 probes per policy. All errors occurred at requested idle 4.995s, while waiting for response headers. Full traces and actual measured client idle durations are retained. Client idle is not the exact server idle interval, and failed responses do not expose a socket port or request ID. No packet capture was taken.

## Controls and interpretation

Same Huawei backend (4 vCPU / 8 GiB) and separate generator (8 vCPU / 16 GiB), private network. Application source remained the synchronous-service baseline; server keep-alive remained 5s. Sixteen independent HTTP/1.1 clients each used one connection, timeout 10s, no retries and no proxy environment. Each client performed three repetitions of delays 4.95, 4.995, 5, 5.005 and 5.05 seconds with lane-rotated ordering, alternating ordinary/conditional availability reads. Policies ran sequentially 5s, 30s, 2s from 16:43:37 to 16:47:24 UTC. Policy order was not randomized or repeated, so time/order effects remain possible.

The experiment issued 1,440 measured reads plus one readiness preflight. Server logs matched all 1,418 successful response IDs. Failed probes stopped before response headers, so request-ID correlation is unavailable for them. The harness correctly exited 1 because errors occurred; this is not a passing overall experiment.

A 2s client expiry is a mitigation candidate for idle reads, with increased reconnect cost: all 240 probes opened new connections. It has not been adopted in the load generator or application. A subsequent decision should test the candidate under the same sustained load and compare latency, CPU, connection churn and errors before adoption. Do not add automatic retries to holds or payment operations based on this read-only evidence. The earlier ReadError root cause remains unconfirmed, and ADR 0022's async-service candidate remains unadopted. No maximum production capacity claim follows from this low-rate test.

## Validation and cleanup

Real local HTTP server tests exercised successful connection reuse, reconnect after server expiry and early client expiry. Final targeted run: **3 tests passed in 1.95s**; Ruff passed. Application integration tests were not rerun because application code did not change. Executed probe and helper hashes were verified against archived files; the local probe matches the executed bytes.

All benchmark containers were stopped at 16:47:58 UTC and the temporary firewall rule removed. Both ECS VMs and synthetic data remain available. This experiment created no holds, orders, payments or credential manifests.

## Reproduction and evidence

```sh
python scripts/idle_boundary_probe.py --origin http://BACKEND:8000 --event EVENT_UUID --output idle-boundary
python docs/capacity/idle-boundary/summarize.py
```

Use a fresh output directory. The second command reproduces the report summary from retained cloud outputs. The server timeout flag is a declared measurement label, not a command to configure the server; verify actual server configuration separately.

- [ADR 0024](../../adr/0024-idle-boundary-experiment.md)
- [Derived summary by delay](summary.json), [raw traces](generator/idle-boundary), [run exit status](generator/idle-exit.log)
- [Generator configuration and hashes](generator/idle-config.json), [executed probe](generator/idle_boundary_probe.py), [executed trace helper](generator/http_load_generator.py)
- [Backend identity and actual timeout source](backend/idle-backend-config.json), [response correlation](backend/correlation.json), [matched server records](backend/matched-ingress.json), [cleanup](backend/idle-cleanup.log)
- [Previous transport investigation](../read-transport/README.md)

Follow-up: [400 RPS client-expiry ABBA comparison](../client-expiry/README.md) passed for both policies, with more TCP connections at 2s. The generator default remains 5s.
