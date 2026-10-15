# Incident 001: a Postgres restart killed every Reliant process

**Date:** 2026-10-15 · **Type:** induced failure (spec Phase 4) · **Environment:** local, docker-compose infra + Node processes · **Status:** resolved, regression tests added

## Summary

We killed Postgres (`docker kill`, the equivalent of `kill -9`) for 30 seconds while ingesting 50 events/s, to check the durability promise in ADR-001: *an event that got a 202 is never lost*.

The promise held: no accepted event was lost from storage. But **all four processes (ingest, consumer, worker, retry-publisher) crashed within the same second Postgres died**, and none came back when Postgres did. Ingest refused every request for the rest of the run, and one accepted event sat undelivered in Kafka with no consumer left to deliver it.

In Kubernetes, every pod would have restarted at once, and every consumer would have rejoined the group (a rebalance) on top of the database outage. The cause was one missing line, `pool.on('error', …)`. Fixing it exposed a second crash in the metrics endpoint.

## Impact (first run, before the fix)

| | |
|---|---|
| Processes crashed | 4 of 4, all at t+18.9s (Postgres killed at t+18.9s) |
| Requests after the crash | 1,920 of 1,920 refused (connection refused: ingest was dead), including the 30s *after* Postgres was back |
| Accepted events (202) | 599 |
| Delivered | 598 |
| Lost from storage | **0**. The undelivered event was in Postgres and Kafka (partition 5, offset 179 uncommitted) and would have been delivered by any restarted consumer |

## Timeline (first run, offsets from the start of the drill)

- **t+0s:** ingest, consumer, worker and retry-publisher running; 10 tenants, one healthy endpoint each; 50 events/s open-loop load.
- **t+18.9s:** Postgres killed. Every process logs `Error: Connection terminated unexpectedly … Unhandled 'error' event` and exits with code 1.
- **t+18.9s → end:** every ingest request fails at the TCP level. Nothing alerts until `ReliantProcessDown` would fire, 2 minutes later.
- **t+49.5s:** Postgres is back. Nothing recovers, because nothing is running.
- **t+79.6s:** load stops; 598 of 599 accepted events were delivered. The last one was still waiting in Kafka.

## Root cause

`lib/infra.js` created the shared `pg.Pool` without an `'error'` listener. When Postgres goes away, each *idle* connection in the pool emits `'error'` through the pool. Node treats an `'error'` event nobody listens for as fatal, so every process using the pool exits. That means all of them, because they all share the pool.

In-flight queries weren't the problem: they reject normally, and every loop already handled that (the worker sweep, the retry-publisher tick, kafkajs retrying `eachBatch`, and ingest's 500). The crash came from connections that weren't doing anything.

### The second crash, found by re-running the drill

With the pool fixed, the worker still died, 17s into the outage this time: `ERR_HTTP_HEADERS_SENT` in `lib/metrics.js`. The `/metrics` handler wrote the `200` header *before* collecting. The worker's gauges query Postgres, so collecting failed, and the `catch` then tried to write a `500` header onto a response that already had one. That throw escaped the async handler as an unhandled rejection. **A routine Prometheus scrape during the outage is what killed the worker.**

## Fixes

| Fix | Where |
|---|---|
| Listen for pool `'error'` and log it as a warning. The pool already discards the dead client and opens a new one on the next query. | `lib/infra.js` |
| `connectionTimeoutMillis: 5000`. Without it, a connect to an unreachable host (as opposed to a refused one, e.g. during a managed-database failover) waits forever. | `lib/infra.js` |
| Collect the metrics first, then write the header. | `lib/metrics.js` |
| Log `err.code` when `err.message` is empty. A refused connection is an `AggregateError` with no message, so the outage was logging `"err":""`. | `lib/log.js` |
| New alert `ReliantIngestErrors`: more than 5% of ingest requests returning 5xx for 1 minute. Before this, 30 seconds of every ingest request failing would have paged no one. | `observability/alerts.yml` |
| Regression tests. One kills the pool's idle connection with `pg_terminate_backend`; the other makes a gauge throw. Each fails with its fix reverted (checked). | `tests/resilience.test.js` |
| The drill itself, kept as a re-runnable scenario | `node loadtest/run.js pg-outage` |

## Verification (same drill, after both fixes)

```
Postgres outage: 50 events/s, kill -9 at t+27.4s, restart after 30s
responses by status: { '202': 1721, '500': 980 }
non-202 window: t+27.5s .. t+58.5s        (Postgres down t+27.4s .. t+57.9s)
accepted: 1721, delivered at receiver: 1721, LOST: 0
duplicate deliveries (same webhook-id twice): 5
process exits: none
```

- **Ingest recovered by itself** within about 0.6s of Postgres starting. Errors covered exactly the outage window and nothing more.
- **Delivery resumed within about 5s**: the receiver went from 577 to 927 events in the first 5-second window after recovery, as the consumer worked through what had queued up in Kafka.
- **5 duplicate deliveries are expected**, not a bug. The consumer had delivered those events inline, but the ledger write failed because Postgres was down, so it never committed the Kafka offset (ADR-005: commit only after the ledger write). kafkajs re-ran the batch and they were delivered again. Receivers dedupe on `webhook-id` (README, receiver contract). The alternative is committing before the ledger write, which risks *losing* events, and that's worse.

## What went well

- The durability design held under a real crash: 0 accepted events lost across three runs.
- Ingest doesn't return 202 until both the Postgres insert and the Kafka ack succeed, so clients were told the truth: every failed request was really not accepted.
- JSON logs and the drill's own receiver-side check made the root cause obvious within minutes.

## What went badly

- One shared, unguarded dependency turned a database blip into a total outage: every process died together.
- The first fix wasn't the whole story. Only re-running the same failure found the metrics crash. *Re-run the drill after every fix.*
- Nothing would have alerted for 2 minutes, and nothing at all once processes were restarted and errors were the only symptom.

## Action items (all done, same day)

- [x] **Ingest returns `503` + `Retry-After: 5` when Postgres is unavailable**, not `500 internal error`. `isUnavailable()` in `lib/infra.js` recognises refused, reset and timed-out connections, plus Postgres's own "shutting down" and "starting up" codes. A real bug still answers 500. Re-run: `{ '202': 1726, '503': 989 }`, with no 500s.
- [x] **kafkajs logs go through our logger** (`logCreator` in `lib/infra.js`). Its own logger printed every error as `{}`; now a failed batch logs `"err":"ECONNREFUSED"`.
- [x] **Drills for Redis and Redpanda:** both found real bugs, see [002](002-redis-outage-stalled-ingest.md) and [003](003-redpanda-outage-hung-ingest.md).
- [x] **Drill against the Kubernetes deployment:** see below.

## Kubernetes re-run (`bash loadtest/k8s-pg-outage.sh`)

Postgres is crash-stopped inside its pod (`pg_ctl stop -m immediate`, then Kubernetes restarts the container; the emptyDir data survives that, but wouldn't survive a pod deletion). Crashes show up as pod restart counts.

**The first run still crashed 4 pods**, with the original unhandled `'error'`. The cluster had never received the fix: the local image tag `reliant:dev` never changes, and with the default `imagePullPolicy: IfNotPresent` the node kept running the first `reliant:dev` it had cached. `docker build` followed by `kubectl rollout restart` *looked* like a deploy and silently ran the old code. Fixed in `k8s/local/kustomization.yaml` by patching every Reliant workload to `imagePullPolicy: Always`. A pod now runs `grep` against its own `/app/lib/infra.js` to confirm what's deployed.

With the fixed image:

```
responses by status: { '202': 2038, '503': 456 }
accepted: 2038, delivered at receiver: 2038, not delivered: 0
pod restarts before: consumer=0 dashboard=0 ingest=0 ingest=0 retry-publisher=0 worker=0
pod restarts after:  consumer=0 dashboard=0 ingest=0 ingest=0 retry-publisher=0 worker=0
```

The error window was about 14s here, longer than the 2s outage under docker-compose: a container restart plus Postgres crash recovery takes longer. While the pod had no ready endpoint, connections to the Service hung rather than being refused, so requests waited for the new 5s `connectionTimeoutMillis` and then got a 503. Without that timeout they would have hung indefinitely.

**Lesson:** a drill also tests the deploy pipeline. "Rolled out successfully" isn't the same as "running the new code".
