# Load test results (Days 4–6)

Spec §14 and v2 §5, scaled to one machine. Harness: `loadtest/run.js` (`ingest | isolation | recovery | recovery-spec | dashboard`).

**Environment:** Windows 11, 20 logical cores, Node 22. Postgres 16, Redis 7 and Redpanda (1 node) all run in Docker Desktop on the same machine, with the default 10-connection `pg` pool per process. The load generator, the services and the databases share one machine, so the absolute numbers are **lower bounds**. What carries over to real hardware is where each bottleneck sits.

**Method:** delivery latency is measured inside Postgres, from `events.created_at` to the start of the first ledger attempt (`attempted_at - latency_ms`), so everything uses one clock. Customer endpoints are a local server that answers 200 at once (`/ok`) or never answers (`/hang`).

## Summary

| Spec target | Result | Verdict |
|---|---|---|
| Ingest 1,000 events/s baseline | ~950/s per process; 2 processes ~1,290/s; 4 processes ~1,930/s | **Met with 2+ processes** |
| Healthy endpoints' p95 unaffected by a broken endpoint of the same tenant | Steady state: unaffected. **First 5s after an endpoint breaks: p95 1.7s** (§5; was 5.8s over ~10s) | **Mostly met:** a sub-5s blip, accepted in ADR-007 |
| Recovery: 600,000-event backlog drained within 5 minutes | **139s** with 6 consumers + 2 workers, 12 partitions: ~4,300 events/s, all 600,000 in the ledger, 0 duplicates (§7) | **Met** |
| Delivery-state query p95 < 200ms (history < 500ms) | 13–23ms (history 15–34ms) over 1M ledger rows (§6) | **Met** |

## 1. Ingest throughput

Closed loop (each sender fires again as soon as it gets a response), 10s per step, 30 tenants, no endpoints, so this measures ingest alone.

| Version | Peak events/s | p95 at peak |
|---|---|---|
| Day 3 code | ~725 | 121ms |
| + batched Kafka publish | ~690 (no change) | 145ms |
| + tenant FK instead of a lookup, one UPDATE per batch | ~960 | 89ms |
| same, 2 ingest processes | ~1,290 | 143ms |
| same, 4 ingest processes | ~1,930 | 45–181ms |

**How the bottleneck was found.** The obvious suspect was disproven first, then each stage was measured on its own at 64-way concurrency:

| Stage | Throughput alone |
|---|---|
| Kafka send, idempotent producer, one message per send | ~1,300/s, flat regardless of concurrency (kafkajs handles an idempotent producer's sends one at a time) |
| Same, batched (group commit) | ~2,470/s |
| Postgres trivial query / INSERT / UPDATE (10 connections) | ~3,100 / ~2,800 / ~3,100 per s |
| Redis INCR | ~28,000/s |

Each request made **3 sequential Postgres round trips** (check the tenant, insert, mark published), which caps out around 1,000/s, and that ceiling sat below the Kafka one. Batching Kafka alone changed nothing. Removing the round trips did:

- **Tenant check moved into the database:** `events.tenant_id REFERENCES tenants`, so the INSERT rejects unknown tenants itself (Postgres error 23503 becomes a 400).
- **One `UPDATE ... WHERE event_id = ANY($1)` per Kafka batch** instead of one per event.
- **Kafka group commit** (`sendBatched` in `lib/infra.js`): while one send is in flight, queue new events, then send them all as one batch. There's no artificial delay, arrival order is kept, and idempotence stays on. Dropping idempotence would be faster (17k sends/s), but retried sends could then reorder a partition.

After that, **one process is CPU-bound** (ingest at ~84% of one core, the generator at ~89%), and adding stateless processes scales throughput. Gains flatten at 4 processes. **Next bottleneck: each event is its own Postgres transaction**, with its own commit to disk. To get past ~2,000/s, batch the inserts too (one multi-row INSERT per batch), the same group-commit idea.

## 2. Endpoint isolation (spec v2 §5)

100 events/s for 30s, 3 tenants (`lt_0` and `lt_1` hash to partition 0, `lt_2` to partition 2).

| Run | Healthy deliveries | p50 | p95 | p99 |
|---|---|---|---|---|
| Baseline: 2 healthy endpoints per tenant | 4,936 | 20ms | 42ms | 59ms |
| + 1 hanging endpoint per tenant | 4,796 | 1,048ms | **5,084ms** | 5,871ms |

p95 per 5-second window of event creation:

```
baseline:   0-5s 42ms | 5-10s 38ms   | 10-15s 52ms   | 15-20s 39ms   | 20-25s 45ms | 25-30s 33ms
hanging:    0-5s 5926 | 5-10s 3755ms | 10-15s 2653ms | 15-20s 1188ms | 20-25s 36ms | 25-30s 65ms
```

**What happens:**
1. Each tenant's *first* event to the hanging endpoint is attempted inline. Kafka processes a partition one message at a time, so that one attempt holds up **every tenant on the partition** for the full 3-second inline timeout. Two tenants share partition 0, so that partition loses about 6 seconds.
2. That failure creates a pending row. Every later event for that endpoint queues behind it without an inline attempt (ADR-008's ordering rule), so the stall doesn't repeat.
3. The backlog built during the stall takes about 20 seconds to work off at 100 events/s, because the consumer's spare capacity is small (see recovery below).

So in steady state the ADR-007 design holds, but **each newly broken endpoint costs its whole partition one inline timeout**. That's worse for an endpoint that alternates between slow and recovered: each time its queue empties, the next event goes inline again. The breakers stayed closed, because almost no requests reached the hanging endpoint after the first one.

## 3. Recovery (spec v2 §1.2)

Consumer offline while ingesting at a target of 300/s for 60s (the generator reached ~222/s): a backlog of **13,317 events**. Consumer restarted, with one healthy endpoint per tenant and 30 tenants.

| | |
|---|---|
| Drain time (consumer start to last delivery, including the group join) | **74.1s** |
| Average drain rate | **~180 events/s** |
| Busiest seconds | 347, 334, 331, 331, 325 deliveries/s |
| Spec target | 600,000 in 300s = **2,000 events/s** |

**One consumer handles ~110 messages/s per partition**, about 9ms each. Each message makes about 6 sequential round trips: the endpoint plus archive lookup, the "pending ahead" check, a breaker read, the HTTP POST, the ledger insert, and a breaker read-then-write. With 3 partitions, that caps the whole pipeline near 340/s. **The consumer, not ingest, is the system's throughput limit.** At the 1,000/s ingest baseline, lag would grow without bound.

Ways to raise it, cheapest first:
1. **More partitions and more consumer processes.** Throughput scales with partitions × consumers. The partition count is the cap, and it's fixed at 3 today (`EVENT_PARTITIONS`). Changing it on a live topic remaps tenants (ADR-004).
2. **Fewer round trips per message:** combine the breaker read with the pending check, and write the breaker state only when it changes.
3. **`eachBatch` instead of `eachMessage`:** process a batch with one multi-row ledger INSERT. That's the spec §10 group-commit advice, applied to the consumer.

## 4. After batching the consumer (`eachBatch`)

The consumer now works in chunks of up to 200 messages per partition, and does its bookkeeping once per chunk instead of once per message:
- one query for the archive check, one for endpoints, one for pending rows
- one Redis `MGET` for breaker states
- one multi-row ledger `INSERT`, one multi-row queue `INSERT`
- breaker writes only for states that changed

Per-endpoint order is kept: each endpoint's events in a chunk are attempted one after another, and anything after a failure or an earlier pending row is queued behind it. A new **3-second budget per endpoint per chunk** stops a slow-but-working endpoint from holding a partition for N × timeout. Offsets are resolved per chunk, only after its writes succeed (ADR-005). Decrypted signing secrets are also cached by ciphertext, because decrypting them per event was measurable CPU.

**Recovery** (backlog ~26k–36k events, 30 tenants, steady-state rate from 5-second windows):

| Consumers | Steady drain rate | Note |
|---|---|---|
| 1, per-message (before) | ~110–340/s | |
| 1, batched | **~800–960/s** | Repeatable over 3 runs. One run reached 2,200–2,570/s and could not be reproduced. |
| 3, batched | **~1,480–1,780/s** at peak | Falls to ~700–870/s at the end, once the lighter partitions are done |

One consumer now uses ~119% CPU while Docker (Postgres + Redis + Redpanda) uses ~18%, so **the consumer is CPU-bound**, not the database. Three consumers help only up to the partition count. The tail is **partition skew**: 30 tenants don't hash evenly onto 3 partitions, so at the end one consumer is left on the busiest partition. **Next lever: more partitions** (e.g. 12), which also means more consumers can be useful. Changing the count on a live topic remaps tenants (ADR-004), so do it at zero lag.

**Isolation**, same setup as §2:

| Run | p50 | p95 | p99 |
|---|---|---|---|
| Baseline | 25ms | 86ms | 162ms |
| + 1 hanging endpoint per tenant | **35ms** (was 1,048ms) | 4,593ms | 5,785ms |

```
hanging:  0-5s 5830ms | 5-10s 1904ms | 10-15s 100ms | 15-20s 75ms | 20-25s 61ms | 25-30s 126ms
```

The transient now clears in ~10s instead of ~20s, and the median is unaffected. **What remains is structural:** kafkajs fetches a partition's next batch only after the current one returns, so the first 3-second inline timeout against a newly broken endpoint still delays everything behind it on that partition. Baseline p95 rose from 42ms to 86ms, because a chunk's events to one endpoint are now sent one after another.

Ways to remove the remaining stall (an ADR-007 decision):
1. **Shorter inline timeout** (e.g. 1s): one line, cuts the stall about 3x, and sends more slow-but-healthy deliveries to the worker.
2. **Queue first, attempt in the background:** write the queue row, commit the offset, attempt outside the batch. No stall at all, but every delivery costs a write again (the Day 2 trade-off).
3. **Pipeline batches:** keep fetching while attempts are in flight, and commit only up to the lowest finished offset. No stall and no extra writes, but the most complex option.

## 5. Endpoint isolation re-measured (Day 6: 1s inline timeout, 12 partitions)

Same setup as §2. `lt_0` and `lt_1` still share a partition (9 of 12), so the cross-tenant case is covered.

| Run | p50 | p95 | p99 |
|---|---|---|---|
| Baseline | 24ms | 50ms | 66ms |
| + 1 hanging endpoint per tenant | 26ms | **594ms** (was 4,593ms) | 1,625ms |

```
baseline: 0-5s 53ms   | 5-10s 55ms | 10-15s 44ms | 15-20s 54ms | 20-25s 46ms | 25-30s 49ms
hanging:  0-5s 1678ms | 5-10s 44ms | 10-15s 43ms | 15-20s 57ms | 20-25s 52ms | 25-30s 53ms
```

The transient is now under 5 seconds, peaking at a p95 of 1.7s. Every window after that matches the baseline. ADR-007 records the decision to accept the remaining blip rather than pipeline batches.

## 6. Dashboard query latency (Day 6)

`npm run loadtest -- dashboard`: one tenant, 200,000 events × 5 endpoints = **1,000,000 ledger rows over 30 days** in monthly partitions, 5% of them still in the retry queue. Measured through the real dashboard API: 1,000 random events for the state query, 300 history requests.

| Query | Concurrency | p50 | p95 | p99 | Target |
|---|---|---|---|---|---|
| State ("what happened to my webhook") | 1 | 8ms | 13ms | 17ms | p95 < 200ms: **met** |
| History (last 100 attempts of an endpoint) | 1 | 9ms | 15ms | 20ms | p95 < 500ms: **met** |
| State | 8 | 14ms | 23ms | 38ms | **met** |
| History | 8 | 24ms | 34ms | 43ms | **met** |

Both queries are index lookups: `ledger_pair` for the state query and `ledger_dashboard` for history, each partitioned by month. So they stay flat as history grows, and a separate `delivery_status` projection table (spec v2 §2) isn't needed to meet the target.

## 7. Recovery benchmark at the spec's size (Day 6)

`npm run loadtest -- recovery-spec`: the spec's condition (v2 §1.2) is consumers offline for 10 minutes at 1,000 events/s. The **600,000-event backlog** is written straight to Postgres and Kafka exactly as ingest writes it (28s), because this measures the drain, not ingest. Then 6 consumers and 2 workers start at once. 30 tenants with one healthy endpoint each; 12 partitions.

```
t+10s 51,745 | t+20s 131,083 | t+30s 211,580 | t+40s 284,861 | t+50s 340,168 | ... | t+100s 557,726
drained 600,000 in 139s (consumer start + group join included) = 4,326 events/s
ledger: 600,000 delivered; duplicates at receiver: 0
```

**Met: 139s against a 300s target**, about 2.2x the required 2,000 events/s, verified against the ledger.

The changes since §4's ~1,500/s: 12 partitions instead of 3 (more consumers can work), and the backlog spread evenly over them.

- **Workers count as delivery capacity.** The first attempt ran consumers only. It handed 99,664 events to the retry queue: once an endpoint's 200-event chunk took longer than its 1s budget (ADR-007), the rest of the chunk was queued, and ordering queued everything after it. With no worker running, that run never finished. A repeat with 2 workers added queued nothing. The difference between the two runs wasn't investigated; contention on this shared machine is the likely cause. Either way, a real recovery runs both, so the benchmark now does.
- **Not measured:** how many consumers are the minimum to meet the target, and behaviour with slow endpoints mixed into the backlog.

## Bugs found by load testing (all fixed)

- **Ingest throughput:** 3 Postgres round trips per request, fixed as described in §1.
- **Consumer stall:** a Kafka message whose `events` row is missing failed the ledger foreign key inside `eachMessage`, so kafkajs retried it forever and the **partition stalled permanently**. It can't happen today, because nothing deletes events, but the ADR-001 retention job would cause it. The consumer now checks the archive row in the same query as the endpoint lookup, and sends such messages to the DLQ topic as `processing_error`.
- **Harness bugs:** leftover Kafka backlog from the ingest ramp (the harness now refuses to start if the group has lag, and moves the group past its own messages with explicit offsets, because `resetOffsets` commits `-1`, which a `fromBeginning` consumer reads as "start over"), and an off-by-one in the recovery target.

## Not yet tested

- Delivery latency at higher rates and with slow-but-answering endpoints (spec §14 row 2)
- Worker-pool scaling (more `delivery-worker.js` processes) and ledger write throughput under retry load (§14 rows 3 and 7)
- Thundering herd is covered by an integration test instead (`tests/pipeline.test.js`): 3 workers, half-open breaker, 40-event backlog; the endpoint saw at most 1 request at a time, in order
