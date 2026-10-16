# ADR-002: Retries live in a Postgres queue, not Kafka retry topics

**Status:** Accepted (Day 4). **This reverses the spec's ADR-002**, which chose time-bucketed Kafka retry topics (`webhooks.retry.{1m,5m,30m,2h,12h}`).

## Context

The spec preferred Kafka retry topics because they put no polling load on Postgres and scale like any other consumer group. On Day 2, reproducing the slow-endpoint bug led somewhere else: retries had to be **decoupled from Kafka partitions**. With 3 partitions and tenants hashed onto them, `tenant_a`, `tenant_b` and `tenant_c` all land on partition 2 (measured on Day 3), so anything that waits inside a partition stalls unrelated tenants.

Retry topics would also be partitioned by `tenant_id`, so they would bring back the same coupling. Getting strict per-endpoint ordering out of them would additionally need extra bookkeeping on top, because the spec's retry consumers order by bucket, not by endpoint.

## Decision

A failed inline attempt is written to `delivery_attempts` (one row per (event, endpoint) pair) and the consumer moves on. `delivery-worker.js` polls that table:

- **The front of each endpoint's queue.** Each claim ranks every pending row within its endpoint (`row_number() ... ORDER BY created_at, id`) and takes the longest run from the oldest row that is due and unlocked, up to 20 rows. The oldest row counts whether it's due or not: skipping a row that's still backing off would let a newer one overtake it (a real ordering bug, found and fixed on Day 3). The worker sends a batch one request at a time and stops at the first failure.
- **Leases instead of row locks.** `FOR UPDATE SKIP LOCKED` is not allowed together with `DISTINCT ON`, so a worker claims rows with `UPDATE ... SET locked_until = NOW() + 60s WHERE locked_until IS NULL OR locked_until < NOW()`. Postgres re-checks that condition under the row lock, so if two workers race, exactly one wins. If a worker dies, its lease expires and another worker takes over. Each claim also writes a fresh `lease` token, and every later write to the row requires it (`updateLeased`), so a worker that stalls past its 60s lease and then returns can't overwrite the new owner's state. It can't take back the POST it already sent: that duplicate is covered by `webhook-id`. Tested in `tests/pipeline.test.js` ("lease fencing").
- **Backoff** comes from `core/backoff.js` (default 1m, 5m, 30m, 2h, 12h; configurable via `RETRY_SCHEDULE_MINUTES`).
- **Per-tenant cap per sweep** (`MAX_PER_TENANT_PER_SWEEP`), so one tenant with many failing endpoints can't take a whole worker.

## Consequences

- Retry parallelism scales with the number of endpoints and workers, not partitions. Any number of workers can run.
- Per-endpoint ordering is enforced structurally by one query, with no extra coordination (see ADR-008 for what that costs).
- Retry state is queryable: the dashboard reads current state straight from `delivery_attempts`.
- **Cost: polling load on Postgres**, proportional to worker count × claim frequency, and each claim ranks every pending row (about 12ms at 10,000 pending). The partial index `delivery_attempts_pending` keeps that cheap at portfolio scale. The load tests must find where it stops being cheap.
- **Cost: retry timing is coarse by up to one poll interval** (default 10s). The spec accepts the same trade-off for retry topics.
- Postgres is now on the hot path for retries as well as ingest, so Postgres capacity matters more than it would under the spec's design.

## Alternatives considered

- **Kafka retry topics (the spec's choice):** no polling load, but brings back the partition coupling above, and per-endpoint ordering would need extra bookkeeping on top.
- **Delayed-message broker features (RabbitMQ delayed exchange, SQS delay queues):** an extra system to run, with the same ordering problem.

## To revisit

If the load tests show polling to be the bottleneck, first try `LISTEN/NOTIFY` to wake workers instead of a fixed poll, and batching the ledger writes, before reconsidering retry topics.

## Update: batches per endpoint (after the round 2 drills)

A rolling consumer restart at ~1,000 events/s pushed endpoints into this queue, and they never came back while the load lasted (`docs/drills.md` §5). The queue drained one row per endpoint per sweep, every sweep waited for its slowest endpoint, and each row cost two Postgres writes through a 10-connection pool. Measured with `npm run loadtest -- retry-drain` (100 endpoints × 200 queued rows, one worker, 3 interleaved runs each): **~10 rows/s per endpoint**, no faster than the drill's inflow, so a backlog never shrank.

Now a worker:
- claims up to 20 rows from the front of each endpoint's queue;
- sends them one at a time and stops at the first failure, handing the rest back at once;
- records each batch's successes in three writes (one ledger insert, the breakers, one lease-fenced update) instead of two writes per row;
- keeps up to 200 endpoint batches in flight and claims again whenever one finishes, instead of sweeping and waiting for the slowest endpoint.

Same benchmark: **~30 rows/s per endpoint** (3,040/s for the worker vs ~1,000/s), 0 order inversions. In the drill, p95 came back to under 150ms within 15–35s of the restart; before, it stayed at 11–38s until the load stopped.

Order and the thundering-herd guarantee are unchanged: still one request in flight per endpoint. A crash mid-batch re-sends that batch's unrecorded successes (duplicates, never a gap), as a consumer crash does with its chunk.
