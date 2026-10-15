# ADR-002: Retries live in a Postgres queue, not Kafka retry topics

**Status:** Accepted (Day 4). **This reverses the spec's ADR-002**, which chose time-bucketed Kafka retry topics (`webhooks.retry.{1m,5m,30m,2h,12h}`).

## Context

The spec preferred Kafka retry topics because they put no polling load on Postgres and scale like any other consumer group. On Day 2, reproducing the slow-endpoint bug led somewhere else: retries had to be **decoupled from Kafka partitions**. With 3 partitions and tenants hashed onto them, `tenant_a`, `tenant_b` and `tenant_c` all land on partition 2 (measured on Day 3), so anything that waits inside a partition stalls unrelated tenants.

Retry topics would also be partitioned by `tenant_id`, so they would bring back the same coupling. Getting strict per-endpoint ordering out of them would additionally need extra bookkeeping on top, because the spec's retry consumers order by bucket, not by endpoint.

## Decision

A failed inline attempt is written to `delivery_attempts` (one row per (event, endpoint) pair) and the consumer moves on. `delivery-worker.js` polls that table:

- **Oldest pending row per endpoint** via `DISTINCT ON (endpoint_id) ... ORDER BY created_at`, chosen *before* filtering on whether it's due. Filtering first was a real ordering bug, found and fixed on Day 3.
- **Leases instead of row locks.** `FOR UPDATE SKIP LOCKED` is not allowed together with `DISTINCT ON`, so a worker claims rows with `UPDATE ... SET locked_until = NOW() + 60s WHERE locked_until IS NULL OR locked_until < NOW()`. Postgres re-checks that condition under the row lock, so if two workers race, exactly one wins. If a worker dies, its lease expires and another worker takes over.
- **Backoff** comes from `core/backoff.js` (default 1m, 5m, 30m, 2h, 12h; configurable via `RETRY_SCHEDULE_MINUTES`).
- **Per-tenant cap per sweep** (`MAX_PER_TENANT_PER_SWEEP`), so one tenant with many failing endpoints can't take a whole worker.

## Consequences

- Retry parallelism scales with the number of endpoints and workers, not partitions. Any number of workers can run.
- Per-endpoint ordering is enforced structurally by one query, with no extra coordination (see ADR-008 for what that costs).
- Retry state is queryable: the dashboard reads current state straight from `delivery_attempts`.
- **Cost: polling load on Postgres**, proportional to worker count × poll frequency, and the `DISTINCT ON` scans every pending row on each poll. The partial index `delivery_attempts_pending` keeps that cheap at portfolio scale. The load tests must find where it stops being cheap.
- **Cost: retry timing is coarse by up to one poll interval** (default 10s). The spec accepts the same trade-off for retry topics.
- Postgres is now on the hot path for retries as well as ingest, so Postgres capacity matters more than it would under the spec's design.

## Alternatives considered

- **Kafka retry topics (the spec's choice):** no polling load, but brings back the partition coupling above, and per-endpoint ordering would need extra bookkeeping on top.
- **Delayed-message broker features (RabbitMQ delayed exchange, SQS delay queues):** an extra system to run, with the same ordering problem.

## To revisit

If the load tests show polling to be the bottleneck, first try `LISTEN/NOTIFY` to wake workers instead of a fixed poll, and batching the ledger writes, before reconsidering retry topics.
