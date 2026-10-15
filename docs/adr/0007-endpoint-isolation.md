# ADR-007: Endpoint isolation through a bounded inline attempt and a separate retry queue

**Status:** Accepted (Day 2 design, extended Day 3)

## Context

Requirement: a slow or failing endpoint must not delay delivery to other endpoints, including other endpoints of the same tenant (spec v2 §1.1.4). Kafka processes one partition strictly in order, so anything that waits inside `eachMessage` stalls every tenant hashed to that partition.

Day 2 reproduced this with `fake-slow-endpoint.js`: a tenant_b event held for 15 seconds blocked tenant_a's event behind it. Adding partitions only made a collision less likely; it didn't remove it. Day 3 measured that all three test tenants hash to the same partition.

The spec's v2 fix was a 10-second timeout, committing after the first attempt, and bounded concurrency within each partition. This design reaches the same goal differently.

## Decision

Bound the time each message can spend in the consumer, and move all retrying out of the consumer:

1. **One inline attempt per event, capped at 1 second** (`INLINE_TIMEOUT_MS` in `consumer.js`), **and a 1-second budget per endpoint per chunk** (`ENDPOINT_BUDGET_MS`). After the budget, an endpoint's remaining events in the chunk are queued for the worker without an attempt, so a slow-but-working endpoint can't hold the partition for N × timeout. An inline **timeout** is queued with no delay: the worker retries it on its next poll with a longer timeout (8s), because 1 second only proves the endpoint is slow, not broken. Other failures back off normally. (Day 4: lowered from 3s after the load test showed the first 3s timeout against a newly broken endpoint degraded its whole partition for ~10s.)
2. **Skip the inline attempt when we already know it will fail:** when the endpoint's circuit breaker isn't closed, or when the endpoint already has an earlier pending delivery (which is also needed for ordering; see ADR-008).
3. **Different endpoints are attempted concurrently** within a chunk (`Promise.all`); one endpoint's events are attempted strictly one after another.
4. **Retries run in `delivery-worker.js`** (ADR-002), one row per endpoint per sweep, in parallel, with a per-tenant cap. One slow endpoint's 20-second retry timeout doesn't hold up a different endpoint's retry.
5. **Circuit breaker** (`core/circuit-breaker.js`, state in Redis): after 5 consecutive failures the endpoint isn't called at all until a cooldown passes, then one trial at a time.

## Consequences

- A permanently down endpoint costs its partition at most about 1 second per pending cycle (in practice once, since later events queue behind the pending row), until the breaker opens. After that it costs essentially nothing.
- **It's still not zero, and the load test shows it** (`docs/load-test-results.md` §2 and §4). In steady state, healthy endpoints are unaffected. But kafkajs fetches a partition's next batch only after the current one returns, so the **first** inline timeout against a newly broken endpoint delays everything behind it on that partition. With the batched consumer this transient lasts ~10s (healthy p95 up to ~5.8s in the first 5s). Three ways to remove it are listed in the results doc. That's an open decision.
- The 1-second cap is much tighter than the spec's 10 seconds. Endpoints that take 1 to 20 seconds to respond always go through the worker: the first retry is immediate, with an 8s timeout, so they're delivered within about one worker poll (≤10s) rather than inline. That's the trade: slow customers get a few seconds of extra latency so they can't stall everyone else on their partition.
- The breaker counts failures per endpoint, regardless of cause. A poison event used to be able to open it for a healthy endpoint; since ADR-008 a healthy ping resets the count and the event is set aside after 3 attempts.

## Alternatives considered

- **More partitions:** reduces collisions but can't eliminate them (Day 2 point 6).
- **Bounded concurrency within a partition (the spec's v2 approach, or a tool like Confluent's parallel consumer):** more throughput, but committing offsets becomes tricky, because a partition's committed offset is one number and you can't mark message N+1 done while message N is still in flight. Day 2 went into exactly why kafkajs avoids this.
