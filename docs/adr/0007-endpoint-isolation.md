# ADR-007: Endpoint isolation through a bounded inline attempt and a separate retry queue

**Status:** Accepted (Day 2 design, extended Day 3)

## Context

Requirement: a slow or failing endpoint must not delay delivery to other endpoints, including other endpoints of the same tenant (spec v2 §1.1.4). Kafka processes one partition strictly in order, so anything that waits inside `eachMessage` stalls every tenant hashed to that partition.

Day 2 reproduced this with `fake-slow-endpoint.js`: a tenant_b event held for 15 seconds blocked tenant_a's event behind it. Adding partitions only made a collision less likely; it didn't remove it. Day 3 measured that all three test tenants hash to the same partition.

The spec's v2 fix was a 10-second timeout, committing after the first attempt, and bounded concurrency within each partition. This design reaches the same goal differently.

## Decision

Bound the time each message can spend in the consumer, and move all retrying out of the consumer:

1. **One inline attempt, capped at 3 seconds** (`INLINE_TIMEOUT_MS` in `consumer.js`). On success, done. Otherwise the pair is written to `delivery_attempts` and the consumer moves on. The worst case for a message is about 3 seconds, however broken the endpoint is.
2. **Skip the inline attempt when we already know it will fail:** when the endpoint's circuit breaker isn't closed, or when the endpoint already has an earlier pending delivery (which is also needed for ordering; see ADR-008).
3. **One event's endpoints are attempted concurrently** (`Promise.all`), so a tenant's slow endpoint doesn't add its 3 seconds to that tenant's healthy endpoints for the same event.
4. **Retries run in `delivery-worker.js`** (ADR-002), one row per endpoint per sweep, in parallel, with a per-tenant cap. One slow endpoint's 20-second retry timeout doesn't hold up a different endpoint's retry.
5. **Circuit breaker** (`core/circuit-breaker.js`, state in Redis): after 5 consecutive failures the endpoint isn't called at all until a cooldown passes, then one trial at a time.

## Consequences

- A permanently down endpoint costs its partition at most about 3 seconds per event, until the breaker opens. After that it costs essentially nothing, because events are queued without a request.
- **It's still not zero.** A slow endpoint whose breaker is closed still adds up to 3 seconds per event to everything behind it in the same partition. The spec's load test ("healthy endpoints' p95 is statistically unchanged") has not been run yet, and it's the real test of this ADR.
- The 3-second cap is tighter than the spec's 10 seconds. Endpoints that take 3 to 20 seconds to respond always go through the retry path, which has growing timeouts (5s up to 20s), so they get delivered, just later.
- The breaker counts failures per endpoint, regardless of cause. A poison event used to be able to open it for a healthy endpoint; since ADR-008 a healthy ping resets the count and the event is set aside after 3 attempts.

## Alternatives considered

- **More partitions:** reduces collisions but can't eliminate them (Day 2 point 6).
- **Bounded concurrency within a partition (the spec's v2 approach, or a tool like Confluent's parallel consumer):** more throughput, but committing offsets becomes tricky, because a partition's committed offset is one number and you can't mark message N+1 done while message N is still in flight. Day 2 went into exactly why kafkajs avoids this.
