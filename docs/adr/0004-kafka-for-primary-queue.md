# ADR-004: Kafka (not RabbitMQ) for the primary delivery queue

**Status:** Accepted (chosen before Day 1; locally it runs on Redpanda, which speaks the Kafka protocol)

## Context

Accepted events need to get from the ingest API to delivery workers durably, in per-tenant order, with consumers that can come and go.

## Decision

`webhooks.events` is a Kafka topic with 3 partitions, keyed by `tenant_id`, consumed by the `delivery-workers` consumer group.

What Kafka **actually provides in this codebase** (as opposed to in the original pitch):

- **Per-tenant ordering into the consumer.** The same key goes to the same partition, which is processed in order. That ordering is what the per-endpoint queue in ADR-008 is built on top of.
- **Consumer-group rebalancing.** If a consumer dies, its partitions move to another one, and uncommitted messages are redelivered (ADR-005).
- **A durable buffer between ingest and delivery.** Ingest keeps accepting while every consumer is down. The recovery benchmark (spec v2 §1.2) is about draining exactly that backlog.

## Consequences

- **Replay doesn't use Kafka at all**, contrary to the spec's original reason ("replayability"). It comes from the Postgres archive (ADR-001). Retries don't use Kafka either (ADR-002). Kafka's job here is narrower than the spec assumed: an ordered, durable intake buffer.
- Parallelism in the consumer is capped by the partition count (3). That is acceptable only because the consumer does a bounded amount of work per message (ADR-007).
- **Changing the partition count changes which partition a tenant maps to.** Events already in flight under the old mapping can be processed out of order relative to new ones. On Day 3 the topic went from 1 to 3 partitions with zero consumer lag at that moment, which is the safe way to do it.
- The topic is now created explicitly in code (`ensureTopics` in `lib/infra.js`) rather than auto-created. The Day 2 to Day 3 gap, where the topic quietly had 1 partition, came from auto-creation.

## Alternatives considered

- **RabbitMQ:** simpler per-message acks, and a slow message doesn't block the messages behind it. But it has no per-key ordering across competing consumers without single-active-consumer tricks, and no long-lived log to drain after an outage.
- **Postgres as the only queue (no broker):** fewer moving parts. Given ADR-002 this is closer than it sounds. But it puts every event's fan-in on the database the ledger already loads. Worth benchmarking rather than dismissing.
