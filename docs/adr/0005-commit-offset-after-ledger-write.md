# ADR-005: Commit the Kafka offset only after the ledger write

**Status:** Accepted (implemented Day 3)

## Context

A delivery attempt involves three steps that can each fail independently: the HTTP call, the record of its outcome, and the Kafka offset commit that tells the broker "this message is done." Their order decides what a crash can lose.

## Decision

The record comes first, then the commit. In `consumer.js`, all of a message's work happens inside `eachMessage`: the inline attempt, its `delivery_ledger` row (written by `lib/deliver.js`), and, on failure, its `delivery_attempts` row. kafkajs only treats an offset as done after `eachMessage` resolves. If any of those writes throws, `eachMessage` rejects and the message is redelivered.

**Even a clean inline success writes one ledger row.** This partly reverses the Day 2 decision to make healthy deliveries cost zero database writes. Without it, a successful delivery leaves no trace, which breaks the spec's core promise of an auditable history for every accepted event.

## Why this order and not the reverse

Suppose the order were reversed and the process crashed after a successful HTTP call:
- **Commit first:** the message is gone from Kafka, but nothing records that it was delivered or even attempted. The event silently disappears from the system's view. That is the one failure this system exists to prevent.
- **Record first (this decision):** the message is redelivered, the endpoint gets a **duplicate**, and the ledger correctly shows two attempts. Duplicates are already allowed by the at-least-once contract, and every request carries `webhook-id` (= `event_id`) so receivers can deduplicate.

## Consequences

- **The duplicate window is wider than "one message."** kafkajs doesn't commit after every message; it commits resolved offsets in batches. A crash can therefore redeliver every message processed since the last commit. For messages that ended up in the retry queue, the unique index on `delivery_attempts (event_id, endpoint_id)` with `ON CONFLICT DO NOTHING` makes a redelivery a no-op. **Messages that succeeded inline will be sent to the customer again.**
- One extra Postgres write per delivery. At high throughput, ledger writes become the bottleneck before Kafka does. The spec's answer is batching (group commit), to be measured in the load tests.
- If Postgres is down, the consumer stops making progress rather than delivering without a record. That is the intended trade: no delivery without a record.

## Alternatives considered

- **Commit first:** rejected for the silent-loss case above.
- **Exactly-once using Kafka transactions:** that only covers Kafka-to-Kafka. The side effect here is an HTTP call to a server we don't control, so it can't be part of the transaction. Exactly-once delivery to customers is explicitly a non-goal (spec v2 §1.1.13).
