# ADR-005: Commit the Kafka offset only after the ledger write

**Status:** Accepted (implemented Day 3)

## Context

A delivery attempt involves three steps that can each fail independently: the HTTP call, the record of its outcome, and the Kafka offset commit that tells the broker "this message is done." Their order decides what a crash can lose.

## Decision

The record comes first, then the commit. `consumer.js` processes each Kafka batch in chunks of up to 200 messages (`eachBatch`, auto-resolve off). For each chunk it makes the inline attempts, then writes all their `delivery_ledger` rows and any `delivery_attempts` rows (one multi-row `INSERT` each), and **only then** calls `resolveOffset` for the chunk's last message. If a write throws, the chunk's offsets are never resolved and its messages are redelivered.

**Even a clean inline success writes one ledger row.** This partly reverses the Day 2 decision to make healthy deliveries cost zero database writes. Without it, a successful delivery leaves no trace, which breaks the spec's core promise of an auditable history for every accepted event.

## Why this order and not the reverse

Suppose the order were reversed and the process crashed after a successful HTTP call:
- **Commit first:** the message is gone from Kafka, but nothing records that it was delivered or even attempted. The event silently disappears from the system's view. That is the one failure this system exists to prevent.
- **Record first (this decision):** the message is redelivered, the endpoint gets a **duplicate**, and the ledger correctly shows two attempts. Duplicates are already allowed by the at-least-once contract, and every request carries `webhook-id` (= `event_id`) so receivers can deduplicate.

## Consequences

- **The duplicate window is one chunk (up to 200 messages) plus whatever has been resolved but not yet committed.** A crash redelivers all of those. For messages that ended up in the retry queue, the unique index on `delivery_attempts (event_id, endpoint_id)` with `ON CONFLICT DO NOTHING` makes a redelivery a no-op. **Messages that succeeded inline will be sent to the customer again.** Batching traded a wider duplicate window for ~5x the throughput (see `docs/load-test-results.md`).
- One extra ledger row per delivery, written in one multi-row `INSERT` per chunk (group commit), so the consumer's database cost is per chunk, not per event.
- If Postgres is down, the consumer stops making progress rather than delivering without a record. That is the intended trade: no delivery without a record.

## Alternatives considered

- **Commit first:** rejected for the silent-loss case above.
- **Exactly-once using Kafka transactions:** that only covers Kafka-to-Kafka. The side effect here is an HTTP call to a server we don't control, so it can't be part of the transaction. Exactly-once delivery to customers is explicitly a non-goal (spec v2 §1.1.13).
