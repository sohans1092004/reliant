# Incident 003: a Kafka (Redpanda) outage made ingest requests hang ~35s

**Date:** 2026-10-15 · **Type:** induced failure · **Drill:** `npm run loadtest -- kafka-outage` · **Status:** resolved

## Summary

Ingest returns 202 only after both the Postgres insert and the Kafka ack. If Kafka is down, the event is already in Postgres: ingest should answer 503 ("saved, not yet queued"), and the retry-publisher publishes it later. We killed Redpanda for 30 seconds at 50 events/s.

**Durability held:** 2,565 of 2,565 events delivered. But ingest didn't answer quickly. **Requests hung for about 35 seconds** before getting their 503. Most HTTP clients time out sooner than that, so the caller would see a timeout and not know whether its event was accepted.

## Impact (first run)

| | |
|---|---|
| Ingest during the outage | no responses for ~35s (in flight peaked at 960), then 1,097 × 503 at once |
| Events | 2,565 accepted (202 or 503-saved), 2,565 delivered, 0 duplicates |
| Consumer | resumed ~30s *after* Redpanda was back |

## Root cause

With the broker gone, kafkajs retries a `send()` with backoff for ~30s before giving up. Ingest's group commit (`sendBatched`) sends one batch at a time, so every message queued behind the stuck send waited too, with no bound on how long.

## Fix

`sendBatched` (`lib/infra.js`) now gives every caller at most **3s** (`PUBLISH_TIMEOUT_MS`). After that the caller gets an error, so ingest answers 503 + `Retry-After` and the event is already safe in Postgres. A message still waiting in the queue is removed from it, and the retry-publisher sends it later. A message already inside the in-flight batch may still land, which just sets `published_to_kafka` (at-least-once, as before).

## Verification (same drill, after the fix)

```
responses by status: { '202': 1667, '503': 951 }
accepted: 2618, delivered at receiver: 2618, not delivered: 0
duplicate deliveries: 1
process exits: none
```

Every request during the outage got its 503 within about 3s. Ingest returned 202 again within about 1s of Redpanda coming back.

## Known, not changed: the consumer resumes ~30s after the broker returns

The consumer crashed with `KafkaJSNoBrokerAvailableError`, and kafkajs restarted it, but it couldn't leave the group while the broker was down. After the broker restart, the group waits out the old member's **session timeout (30s, the kafkajs default)** before rebalancing. Events aren't lost, just delayed. They were already in Kafka, and one consumer drains a backlog at ~900 events/s.

Lowering `sessionTimeout` would shorten the delay, but a consumer busy with a slow chunk (up to 1s per endpoint, ADR-007) could then be kicked out of the group mid-batch, causing needless rebalances. Revisit together with heartbeat calls inside `eachBatch`.
