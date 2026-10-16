# ADR-001: Postgres is the durable event archive, independent of Kafka retention

**Status:** Accepted (implemented Day 1, hardened Day 3)

## Context

Tenants must be able to replay an event for at least 30 days. Kafka retention is usually days and is an operational setting. Anyone can shorten it to save disk without knowing replay depends on it. Saying "it's in Kafka" is not a durability guarantee for this requirement.

## Decision

Every accepted event is written to the Postgres `events` table **before** it is published to Kafka (`ingest-api.js`). Kafka is treated only as the delivery queue. Replay never reads from Kafka: `POST /api/deliveries/:event_id/:endpoint_id/replay` (`dashboard-api.js`) re-queues the pair in `delivery_attempts`, and the worker reads the payload from `events`.

The ingest path has two durability boundaries, and the client gets its 202 only after both:

1. `INSERT INTO events`. The `event_id` primary key also makes a re-submitted event a no-op (dedup).
2. Kafka ack from the idempotent producer, then `published_to_kafka = TRUE`.

If step 2 fails, the API returns 503. The event is already safe and `retry-publisher.js` will publish it, so a client retry is also safe.

## Consequences

- Replay works for any event still in Postgres, however old, regardless of Kafka retention.
- Every event costs one Postgres insert on the hot ingest path. Postgres insert rate becomes the ingest ceiling before Kafka does (to be measured in the load tests).
- An event can exist in Postgres but not yet in Kafka. That window is real, and `retry-publisher.js` exists to close it. It skips events younger than 30 seconds so it doesn't race an in-flight ingest request.
- **Retention:** `retention.js` (`npm run retention -- --apply`) archives events older than `RETENTION_DAYS` (default 35, refuses < 30) together with their ledger and finished delivery rows to gzip JSON-lines files, then deletes them. Events with a pending delivery are never touched. With `ARCHIVE_BUCKET` set the files go to S3, encrypted at rest under `KMS_KEY_ID` (tested against LocalStack); otherwise to local files. Kafka messages whose archive row is gone are sent to the DLQ topic by the consumer instead of stalling it.

## Alternatives considered

- **Kafka as the archive (long or infinite retention, or tiered storage):** one fewer write per event, but replay would mean scanning a topic by offset or time, a payload couldn't be looked up by `event_id`, and the replay guarantee would hang on a broker config value.
- **Transactional outbox (one Postgres transaction writes the event plus an outbox row; a relay publishes from the outbox):** removes the dual-write window entirely. It's effectively what `events.status` + `retry-publisher.js` already approximate. Worth formalizing if the 503 path turns out to be common.
