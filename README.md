# Reliant

Webhook delivery with durable accountability: every accepted event is delivered, retrying on a known schedule, or dead-lettered and replayable. Never silently lost. Spec: `../# Reliant — Engineering Specification.md`.

## Run it

```sh
docker compose up -d          # Postgres, Redis, Redpanda
cp .env.example .env          # fill in DB creds, SECRET_ENCRYPTION_KEY, INGEST_TOKEN
npm install
npm run migrate               # schema + Kafka topics + signing secrets (printed once)

npm run ingest                # :3000  POST /events
npm run consumer              # Kafka -> inline delivery attempt
npm run worker                # retries, run as many as you like
npm run retry-publisher       # re-publishes events whose Kafka publish failed
npm run dashboard             # :3001  UI + API, dev keys: dev_key_tenant_a / dev_key_tenant_b

node producer.js              # send a test event (TEST_TENANT_ID=tenant_b for the slow one)
```

## Tests

```sh
npm test                      # core: backoff, circuit breaker, signing, validation (no I/O)
npm run test:integration      # needs docker compose up and no consumer/worker running:
                              # tenant isolation + full pipeline end to end
```

## Layout

| Path | What |
|---|---|
| `core/` | Pure logic, no I/O (spec §4 / Phase 1) |
| `lib/infra.js` | Shared Postgres / Kafka / Redis clients, config, topic setup |
| `lib/deliver.js` | One delivery attempt: SSRF check, HMAC sign, POST, ledger row, breaker update |
| `lib/breaker.js` | Circuit-breaker state in Redis |
| `ingest-api.js` | Validate, dedup, archive in Postgres, publish to Kafka, then 202 |
| `consumer.js` | Inline attempt (3s cap) or queue in `delivery_attempts` |
| `delivery-worker.js` | Leased retries, oldest-first per endpoint, backoff, DLQ |
| `dashboard-api.js` + `public/` | Tenant API + UI |
| `schema.sql` | Idempotent schema, also applied by `npm run migrate` |

## Receiver contract

Every request is a signed `POST` with `webhook-id` (= `event_id`), `webhook-timestamp` and `webhook-signature` (`v1,<base64 HMAC-SHA256 of id.timestamp.body>`; two signatures, space-separated, during a secret rotation). See `verify()` in `core/signing.js`. Receivers should:

- return 2xx only once the event is handled; anything else is retried
- deduplicate on `webhook-id`, because delivery is at-least-once
- answer `type: "reliant.ping"` with 2xx and do nothing else; Reliant uses it to tell a broken endpoint from one bad event

## Where this deviates from the spec

Decisions and their trade-offs are in [`docs/adr/`](docs/adr/README.md). The two that differ from the spec:

- **[ADR-002](docs/adr/0002-retry-queue-in-postgres.md):** retries use a Postgres queue (`delivery_attempts`), not Kafka retry topics.
- **[ADR-008](docs/adr/0008-ordering-guarantee.md):** delivery is completion-ordered per endpoint, not initiation-ordered. If an event fails 3 times, the endpoint is pinged. If the ping succeeds, the event is the problem: it's dead-lettered as `payload_rejected` (replayable) so it stops blocking the queue.

Smaller deviations:

- **A clean inline success writes only a ledger row, not a `delivery_attempts` row.** The status API falls back to the ledger.
- **Ingest dedups on the `events.event_id` primary key only.** There's no Redis fast path in front of it, since the PK index check is already cheap.
- **Not built (needs AWS / real infra):** Kubernetes, Terraform, CI/CD, load tests (§14), OpenTelemetry/metrics, ledger time-partitioning, per-tenant retry policies.
